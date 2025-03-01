/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.lookup.RocksDBSetState;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.KeyProjectedRow;
import org.apache.paimon.utils.TypeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 主键表的二级索引查找表实现。
 * <p>
 * 该类继承自 PrimaryKeyLookupTable，用于根据二级索引查找主键表的数据。
 * 它支持二级索引的建立、维护和数据查找功能。
 */
public class SecondaryIndexLookupTable extends PrimaryKeyLookupTable {

    /** 二级索引投影行，用于提取二级索引字段 */
    private final KeyProjectedRow secKeyRow;

    /** 二级索引状态管理器，用于存储和管理二级索引 */
    private RocksDBSetState<InternalRow, InternalRow> indexState;

    /**
     * 构造函数，初始化 SecondaryIndexLookupTable 对象。
     *
     * @param context 上下文信息
     * @param lruCacheSize LRU 缓存大小
     */
    public SecondaryIndexLookupTable(Context context, long lruCacheSize) {
        super(context, lruCacheSize / 2, context.table.primaryKeys()); // 调用父类构造函数
        List<String> fieldNames = projectedType.getFieldNames(); // 获取投影后的字段名称列表

        // 获取二级索引字段在投影类型中的索引
        int[] secKeyMapping = context.joinKey.stream().mapToInt(fieldNames::indexOf).toArray();
        this.secKeyRow = new KeyProjectedRow(secKeyMapping); // 创建二级索引投影行
    }

    /**
     * 初始化查找表。
     * <p>
     * 该方法负责打开状态工厂、创建表状态和二级索引状态，装载初始数据。
     *
     * @throws Exception 如果初始化过程中出现异常
     */
    @Override
    public void open() throws Exception {
        openStateFactory(); // 打开状态工厂
        createTableState(); // 创建表状态

        // 创建二级索引状态管理器
        this.indexState =
                stateFactory.setState(
                        "sec-index", // 二级索引名称
                        InternalSerializers.create(
                                TypeUtils.project(projectedType, secKeyRow.indexMapping())), // 键序列化器
                        InternalSerializers.create(
                                TypeUtils.project(projectedType, primaryKeyRow.indexMapping())), // 值序列化器
                        lruCacheSize); // 缓存大小

        bootstrap(); // 装载初始数据
    }

    /**
     * 真正获取数据的方法。
     * <p>
     * 该方法会根据二级索引字段，从二级索引状态中获取主键列表，进而获取主键对应的数据行。
     *
     * @param key 查询键
     * @return 匹配的数据行列表
     * @throws IOException 如果获取过程中出现IO异常
     */
    @Override
    public List<InternalRow> innerGet(InternalRow key) throws IOException {
        List<InternalRow> pks = indexState.get(key); // 获取主键列表
        List<InternalRow> values = new ArrayList<>(pks.size()); // 初始化结果列表

        for (InternalRow pk : pks) { // 遍历主键列表
            InternalRow row = tableState.get(pk); // 根据主键获取数据行
            if (row != null) { // 如果数据行存在
                values.add(row); // 添加到结果列表
            }
        }
        return values; // 返回结果列表
    }

    /**
     * 刷新数据行。
     * <p>
     * 该方法会根据主键更新或删除数据行，并同步更新二级索引状态。
     * <p>
     * 如果传入的数据行是 INSERT 或 UPDATE_AFTER 类型，则插入或更新数据行；
     * 如果传入的数据行是 DELETE 类型，则删除数据行。
     *
     * @param row 数据行
     * @param predicate 过滤谓词
     * @throws IOException 如果刷新过程中出现IO异常
     */
    @Override
    protected void refreshRow(InternalRow row, Predicate predicate) throws IOException {
        primaryKeyRow.replaceRow(row); // 提取主键字段

        boolean previousFetched = false; // 是否获取了旧值
        InternalRow previous = null; // 旧值

        if (userDefinedSeqComparator != null) { // 如果存在用户定义的序列比较器
            previous = tableState.get(primaryKeyRow); // 获取旧值
            previousFetched = true; // 标记已获取旧值
            if (previous != null && userDefinedSeqComparator.compare(previous, row) > 0) {
                // 如果旧值较新值更优先，跳过更新
                return;
            }
        }

        if (row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER) {
            // 插入或更新数据行
            if (!previousFetched) {
                previous = tableState.get(primaryKeyRow); // 获取旧值
            }
            if (previous != null) {
                // 如果存在旧值，从二级索引中删除旧值对应的主键
                indexState.retract(secKeyRow.replaceRow(previous), primaryKeyRow);
            }

            if (predicate == null || predicate.test(row)) { // 如果满足谓词
                tableState.put(primaryKeyRow, row); // 更新数据行
                indexState.add(secKeyRow.replaceRow(row), primaryKeyRow); // 更新二级索引
            } else {
                // 如果不满足谓词，删除主键对应的数据行
                tableState.delete(primaryKeyRow);
            }
        } else {
            // 删除数据行
            tableState.delete(primaryKeyRow);
            indexState.retract(secKeyRow.replaceRow(row), primaryKeyRow); // 从二级索引中删除主键
        }
    }

    /**
     * 批量加载辅助方法，用于扩展批量加载功能。
     * <p>
     * 该方法会在批量加载数据时，同步更新二级索引状态。
     *
     * @param key 键字节数组
     * @param value 值字节数组
     * @throws IOException 如果写入过程中出现IO异常
     */
    @Override
    public void bulkLoadWritePlus(byte[] key, byte[] value) throws IOException {
        InternalRow row = tableState.deserializeValue(value); // 反序列化值为数据行
        indexState.add(secKeyRow.replaceRow(row), primaryKeyRow.replaceRow(row)); // 更新二级索引
    }
}