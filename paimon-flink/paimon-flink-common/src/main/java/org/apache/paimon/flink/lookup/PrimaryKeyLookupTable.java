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
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.RocksDBValueState;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.KeyProjectedRow;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 主键表的查找表实现。
 * <p>
 * 该类继承自FullCacheLookupTable，用于管理主键表的数据查找和缓存操作。
 * 它支持主键索引、LRU缓存、数据刷新等功能。
 */
public class PrimaryKeyLookupTable extends FullCacheLookupTable {

    /** LRU缓存大小 */
    protected final long lruCacheSize;

    /** 主键投影行，用于提取主键字段 */
    protected final KeyProjectedRow primaryKeyRow;

    /** 主键重新排列的投影行，用于调整主键字段的顺序 */
    @Nullable private final ProjectedRow keyRearrange;

    /** RocksDB值状态管理器，用于存储和管理主键对应的数据行 */
    protected RocksDBValueState<InternalRow, InternalRow> tableState;

    /**
     * 构造函数，初始化PrimaryKeyLookupTable对象。
     *
     * @param context 上下文信息
     * @param lruCacheSize LRU缓存大小
     * @param joinKey 连接键
     */
    public PrimaryKeyLookupTable(Context context, long lruCacheSize, List<String> joinKey) {
        super(context); // 调用父类构造函数
        this.lruCacheSize = lruCacheSize; // 设置LRU缓存大小
        List<String> fieldNames = projectedType.getFieldNames(); // 获取投影后的字段名称列表
        FileStoreTable table = context.table; // 获取文件存储表实例

        // 获取主键字段在投影类型中的索引
        int[] primaryKeyMapping = table.primaryKeys().stream().mapToInt(fieldNames::indexOf).toArray();
        this.primaryKeyRow = new KeyProjectedRow(primaryKeyMapping); // 创建主键投影行

        ProjectedRow keyRearrange = null; // 初始化主键重新排列的投影行
        if (!table.primaryKeys().equals(joinKey)) { // 如果主键和连接键不一致
            // 创建一个投影行，将主键字段重新排列为连接键的顺序
            keyRearrange = ProjectedRow.from(table.primaryKeys().stream().map(joinKey::indexOf).mapToInt(value -> value).toArray());
        }
        this.keyRearrange = keyRearrange; // 设置主键重新排列的投影行
    }

    /**
     * 初始化查找表。
     * <p>
     * 该方法负责打开状态工厂、创建表状态、并加载初始数据。
     *
     * @throws Exception 如果初始化过程中出现异常
     */
    @Override
    public void open() throws Exception {
        openStateFactory(); // 打开状态工厂
        createTableState(); // 创建表状态
        bootstrap(); // 装载初始数据
    }

    /**
     * 创建表状态。
     * <p>
     * 该方法使用状态工厂创建RocksDB值状态管理器，用于存储主键对应的数据行。
     *
     * @throws IOException 如果创建过程中出现IO异常
     */
    protected void createTableState() throws IOException {
        this.tableState = stateFactory.valueState(
                "table", // 表名
                InternalSerializers.create(TypeUtils.project(projectedType, primaryKeyRow.indexMapping())), // 键序列化器
                InternalSerializers.create(projectedType), // 值序列化器
                lruCacheSize); // 缓存大小
    }

    /**
     * 真正获取数据的方法。
     * <p>
     * 该方法会从表状态中获取与主键对应的数据行。
     *
     * @param key 查询键
     * @return 匹配的数据行列表
     * @throws IOException 如果获取过程中出现IO异常
     */
    @Override
    public List<InternalRow> innerGet(InternalRow key) throws IOException {
        if (keyRearrange != null) { // 如果需要重新排列主键
            key = keyRearrange.replaceRow(key); // 重新排列主键字段
        }
        InternalRow value = tableState.get(key); // 从表状态中获取数据行
        return value == null ? Collections.emptyList() : Collections.singletonList(value); // 返回结果
    }

    /**
     * 刷新数据行。
     * <p>
     * 该方法会根据主键更新或删除数据行，并支持用户定义的序列比较器。
     * <p>
     * 如果传入的数据行是INSERT或UPDATE_AFTER类型，则更新主键对应的数据行。
     * 如果传入的数据行是DELETE类型，则删除主键对应的数据行。
     *
     * @param row 数据行
     * @param predicate 过滤谓词
     * @throws IOException 如果刷新过程中出现IO异常
     */
    @Override
    protected void refreshRow(InternalRow row, Predicate predicate) throws IOException {
        primaryKeyRow.replaceRow(row); // 提取主键字段

        if (userDefinedSeqComparator != null && userDefinedSeqComparator.compare(row, tableState.get(primaryKeyRow)) > 0) {
            // 如果存在用户定义的序列比较器，并且新数据行较旧数据行更优先，则跳过更新
            return;
        }

        if (row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER) {
            // 插入或更新数据行
            if (predicate == null || predicate.test(row)) { // 如果满足谓词
                tableState.put(primaryKeyRow, row); // 更新数据行
            } else {
                // 如果不满足谓词，删除主键对应的数据行
                tableState.delete(primaryKeyRow);
            }
        } else {
            // 删除数据行
            tableState.delete(primaryKeyRow);
        }
    }

    /**
     * 将InternalRow对象转换为主键的字节数组。
     * <p>
     * 该方法会将主键字段序列化为字节数组。
     *
     * @param row 数据行
     * @return 主键的字节数组
     * @throws IOException 如果序列化过程中出现异常
     */
    @Override
    public byte[] toKeyBytes(InternalRow row) throws IOException {
        primaryKeyRow.replaceRow(row); // 提取主键字段
        return tableState.serializeKey(primaryKeyRow); // 序列化为主键字节数组
    }

    /**
     * 将InternalRow对象转换为值的字节数组。
     * <p>
     * 该方法会将数据行序列化为字节数组。
     *
     * @param row 数据行
     * @return 值的字节数组
     * @throws IOException 如果序列化过程中出现异常
     */
    @Override
    public byte[] toValueBytes(InternalRow row) throws IOException {
        return tableState.serializeValue(row); // 序列化为值字节数组
    }

    /**
     * 创建一个TableBulkLoader对象。
     * <p>
     * 该方法会创建一个批量加载器，用于将数据批量加载到表状态中。
     *
     * @return TableBulkLoader对象
     */
    @Override
    public TableBulkLoader createBulkLoader() {
        BulkLoader bulkLoader = tableState.createBulkLoader(); // 创建RocksDB批量加载器

        // 返回一个匿名内部类，实现TableBulkLoader接口
        return new TableBulkLoader() {
            @Override
            public void write(byte[] key, byte[] value) throws BulkLoader.WriteException, IOException {
                bulkLoader.write(key, value); // 写入键值对
                bulkLoadWritePlus(key, value); // 调用辅助方法
            }

            @Override
            public void finish() throws IOException {
                bulkLoader.finish(); // 完成批量加载
            }
        };
    }

    /**
     * 批量加载辅助方法，用于扩展批量加载功能。
     *
     * @param key 键字节数组
     * @param value 值字节数组
     * @throws IOException 如果写入过程中出现IO异常
     */
    public void bulkLoadWritePlus(byte[] key, byte[] value) throws IOException {}
}
