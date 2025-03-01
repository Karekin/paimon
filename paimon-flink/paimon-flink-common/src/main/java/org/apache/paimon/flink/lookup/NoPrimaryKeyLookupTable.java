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
import org.apache.paimon.lookup.RocksDBListState;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.KeyProjectedRow;
import org.apache.paimon.utils.TypeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 一个没有主键的表的查找表实现。
 * 继承自 {@link FullCacheLookupTable}，用于处理没有主键的表的查找操作。
 * 通过使用 LRU 缓存来存储查找结果，提高查找效率。
 */
public class NoPrimaryKeyLookupTable extends FullCacheLookupTable {

    /**
     * LRU 缓存的大小。
     * 用于控制缓存中最多可以存储的条目数量。
     */
    private final long lruCacheSize;

    /**
     * 用于投影连接键的行对象。
     * 它将连接键映射到对应的字段索引，用于在查找时快速定位数据。
     */
    private final KeyProjectedRow joinKeyRow;

    /**
     * 状态对象，用于存储和管理查找表的数据。
     * 使用了 RocksDB 作为底层存储引擎。
     */
    private RocksDBListState<InternalRow, InternalRow> state;

    /**
     * 构造方法，用于初始化没有主键的查找表。
     *
     * @param context       查找表的上下文对象，包含表的元数据和配置信息。
     * @param lruCacheSize  LRU 缓存的大小。
     */
    public NoPrimaryKeyLookupTable(Context context, long lruCacheSize) {
        super(context);  // 调用父类的构造方法，初始化父类的成员变量。
        this.lruCacheSize = lruCacheSize;  // 保存 LRU 缓存的大小。

        /**
         * 获取投影类型的字段名称列表。
         * 用于确定连接键在投影类型中的位置。
         */
        List<String> fieldNames = projectedType.getFieldNames();

        /**
         * 将连接键映射到字段索引。
         * 通过连接键的名称在字段名称列表中找到对应的索引。
         */
        int[] joinKeyMapping = context.joinKey.stream()
                .mapToInt(fieldNames::indexOf)
                .toArray();

        /**
         * 创建用于投影连接键的行对象。
         * 它将连接键映射到对应的字段索引，用于在查找时快速定位数据。
         */
        this.joinKeyRow = new KeyProjectedRow(joinKeyMapping);
    }

    /**
     * 打开查找表，初始化状态对象。
     *
     * @throws Exception 如果初始化过程中出现异常。
     */
    @Override
    public void open() throws Exception {
        openStateFactory();  // 打开状态工厂，初始化状态管理器。

        /**
         * 创建状态对象，用于存储和管理查找表的数据。
         * 使用了 RocksDB 的列表状态（ListState）来存储数据。
         */
        this.state = stateFactory.listState(
                "join-key-index",  // 状态的名称。
                InternalSerializers.create(
                        TypeUtils.project(projectedType, joinKeyRow.indexMapping())),
                InternalSerializers.create(projectedType),
                lruCacheSize);  // LRU 缓存的大小。

        bootstrap();  // 启动查找表，加载初始数据。
    }

    /**
     * 根据给定的键获取匹配的行。
     *
     * @param key 查询键。
     * @return 匹配的行列表。
     * @throws IOException 如果读取数据时出现异常。
     */
    @Override
    public List<InternalRow> innerGet(InternalRow key) throws IOException {
        return state.get(key);  // 从状态对象中获取与给定键匹配的行列表。
    }

    /**
     * 刷新查找表的数据。
     *
     * @param incremental 增量数据的迭代器。
     * @throws IOException 如果刷新过程中出现异常。
     */
    @Override
    public void refresh(Iterator<InternalRow> incremental) throws IOException {
        if (userDefinedSeqComparator != null) {
            throw new IllegalArgumentException(
                    "Append table does not support user defined sequence fields.");
        }
        super.refresh(incremental);  // 调用父类的刷新方法，处理增量数据。
    }

    /**
     * 刷新单行数据。
     *
     * @param row        要刷新的行。
     * @param predicate  过滤条件。
     * @throws IOException 如果刷新过程中出现异常。
     */
    @Override
    protected void refreshRow(InternalRow row, Predicate predicate) throws IOException {
        joinKeyRow.replaceRow(row);  // 将连接键投影到行对象中。

        if (row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER) {
            if (predicate == null || predicate.test(row)) {
                state.add(joinKeyRow, row);  // 如果行是插入或更新后的行，并且满足过滤条件，则将其添加到状态对象中。
            }
        } else {
            throw new RuntimeException(
                    String.format(
                            "Received %s message. Only INSERT/UPDATE_AFTER values are expected here.",
                            row.getRowKind()));  // 如果行的类型不是插入或更新后的行，抛出异常。
        }
    }

    /**
     * 将行的键序列化为字节数组。
     *
     * @param row 要序列化的行。
     * @return 序列化后的字节数组。
     * @throws IOException 如果序列化过程中出现异常。
     */
    @Override
    public byte[] toKeyBytes(InternalRow row) throws IOException {
        joinKeyRow.replaceRow(row);  // 将连接键投影到行对象中。
        return state.serializeKey(joinKeyRow);  // 将键序列化为字节数组。
    }

    /**
     * 将行的值序列化为字节数组。
     *
     * @param row 要序列化的行。
     * @return 序列化后的字节数组。
     * @throws IOException 如果序列化过程中出现异常。
     */
    @Override
    public byte[] toValueBytes(InternalRow row) throws IOException {
        return state.serializeValue(row);  // 将值序列化为字节数组。
    }

    /**
     * 创建一个批量加载器，用于高效地加载大量数据。
     *
     * @return 创建的批量加载器。
     */
    @Override
    public TableBulkLoader createBulkLoader() {
        BulkLoader bulkLoader = state.createBulkLoader();  // 创建底层的批量加载器。

        return new TableBulkLoader() {

            private final List<byte[]> values = new ArrayList<>();  // 用于存储值的列表。

            private byte[] currentKey;  // 当前正在处理的键。

            /**
             * 写入一行数据。
             *
             * @param key   行的键。
             * @param value 行的值。
             * @throws IOException 如果写入过程中出现异常。
             */
            @Override
            public void write(byte[] key, byte[] value) throws IOException {
                if (currentKey != null && !Arrays.equals(key, currentKey)) {
                    flush();  // 如果当前键与新键不同，刷新当前键的数据。
                }
                currentKey = key;  // 更新当前键。
                values.add(value);  // 将值添加到列表中。
            }

            /**
             * 完成批量加载。
             *
             * @throws IOException 如果完成过程中出现异常。
             */
            @Override
            public void finish() throws IOException {
                flush();  // 刷新剩余的数据。
                bulkLoader.finish();  // 完成底层批量加载器的操作。
            }

            /**
             * 刷新当前键的数据。
             *
             * @throws IOException 如果刷新过程中出现异常。
             */
            private void flush() throws IOException {
                if (currentKey != null && values.size() > 0) {
                    try {
                        bulkLoader.write(currentKey, state.serializeList(values));
                    } catch (BulkLoader.WriteException e) {
                        throw new RuntimeException(e);  // 如果写入失败，抛出异常。
                    }
                }

                currentKey = null;  // 重置当前键。
                values.clear();  // 清空值列表。
            }
        };
    }
}