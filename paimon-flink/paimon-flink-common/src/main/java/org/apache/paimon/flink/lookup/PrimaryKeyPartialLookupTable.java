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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.query.RemoteTableQuery;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.CoreOptions.SCAN_BOUNDED_WATERMARK;
import static org.apache.paimon.CoreOptions.STREAM_SCAN_MODE;
import static org.apache.paimon.CoreOptions.StreamScanMode.FILE_MONITOR;

/**
 * 通过主键直接读取LSM树的查找表。
 * 该查找表支持基于主键的部分查找，可以直接访问LSM树结构。可通过主键直接检索数据，
 * 无需完整主键，通过部分主键即可定位数据所在的分区和桶，从而高效地检索数据。
 */
public class PrimaryKeyPartialLookupTable implements LookupTable {

    // 创建 QueryExecutor 的工厂方法，根据 Predicate 条件生成查询执行器
    // Predicate 用于表示查询过滤条件
    private final Function<Predicate, QueryExecutor> executorFactory;

    // 从主键中提取固定桶信息的工具类
    // 固定桶模式下，主键与桶的映射是固定的，桶的大小是预先确定的
    private final FixedBucketFromPkExtractor extractor;

    // 主键列重新排列的新行结构，用于当主键列与连接键列不一致时重新排列
    // 当查询主键与表存储的主键顺序不一致时，需要将查询主键调整为目标表主键的顺序
    @Nullable private final ProjectedRow keyRearrange;

    // 裁剪后的主键列重新排列的新行结构，用于特定场景下的主键排列（如部分主键匹配）
    // 类似于主键重新排列，但仅针对裁剪后的主键列进行重新排列
    @Nullable private final ProjectedRow trimmedKeyRearrange;

    // 当前特定分区的过滤条件，用于过滤特定的数据分区
    // 用于指定当前查询在特定分区进行，提升查询效率
    private Predicate specificPartition;

    // 当前查询执行器，用于执行实际的数据查询操作
    // 在 open 方法中初始化，并通过 QueryExecutor 完成具体的查询逻辑
    private QueryExecutor queryExecutor;

    // 构造函数，用于初始化 PartialLookupTable
    // 参数包括 QueryExecutor 工厂方法、表对象和连接键
    private PrimaryKeyPartialLookupTable(
            Function<Predicate, QueryExecutor> executorFactory,
            FileStoreTable table,
            List<String> joinKey) {
        this.executorFactory = executorFactory;

        // 检查表的分桶模式是否为固定哈希分桶模式
        // 如果不是，则抛出不支持的异常
        // 固定哈希分桶模式下，数据均匀分布到固定数量的桶中，便于高效管理
        if (table.bucketMode() != BucketMode.HASH_FIXED) {
            throw new UnsupportedOperationException(
                    "Unsupported mode for partial lookup: " + table.bucketMode());
        }

        // 初始化从主键提取固定桶的工具类
        this.extractor = new FixedBucketFromPkExtractor(table.schema());

        // 初始化主键列重新排列的 ProjectedRow
        // 如果表的主键列与连接键列不一致，需要重新排列主键列
        ProjectedRow keyRearrange = null;
        if (!table.primaryKeys().equals(joinKey)) {
            // 将表的主键列按照连接键列的顺序重新排列
            // 使用桌标操作，将主键列调整为与连接键一致的顺序
            keyRearrange = ProjectedRow.from(
                    table.primaryKeys().stream()
                            .map(joinKey::indexOf) // 找到主键列在连接键中的索引
                            .mapToInt(value -> value) // 转换为整数类型
                            .toArray());
        }
        this.keyRearrange = keyRearrange;

        // 初始化裁剪后的主键列重新排列的 ProjectedRow
        // 用于裁剪后的主键列的重新排列
        List<String> trimmedPrimaryKeys = table.schema().trimmedPrimaryKeys();
        ProjectedRow trimmedKeyRearrange = null;
        if (!trimmedPrimaryKeys.equals(joinKey)) {
            // 将裁剪后的主键列按照连接键列的顺序重新排列
            // 类似于主键列重新排列，但针对裁剪后的主键列
            trimmedKeyRearrange = ProjectedRow.from(
                    trimmedPrimaryKeys.stream()
                            .map(joinKey::indexOf)
                            .mapToInt(value -> value)
                            .toArray());
        }
        this.trimmedKeyRearrange = trimmedKeyRearrange;
    }

    // 用于测试访问查询执行器
    @VisibleForTesting
    QueryExecutor queryExecutor() {
        return queryExecutor;
    }

    // 设置特定分区的过滤条件
    // Predicate 表示分区过滤条件，用于确定查询的分区范围
    @Override
    public void specificPartitionFilter(Predicate filter) {
        this.specificPartition = filter;
    }

    // 打开查找表，准备查询操作
    @Override
    public void open() throws Exception {
        // 使用特定分区的过滤条件生成查询执行器
        this.queryExecutor = executorFactory.apply(specificPartition);
        // 刷新查询执行器，确保数据是最新的
        refresh();
    }

    // 根据给定的主键查询数据
    @Override
    public List<InternalRow> get(InternalRow key) throws IOException {
        // 调整主键行，重新排列列顺序（如果需要）
        InternalRow adjustedKey = key;
        if (keyRearrange != null) {
            // 根据 keyRearrange 的逻辑重新排列主键行
            adjustedKey = keyRearrange.replaceRow(adjustedKey);
        }

        // 设置记录到 extractor 中，提取分区和桶信息
        this.extractor.setRecord(adjustedKey);

        // 获取桶号和分区信息
        int bucket = extractor.bucket();
        BinaryRow partition = extractor.partition();

        // 调整裁剪后的主键行（如果需要）
        InternalRow trimmedKey = key;
        if (trimmedKeyRearrange != null) {
            // 根据 trimmedKeyRearrange 的逻辑重新排列裁剪后的主键行
            trimmedKey = trimmedKeyRearrange.replaceRow(trimmedKey);
        }

        // 使用查询执行器在指定分区和桶中查找数据
        InternalRow kv = queryExecutor.lookup(partition, bucket, trimmedKey);

        // 如果未找到数据，返回空列表；否则返回查找结果
        if (kv == null) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(kv);
        }
    }

    // 刷新查找表，更新数据缓存或索引
    @Override
    public void refresh() {
        queryExecutor.refresh();
    }

    // 关闭查找表，释放资源
    @Override
    public void close() throws IOException {
        // 如果查询执行器存在，关闭它
        if (queryExecutor != null) {
            queryExecutor.close();
        }
    }

    // 创建本地表的 PartialLookupTable
    // 参数包括表对象、投影列、临时路径、连接键和要求缓存的桶ID
    public static PrimaryKeyPartialLookupTable createLocalTable(
            FileStoreTable table,
            int[] projection,
            File tempPath,
            List<String> joinKey,
            Set<Integer> requireCachedBucketIds) {
        // 使用 LocalQueryExecutor 构造 PartialLookupTable
        return new PrimaryKeyPartialLookupTable(
                filter -> new LocalQueryExecutor(
                        table, projection, tempPath, filter, requireCachedBucketIds),
                table,
                joinKey);
    }

    // 创建远程表的 PartialLookupTable
    // 参数包括表对象、投影列和连接键
    public static PrimaryKeyPartialLookupTable createRemoteTable(
            FileStoreTable table, int[] projection, List<String> joinKey) {
        // 使用 RemoteQueryExecutor 构造 PartialLookupTable
        return new PrimaryKeyPartialLookupTable(
                filter -> new RemoteQueryExecutor(table, projection), table, joinKey);
    }

    // 查询执行器接口，用于执行查询并管理资源
    interface QueryExecutor extends Closeable {

        // 根据分区、桶和键查找数据
        InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException;

        // 刷新查询执行器，更新数据缓存或索引
        void refresh();
    }

    // 本地查询执行器
    static class LocalQueryExecutor implements QueryExecutor {

        // 本地表查询对象，用于执行本地查询操作
        private final LocalTableQuery tableQuery;

        // 流式扫描对象，用于对表存储进行分区分桶的扫描查询
        private final StreamTableScan scan;

        // 构造函数，初始化本地查询执行器
        private LocalQueryExecutor(
                FileStoreTable table,
                int[] projection,
                File tempPath,
                @Nullable Predicate filter,
                Set<Integer> requireCachedBucketIds) {
            // 创建本地表查询对象
            this.tableQuery = table.newLocalTableQuery()
                    .withValueProjection(Projection.of(projection).toNestedIndexes())
                    .withIOManager(new IOManagerImpl(tempPath.toString()));

            // 创建分区分桶的流式扫描对象
            Map<String, String> dynamicOptions = new HashMap<>();
            dynamicOptions.put(STREAM_SCAN_MODE.key(), FILE_MONITOR.getValue());
            dynamicOptions.put(SCAN_BOUNDED_WATERMARK.key(), null);
            this.scan = table.copy(dynamicOptions)
                    .newReadBuilder()
                    .withFilter(filter) // 使用过滤条件
                    .withBucketFilter(
                            requireCachedBucketIds == null
                                    ? null
                                    : requireCachedBucketIds::contains) // 使用桶过滤
                    .newStreamScan();
        }

        // 根据分区、桶和键查找数据
        @Override
        public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
                throws IOException {
            return tableQuery.lookup(partition, bucket, key);
        }

        // 刷新本地查询执行器
        @Override
        public void refresh() {
            while (true) {
                // 获取分区分桶的待处理切片
                List<Split> splits = scan.plan().splits();
                if (splits.isEmpty()) {
                    return;
                }

                // 遍历每个切片并刷新数据
                for (Split split : splits) {
                    if (!(split instanceof DataSplit)) {
                        // 不支持的切片类型抛出异常
                        throw new IllegalArgumentException(
                                "Unsupported split: " + split.getClass());
                    }

                    // 获取分区、桶号和数据文件元数据
                    BinaryRow partition = ((DataSplit) split).partition();
                    int bucket = ((DataSplit) split).bucket();
                    List<DataFileMeta> before = ((DataSplit) split).beforeFiles();
                    List<DataFileMeta> after = ((DataSplit) split).dataFiles();

                    // 刷新文件记录信息
                    tableQuery.refreshFiles(partition, bucket, before, after);
                }
            }
        }

        // 关闭本地查询执行器，释放资源
        @Override
        public void close() throws IOException {
            tableQuery.close();
        }
    }

    // 远程查询执行器
    static class RemoteQueryExecutor implements QueryExecutor {

        // 远程表查询对象，用于执行远程查询
        private final RemoteTableQuery tableQuery;

        // 构造函数，初始化远程查询执行器
        private RemoteQueryExecutor(FileStoreTable table, int[] projection) {
            this.tableQuery = new RemoteTableQuery(table).withValueProjection(projection);
        }

        // 根据分区、桶和键查找数据
        @Override
        public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
                throws IOException {
            return tableQuery.lookup(partition, bucket, key);
        }

        // 刷新远程查询执行器
        @Override
        public void refresh() {}

        // 关闭远程查询执行器，释放资源
        @Override
        public void close() throws IOException {
            tableQuery.close();
        }
    }
}
