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

package org.apache.paimon.table.sink;

import org.apache.paimon.FileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.BundleFileStoreWriter;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.FileStoreWrite.State;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * {@link TableWrite} 的实现类，用于向 {@link FileStore} 写入数据。
 *
 * @param <T> 要写入的数据类型
 */
public class TableWriteImpl<T> implements InnerTableWrite, Restorable<List<State<T>>> {

    /** 数据表的行类型定义 */
    private final RowType rowType;

    /** 文件存储写入组件 */
    private final FileStoreWrite<T> write;

    /** 负责从 {@link InternalRow} 提取主键和桶信息 */
    private final KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor;

    /** 负责从 {@link SinkRecord} 提取写入的数据 */
    private final RecordExtractor<T> recordExtractor;

    /** 用于生成 {@link RowKind}（数据变更类型）的组件 */
    @Nullable private final RowKindGenerator rowKindGenerator;

    /** 是否忽略 DELETE 记录 */
    private final boolean ignoreDelete;

    /** 是否已经提交批处理任务 */
    private boolean batchCommitted = false;

    /** 桶模式 */
    private BucketMode bucketMode;

    /** 记录不可为空的字段索引 */
    private final int[] notNullFieldIndex;

    /**
     * 构造函数，初始化 TableWriteImpl 组件。
     */
    public TableWriteImpl(
            RowType rowType,
            FileStoreWrite<T> write,
            KeyAndBucketExtractor<InternalRow> keyAndBucketExtractor,
            RecordExtractor<T> recordExtractor,
            @Nullable RowKindGenerator rowKindGenerator,
            boolean ignoreDelete) {
        this.rowType = rowType;
        this.write = write;
        this.keyAndBucketExtractor = keyAndBucketExtractor;
        this.recordExtractor = recordExtractor;
        this.rowKindGenerator = rowKindGenerator;
        this.ignoreDelete = ignoreDelete;

        // 获取所有不可为空字段的索引
        List<String> notNullColumnNames =
                rowType.getFields().stream()
                        .filter(field -> !field.type().isNullable())
                        .map(DataField::name)
                        .collect(Collectors.toList());
        this.notNullFieldIndex = rowType.getFieldIndices(notNullColumnNames);
    }

    /**
     * 设置是否忽略先前的文件。
     */
    @Override
    public TableWriteImpl<T> withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        write.withIgnorePreviousFiles(ignorePreviousFiles);
        return this;
    }

    /**
     * 设置执行模式（批处理或流处理）。
     */
    @Override
    public TableWriteImpl<T> withExecutionMode(boolean isStreamingMode) {
        write.withExecutionMode(isStreamingMode);
        return this;
    }

    /**
     * 设置 I/O 管理器。
     */
    @Override
    public TableWriteImpl<T> withIOManager(IOManager ioManager) {
        write.withIOManager(ioManager);
        return this;
    }

    /**
     * 设置内存池。
     */
    @Override
    public TableWriteImpl<T> withMemoryPool(MemorySegmentPool memoryPool) {
        write.withMemoryPool(memoryPool);
        return this;
    }

    /**
     * 设置内存池工厂。
     */
    public TableWriteImpl<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        write.withMemoryPoolFactory(memoryPoolFactory);
        return this;
    }

    /**
     * 设置压缩执行器。
     */
    public TableWriteImpl<T> withCompactExecutor(ExecutorService compactExecutor) {
        write.withCompactExecutor(compactExecutor);
        return this;
    }

    /**
     * 设置桶模式。
     */
    public TableWriteImpl<T> withBucketMode(BucketMode bucketMode) {
        this.bucketMode = bucketMode;
        return this;
    }

    /**
     * 设置是否只插入数据（不允许更新或删除）。
     */
    @Override
    public void withInsertOnly(boolean insertOnly) {
        write.withInsertOnly(insertOnly);
    }

    /**
     * 获取指定行的分区信息。
     */
    @Override
    public BinaryRow getPartition(InternalRow row) {
        keyAndBucketExtractor.setRecord(row);
        return keyAndBucketExtractor.partition();
    }

    /**
     * 获取指定行的桶编号。
     */
    @Override
    public int getBucket(InternalRow row) {
        keyAndBucketExtractor.setRecord(row);
        return keyAndBucketExtractor.bucket();
    }

    /**
     * 向存储中写入数据（不指定存储桶）。
     *
     * @param row 需要写入的数据行
     * @throws Exception 写入失败时抛出异常
     */
    @Override
    public void write(InternalRow row) throws Exception {
        writeAndReturn(row);
    }

    /**
     * 向存储中写入数据（指定存储桶）。
     *
     * @param row 需要写入的数据行
     * @param bucket 目标存储桶 ID
     * @throws Exception 写入失败时抛出异常
     */
    @Override
    public void write(InternalRow row, int bucket) throws Exception {
        writeAndReturn(row, bucket);
    }

    /**
     * 批量写入数据（BundleRecords）。
     *
     * @param partition 目标分区
     * @param bucket 目标存储桶 ID
     * @param bundle 需要写入的数据集合
     * @throws Exception 写入失败时抛出异常
     */
    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle) throws Exception {
        if (write instanceof BundleFileStoreWriter) {
            // 如果当前写入器支持批量写入，直接调用批量写入方法
            ((BundleFileStoreWriter) write).writeBundle(partition, bucket, bundle);
        } else {
            // 否则逐条写入
            for (InternalRow row : bundle) {
                write(row, bucket);
            }
        }
    }

    /**
     * 将数据转换为 SinkRecord 并写入存储。
     *
     * @param row 需要写入的数据行
     * @return 如果成功写入，返回对应的 SinkRecord，否则返回 null
     * @throws Exception 写入失败时抛出异常
     */
    @Nullable
    public SinkRecord writeAndReturn(InternalRow row) throws Exception {
        checkNullability(row);
        RowKind rowKind = RowKindGenerator.getRowKind(rowKindGenerator, row);
        if (ignoreDelete && rowKind.isRetract()) {
            // 如果忽略删除操作，直接返回 null
            return null;
        }
        SinkRecord record = toSinkRecord(row);
        // 写入数据到存储
        write.write(record.partition(), record.bucket(), recordExtractor.extract(record, rowKind));
        return record;
    }

    /**
     * 将数据转换为 SinkRecord 并写入存储（指定存储桶）。
     *
     * @param row 需要写入的数据行
     * @param bucket 目标存储桶 ID
     * @return 如果成功写入，返回对应的 SinkRecord，否则返回 null
     * @throws Exception 写入失败时抛出异常
     */
    @Nullable
    public SinkRecord writeAndReturn(InternalRow row, int bucket) throws Exception {
        checkNullability(row);
        RowKind rowKind = RowKindGenerator.getRowKind(rowKindGenerator, row);
        if (ignoreDelete && rowKind.isRetract()) {
            return null;
        }
        SinkRecord record = toSinkRecord(row, bucket);
        write.write(record.partition(), bucket, recordExtractor.extract(record, rowKind));
        return record;
    }

    /**
     * 检查行中的字段是否符合非空约束。
     *
     * @param row 需要检查的数据行
     * @throws RuntimeException 如果字段不符合非空约束，则抛出异常
     */
    private void checkNullability(InternalRow row) {
        for (int idx : notNullFieldIndex) {
            if (row.isNullAt(idx)) {
                String columnName = rowType.getFields().get(idx).name();
                throw new RuntimeException(
                        String.format("不能将空值写入非空列（%s）", columnName));
            }
        }
    }

    /**
     * 生成 SinkRecord（带存储桶 ID）。
     *
     * @param row 需要转换的数据行
     * @param bucket 目标存储桶 ID
     * @return 生成的 SinkRecord
     */
    private SinkRecord toSinkRecord(InternalRow row, int bucket) {
        keyAndBucketExtractor.setRecord(row);
        return new SinkRecord(
                keyAndBucketExtractor.partition(),
                bucket,
                keyAndBucketExtractor.trimmedPrimaryKey(),
                row);
    }

    /**
     * 生成 SinkRecord（自动计算存储桶 ID）。
     *
     * @param row 需要转换的数据行
     * @return 生成的 SinkRecord
     */
    private SinkRecord toSinkRecord(InternalRow row) {
        keyAndBucketExtractor.setRecord(row);
        return new SinkRecord(
                keyAndBucketExtractor.partition(),
                keyAndBucketExtractor.bucket(),
                keyAndBucketExtractor.trimmedPrimaryKey(),
                row);
    }

    /**
     * 将 SinkRecord 转换为日志记录。
     *
     * @param record 需要转换的 SinkRecord
     * @return 转换后的日志记录
     */
    public SinkRecord toLogRecord(SinkRecord record) {
        keyAndBucketExtractor.setRecord(record.row());
        return new SinkRecord(
                record.partition(),
                bucketMode == BucketMode.BUCKET_UNAWARE ? -1 : record.bucket(),
                keyAndBucketExtractor.logPrimaryKey(),
                record.row());
    }

    /**
     * 执行 compact（合并）操作，以优化存储。
     *
     * @param partition 目标分区
     * @param bucket 目标存储桶 ID
     * @param fullCompaction 是否执行完整 compact
     * @throws Exception compact 过程中发生异常
     */
    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        write.compact(partition, bucket, fullCompaction);
    }

    /**
     * 设置度量指标注册表。
     *
     * @param metricRegistry 需要注册的度量指标
     * @return 当前 TableWriteImpl 实例
     */
    @Override
    public TableWriteImpl<T> withMetricRegistry(MetricRegistry metricRegistry) {
        write.withMetricRegistry(metricRegistry);
        return this;
    }


    /**
     * 通知存储系统，在指定的快照 ID 和存储桶中创建了新文件。
     *
     * <p>这些文件通常是由另一个作业创建的。当前此方法仅被专门用于 compact 作业，
     * 以便查看 writer 作业创建的文件。
     *
     * @param snapshotId  快照 ID
     * @param partition   目标分区
     * @param bucket      目标存储桶 ID
     * @param files       新创建的文件列表
     */
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        write.notifyNewFiles(snapshotId, partition, bucket, files);
    }

    /**
     * 准备提交数据（支持等待 compact 操作）。
     *
     * @param waitCompaction   是否等待 compact 完成
     * @param commitIdentifier 提交标识符
     * @return 提交消息列表
     * @throws Exception 提交过程中发生的异常
     */
    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        return write.prepareCommit(waitCompaction, commitIdentifier);
    }

    /**
     * 进行一次性提交，适用于批量写入。
     *
     * @return 提交消息列表
     * @throws Exception 提交失败时抛出异常
     */
    @Override
    public List<CommitMessage> prepareCommit() throws Exception {
        // 检查是否已经提交过
        checkState(!batchCommitted, "BatchTableWrite 仅支持一次性提交。");
        batchCommitted = true;
        return prepareCommit(true, BatchWriteBuilder.COMMIT_IDENTIFIER);
    }

    /**
     * 关闭写入器，释放资源。
     *
     * @throws Exception 关闭过程中发生的异常
     */
    @Override
    public void close() throws Exception {
        write.close();
    }

    /**
     * 创建一个检查点，用于保存当前写入状态，以便故障恢复。
     *
     * @return 当前写入状态的快照
     */
    @Override
    public List<State<T>> checkpoint() {
        return write.checkpoint();
    }

    /**
     * 从之前的检查点恢复写入状态。
     *
     * @param state 之前的写入状态
     */
    @Override
    public void restore(List<State<T>> state) {
        write.restore(state);
    }

    /**
     * 供测试使用的方法，获取底层的 FileStoreWrite 实例。
     *
     * @return FileStoreWrite 实例
     */
    @VisibleForTesting
    public FileStoreWrite<T> getWrite() {
        return write;
    }

    /**
     * 从 SinkRecord 提取数据的接口，定义如何从 SinkRecord 中提取数据记录。
     *
     * @param <T> 提取的数据类型
     */
    public interface RecordExtractor<T> {
        /**
         * 从 SinkRecord 中提取数据记录。
         *
         * @param record   SinkRecord 实例
         * @param rowKind  行变更类型（INSERT、UPDATE、DELETE 等）
         * @return 提取的数据记录
         */
        T extract(SinkRecord record, RowKind rowKind);
    }

}
