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

package org.apache.paimon.operation;

import org.apache.paimon.FileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 *
 * 文件存储写入接口，提供创建 RecordWriter 以及将 SinkRecord 写入 FileStore 的功能。
 *
 * @param <T> 要写入的记录类型
 */
public interface FileStoreWrite<T> extends Restorable<List<FileStoreWrite.State<T>>> {

    /**
     * 绑定 IO 管理器
     *
     * @param ioManager IO 管理器
     */
    FileStoreWrite<T> withIOManager(IOManager ioManager);

    /**
     * 绑定当前文件存储写入的内存池。
     *
     * @param memoryPool 指定的内存池
     */
    default FileStoreWrite<T> withMemoryPool(MemorySegmentPool memoryPool) {
        return withMemoryPoolFactory(new MemoryPoolFactory(memoryPool));
    }

    /**
     * 绑定当前文件存储写入的内存池工厂。
     *
     * @param memoryPoolFactory 指定的内存池工厂
     */
    FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory);

    /**
     * 设置写入操作是否应忽略之前存储的文件。
     *
     * @param ignorePreviousFiles 是否忽略之前存储的文件
     */
    void withIgnorePreviousFiles(boolean ignorePreviousFiles);

    /**
     * 根据当前模式进行优化，区分是否为流模式。
     *
     * @param isStreamingMode 是否处于流式模式
     */
    void withExecutionMode(boolean isStreamingMode);

    /**
     * 绑定度量指标注册器，用于监控数据压缩（compaction）过程。
     */
    FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry);

    /**
     * 绑定压缩任务的执行器。
     *
     * @param compactExecutor 压缩任务执行器
     */
    void withCompactExecutor(ExecutorService compactExecutor);

    /**
     * 当记录的“仅插入”状态发生变化时调用此方法。
     *
     * @param insertOnly 如果为 true，则所有后续记录都是 {@link org.apache.paimon.types.RowKind#INSERT}，
     *                   并且不会有相同主键的两条记录。
     */
    void withInsertOnly(boolean insertOnly);

    /**
     * 根据分区和 bucket 将数据写入存储。
     *
     * @param partition  数据所属的分区
     * @param bucket     数据所属的 bucket ID
     * @param data       要写入的数据
     * @throws Exception 写入数据时可能发生的异常
     */
    void write(BinaryRow partition, int bucket, T data) throws Exception;

    /**
     * 对存储在指定分区和 bucket 中的数据进行合并（compaction）。
     * 需要注意的是，压缩操作仅提交，而不是同步执行，因此方法返回时压缩可能尚未完成。
     *
     * @param partition        需要进行压缩的分区
     * @param bucket           需要进行压缩的 bucket ID
     * @param fullCompaction   是否触发完整压缩（true 表示强制执行全量合并，false 表示正常合并）
     * @throws Exception       在压缩过程中可能抛出的异常
     */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /**
     * 通知存储层在特定快照 ID 下的指定分区和 bucket 中创建了一些新文件。
     *
     * <p>通常情况下，这些文件是由另一个任务创建的。目前，该方法仅用于专门的压缩任务，
     * 以查看由写入任务创建的新文件。
     *
     * @param snapshotId  生成新文件的快照 ID
     * @param partition   生成新文件的分区
     * @param bucket      生成新文件的 bucket ID
     * @param files       生成的新文件列表
     */
    void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files);

    /**
     * 预提交写入操作，将数据准备提交到存储层。
     *
     * @param waitCompaction     是否等待当前的合并（compaction）完成
     * @param commitIdentifier   提交操作的唯一标识符
     * @return                   可提交的文件列表
     * @throws Exception         提交过程中可能抛出的异常
     */
    List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier) throws Exception;

    /**
     * 关闭写入器，释放资源。
     *
     * @throws Exception  关闭时可能抛出的异常
     */
    void close() throws Exception;

    /**
     * FileStoreWrite 的可恢复状态类，用于存储当前的写入状态，以支持容错恢复。
     */
    class State<T> {

        /** 该写入状态所属的分区 */
        protected final BinaryRow partition;

        /** 该写入状态所属的 bucket ID */
        protected final int bucket;

        /** 该写入状态基于的快照 ID */
        protected final long baseSnapshotId;

        /** 上一次提交的唯一标识符 */
        protected final long lastModifiedCommitIdentifier;

        /** 该状态下的所有数据文件元数据 */
        protected final List<DataFileMeta> dataFiles;

        /** 该状态记录的最大序列号 */
        protected final long maxSequenceNumber;

        /** 索引维护器（可为空） */
        @Nullable protected final IndexMaintainer<T> indexMaintainer;

        /** 删除向量维护器（可为空） */
        @Nullable protected final DeletionVectorsMaintainer deletionVectorsMaintainer;

        /** 该状态的提交增量信息 */
        protected final CommitIncrement commitIncrement;

        /**
         * 构造 FileStoreWrite 的状态信息。
         *
         * @param partition                该状态所属的分区
         * @param bucket                   该状态所属的 bucket
         * @param baseSnapshotId           该状态基于的快照 ID
         * @param lastModifiedCommitIdentifier 上一次提交的唯一标识符
         * @param dataFiles                该状态下的数据文件列表
         * @param maxSequenceNumber        该状态记录的最大序列号
         * @param indexMaintainer          索引维护器（可为空）
         * @param deletionVectorsMaintainer 删除向量维护器（可为空）
         * @param commitIncrement          提交增量信息
         */
        protected State(
                BinaryRow partition,
                int bucket,
                long baseSnapshotId,
                long lastModifiedCommitIdentifier,
                Collection<DataFileMeta> dataFiles,
                long maxSequenceNumber,
                @Nullable IndexMaintainer<T> indexMaintainer,
                @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer,
                CommitIncrement commitIncrement) {
            this.partition = partition;
            this.bucket = bucket;
            this.baseSnapshotId = baseSnapshotId;
            this.lastModifiedCommitIdentifier = lastModifiedCommitIdentifier;
            this.dataFiles = new ArrayList<>(dataFiles);
            this.maxSequenceNumber = maxSequenceNumber;
            this.indexMaintainer = indexMaintainer;
            this.deletionVectorsMaintainer = deletionVectorsMaintainer;
            this.commitIncrement = commitIncrement;
        }

        /**
         * 返回当前状态的字符串表示。
         *
         * @return 当前状态的字符串信息
         */
        @Override
        public String toString() {
            return String.format(
                    "{%s, %d, %d, %d, %s, %d, %s, %s, %s}",
                    partition,
                    bucket,
                    baseSnapshotId,
                    lastModifiedCommitIdentifier,
                    dataFiles,
                    maxSequenceNumber,
                    indexMaintainer,
                    deletionVectorsMaintainer,
                    commitIncrement);
        }
    }

}
