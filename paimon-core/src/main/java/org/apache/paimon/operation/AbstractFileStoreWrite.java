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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.paimon.io.DataFileMeta.getMaxSequenceNumber;

/**
 * 文件存储写入操作的抽象基类，提供基本的写入逻辑实现。
 *
 * @param <T> 需要写入的记录类型
 */
public abstract class AbstractFileStoreWrite<T> implements FileStoreWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFileStoreWrite.class);

    /** 当前提交的用户 */
    private final String commitUser;

    /** 快照管理器，管理存储系统的快照 */
    protected final SnapshotManager snapshotManager;

    /** 文件存储扫描器，用于扫描现有的文件元数据 */
    private final FileStoreScan scan;

    /** 允许的最大写入器数量 */
    private final int writerNumberMax;

    /** 可选的索引维护器工厂，用于管理索引 */
    @Nullable private final IndexMaintainer.Factory<T> indexFactory;

    /** 可选的删除向量维护器工厂，用于管理删除向量 */
    @Nullable private final DeletionVectorsMaintainer.Factory dvMaintainerFactory;

    /** 可选的 IO 管理器，用于管理 I/O 操作 */
    @Nullable protected IOManager ioManager;

    /** 记录当前活跃的写入器，按照分区和桶（bucket）进行组织 */
    protected final Map<BinaryRow, Map<Integer, WriterContainer<T>>> writers;

    /** 懒加载的压缩执行器 */
    private ExecutorService lazyCompactExecutor;

    /** 标记在退出时是否关闭压缩执行器 */
    private boolean closeCompactExecutorWhenLeaving = true;

    /** 是否忽略之前存储的文件 */
    private boolean ignorePreviousFiles = false;

    /** 是否处于流式写入模式 */
    protected boolean isStreamingMode = false;

    /** 用于记录压缩操作的度量指标 */
    protected CompactionMetrics compactionMetrics = null;

    /** 关联的表名称 */
    protected final String tableName;

    /** 是否仅支持插入（即不支持更新和删除） */
    private boolean isInsertOnly;

    /**
     * 构造函数，初始化 AbstractFileStoreWrite。
     *
     * @param commitUser 当前提交用户
     * @param snapshotManager 快照管理器
     * @param scan 文件存储扫描器
     * @param indexFactory 可选的索引维护器工厂
     * @param dvMaintainerFactory 可选的删除向量维护器工厂
     * @param tableName 关联的表名称
     * @param writerNumberMax 允许的最大写入器数量
     */
    protected AbstractFileStoreWrite(
            String commitUser,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            @Nullable IndexMaintainer.Factory<T> indexFactory,
            @Nullable DeletionVectorsMaintainer.Factory dvMaintainerFactory,
            String tableName,
            int writerNumberMax) {
        this.commitUser = commitUser;
        this.snapshotManager = snapshotManager;
        this.scan = scan;
        this.indexFactory = indexFactory;
        this.dvMaintainerFactory = dvMaintainerFactory;
        this.writers = new HashMap<>();
        this.tableName = tableName;
        this.writerNumberMax = writerNumberMax;
    }

    /**
     * 配置 I/O 管理器
     *
     * @param ioManager 需要配置的 I/O 管理器
     * @return 当前 FileStoreWrite 实例
     */
    @Override
    public FileStoreWrite<T> withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        // 设置内存池工厂，当前方法未实现具体逻辑，子类可覆盖此方法
        return this;
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        // 设置是否忽略之前存储的文件，适用于全量写入或者清空表重新写入的情况
        this.ignorePreviousFiles = ignorePreviousFiles;
    }

    @Override
    public void withCompactExecutor(ExecutorService compactExecutor) {
        // 设置压缩任务执行器，防止数据碎片过多影响查询性能
        this.lazyCompactExecutor = compactExecutor;
        this.closeCompactExecutorWhenLeaving = false;
    }

    @Override
    public void withInsertOnly(boolean insertOnly) {
        // 设置是否为仅插入模式（即不支持更新和删除）
        this.isInsertOnly = insertOnly;
        for (Map<Integer, WriterContainer<T>> containerMap : writers.values()) {
            for (WriterContainer<T> container : containerMap.values()) {
                container.writer.withInsertOnly(insertOnly);
            }
        }
    }

    /**
     * 根据分区和 bucket 将数据写入存储。
     *
     * @param partition 分区
     * @param bucket 桶编号
     * @param data 需要写入的数据
     * @throws Exception 发生异常时抛出
     */
    @Override
    public void write(BinaryRow partition, int bucket, T data) throws Exception {
        // 根据分区和桶获取 WriterContainer，获取写入组件
        WriterContainer<T> container = getWriterWrapper(partition, bucket);
        // 执行写入
        container.writer.write(data);
        // 如果存在索引维护器，通知其有新数据写入
        if (container.indexMaintainer != null) {
            container.indexMaintainer.notifyNewRecord(data);
        }
    }

    /**
     * 对存储数据进行压缩操作，以减少存储空间占用并优化查询性能。
     *
     * @param partition 分区
     * @param bucket 桶编号
     * @param fullCompaction 是否执行完全压缩
     * @throws Exception 发生异常时抛出
     */
    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        getWriterWrapper(partition, bucket).writer.compact(fullCompaction);
    }

    /**
     * 记录新创建的文件，通常用于数据写入过程中产生的新数据文件。
     *
     * @param snapshotId 产生新文件的快照 ID
     * @param partition 产生新文件的分区
     * @param bucket 产生新文件的桶
     * @param files 新创建的文件列表
     */
    @Override
    public void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        WriterContainer<T> writerContainer = getWriterWrapper(partition, bucket);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "获取额外的压缩文件，分区: {}, bucket: {}，额外快照: {}, 基础快照: {}。\n文件: {}",
                    partition,
                    bucket,
                    snapshotId,
                    writerContainer.baseSnapshotId,
                    files);
        }
        if (snapshotId > writerContainer.baseSnapshotId) {
            writerContainer.writer.addNewFiles(files);
        }
    }

    /**
     * 准备提交数据，确保所有的写入操作都被正确记录。
     *
     * @param waitCompaction 是否等待当前压缩任务完成
     * @param commitIdentifier 本次提交的唯一标识符
     * @return 提交的 CommitMessage 列表
     * @throws Exception 发生异常时抛出
     */
    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier) throws Exception {
        long latestCommittedIdentifier;

        // 获取最新提交的标识符，如果是首次提交，跳过快照扫描，减少不必要的开销
        if (writers.values().stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .mapToLong(w -> w.lastModifiedCommitIdentifier)
                .max()
                .orElse(Long.MIN_VALUE)
                == Long.MIN_VALUE) {
            latestCommittedIdentifier = Long.MIN_VALUE;
        } else {
            latestCommittedIdentifier =
                    snapshotManager
                            .latestSnapshotOfUser(commitUser)
                            .map(Snapshot::commitIdentifier)
                            .orElse(Long.MIN_VALUE);
        }

        List<CommitMessage> result = new ArrayList<>();

        // 遍历所有分区和桶，处理提交逻辑
        Iterator<Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>>> partIter = writers.entrySet().iterator();
        while (partIter.hasNext()) {
            Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partEntry = partIter.next();
            BinaryRow partition = partEntry.getKey();
            Iterator<Map.Entry<Integer, WriterContainer<T>>> bucketIter = partEntry.getValue().entrySet().iterator();

            while (bucketIter.hasNext()) {
                Map.Entry<Integer, WriterContainer<T>> entry = bucketIter.next();
                int bucket = entry.getKey();
                WriterContainer<T> writerContainer = entry.getValue();

                // 获取提交增量信息
                CommitIncrement increment = writerContainer.writer.prepareCommit(waitCompaction);
                List<IndexFileMeta> newIndexFiles = new ArrayList<>();
                if (writerContainer.indexMaintainer != null) {
                    newIndexFiles.addAll(writerContainer.indexMaintainer.prepareCommit());
                }
                CompactDeletionFile compactDeletionFile = increment.compactDeletionFile();
                if (compactDeletionFile != null) {
                    compactDeletionFile.getOrCompute().ifPresent(newIndexFiles::add);
                }

                // 构造提交消息
                CommitMessageImpl committable = new CommitMessageImpl(
                        partition,
                        bucket,
                        increment.newFilesIncrement(),
                        increment.compactIncrement(),
                        new IndexIncrement(newIndexFiles)
                );
                result.add(committable);

                // 如果提交为空，并且写入器的最新修改已被提交，关闭该写入器并释放资源
                if (committable.isEmpty()) {
                    if (writerContainer.lastModifiedCommitIdentifier < latestCommittedIdentifier
                            && !writerContainer.writer.isCompacting()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "关闭写入器，分区: {}, bucket: {}，"
                                            + "写入器最后修改的提交标识符: {}, "
                                            + "最新已提交标识符: {}, "
                                            + "当前提交标识符: {}。",
                                    partition,
                                    bucket,
                                    writerContainer.lastModifiedCommitIdentifier,
                                    latestCommittedIdentifier,
                                    commitIdentifier);
                        }
                        writerContainer.writer.close();
                        bucketIter.remove();
                    }
                } else {
                    writerContainer.lastModifiedCommitIdentifier = commitIdentifier;
                }
            }

            // 如果当前分区没有可用的桶，移除该分区
            if (partEntry.getValue().isEmpty()) {
                partIter.remove();
            }
        }

        return result;
    }


    @Override
    public void close() throws Exception {
        for (Map<Integer, WriterContainer<T>> bucketWriters : writers.values()) {
            for (WriterContainer<T> writerContainer : bucketWriters.values()) {
                writerContainer.writer.close();
            }
        }
        writers.clear();
        if (lazyCompactExecutor != null && closeCompactExecutorWhenLeaving) {
            lazyCompactExecutor.shutdownNow();
        }
        if (compactionMetrics != null) {
            compactionMetrics.close();
        }
    }

    @Override
    public List<State<T>> checkpoint() {
        List<State<T>> result = new ArrayList<>();

        for (Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partitionEntry :
                writers.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            for (Map.Entry<Integer, WriterContainer<T>> bucketEntry :
                    partitionEntry.getValue().entrySet()) {
                int bucket = bucketEntry.getKey();
                WriterContainer<T> writerContainer = bucketEntry.getValue();

                CommitIncrement increment;
                try {
                    increment = writerContainer.writer.prepareCommit(false);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to extract state from writer of partition "
                                    + partition
                                    + " bucket "
                                    + bucket,
                            e);
                }
                // writer.allFiles() must be fetched after writer.prepareCommit(), because
                // compaction result might be updated during prepareCommit
                Collection<DataFileMeta> dataFiles = writerContainer.writer.dataFiles();
                result.add(
                        new State<>(
                                partition,
                                bucket,
                                writerContainer.baseSnapshotId,
                                writerContainer.lastModifiedCommitIdentifier,
                                dataFiles,
                                writerContainer.writer.maxSequenceNumber(),
                                writerContainer.indexMaintainer,
                                writerContainer.deletionVectorsMaintainer,
                                increment));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Extracted state " + result);
        }
        return result;
    }

    @Override
    public void restore(List<State<T>> states) {
        for (State<T> state : states) {
            RecordWriter<T> writer =
                    createWriter(
                            state.baseSnapshotId,
                            state.partition,
                            state.bucket,
                            state.dataFiles,
                            state.maxSequenceNumber,
                            state.commitIncrement,
                            compactExecutor(),
                            state.deletionVectorsMaintainer);
            notifyNewWriter(writer);
            WriterContainer<T> writerContainer =
                    new WriterContainer<>(
                            writer,
                            state.indexMaintainer,
                            state.deletionVectorsMaintainer,
                            state.baseSnapshotId);
            writerContainer.lastModifiedCommitIdentifier = state.lastModifiedCommitIdentifier;
            writers.computeIfAbsent(state.partition, k -> new HashMap<>())
                    .put(state.bucket, writerContainer);
        }
    }

    public Map<BinaryRow, List<Integer>> getActiveBuckets() {
        Map<BinaryRow, List<Integer>> result = new HashMap<>();
        for (Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partitions :
                writers.entrySet()) {
            result.put(partitions.getKey(), new ArrayList<>(partitions.getValue().keySet()));
        }
        return result;
    }
    // 根据分区和桶获取WriterContainer
    protected WriterContainer<T> getWriterWrapper(BinaryRow partition, int bucket) {
        // 从writers映射中获取当前分区对应的桶映射
        Map<Integer, WriterContainer<T>> buckets = writers.get(partition);
        // 如果当前分区尚未初始化桶映射，则创建一个新的并放入writers映射中
        if (buckets == null) {
            buckets = new HashMap<>();
            writers.put(partition.copy(), buckets);
        }
        // 使用computeIfAbsent方法，如果桶映射中不存在指定桶的WriterContainer，则创建一个新的
        return buckets.computeIfAbsent(
                bucket, k -> createWriterContainer(partition.copy(), bucket, ignorePreviousFiles));
    }

    private long writerNumber() {
        return writers.values().stream().mapToLong(e -> e.values().size()).sum();
    }

    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 创建WriterContainer，包含写入器、索引维护器和删除向量维护器
    */
    @VisibleForTesting
    public WriterContainer<T> createWriterContainer(
            BinaryRow partition, int bucket, boolean ignorePreviousFiles) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating writer for partition {}, bucket {}", partition, bucket);
        }
        // 如果不是流模式且当前写入器数量达到上限，则强制缓冲区溢出
        if (!isStreamingMode && writerNumber() >= writerNumberMax) {
            try {
                forceBufferSpill();
            } catch (Exception e) {
                throw new RuntimeException("Error happens while force buffer spill", e);
            }
        }
        // 获取最新的快照ID
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        // 如果不忽略之前的文件且存在最新的快照ID，则扫描并恢复现有的文件元数据
        List<DataFileMeta> restoreFiles = new ArrayList<>();
        if (!ignorePreviousFiles && latestSnapshotId != null) {
            restoreFiles = scanExistingFileMetas(latestSnapshotId, partition, bucket);
        }
        // 根据配置创建或恢复索引维护器
        IndexMaintainer<T> indexMaintainer =
                indexFactory == null
                        ? null
                        : indexFactory.createOrRestore(
                                ignorePreviousFiles ? null : latestSnapshotId, partition, bucket);
        // 根据配置创建或恢复删除向量维护器
        DeletionVectorsMaintainer deletionVectorsMaintainer =
                dvMaintainerFactory == null
                        ? null
                        : dvMaintainerFactory.createOrRestore(
                                ignorePreviousFiles ? null : latestSnapshotId, partition, bucket);
        // 创建写入器，并配置相关参数
        RecordWriter<T> writer =
                createWriter(
                        latestSnapshotId,
                        partition.copy(),
                        bucket,
                        restoreFiles,
                        getMaxSequenceNumber(restoreFiles),
                        null,
                        compactExecutor(),
                        deletionVectorsMaintainer);
        // 设置写入器为仅插入模式（如果配置为仅插入）
        writer.withInsertOnly(isInsertOnly);
        // 通知有新写入器创建
        notifyNewWriter(writer);
        // 返回包含写入器、索引维护器和删除向量维护器的新WriterContainer
        return new WriterContainer<>(
                writer, indexMaintainer, deletionVectorsMaintainer, latestSnapshotId);
    }

    @Override
    public void withExecutionMode(boolean isStreamingMode) {
        // 设置是否处于流模式
        this.isStreamingMode = isStreamingMode;
    }

    @Override
    public FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry) {
        // 绑定度量指标
        this.compactionMetrics = new CompactionMetrics(metricRegistry, tableName);
        return this;
    }

    /**
     * 扫描已有的数据文件元数据（DataFileMeta）。
     *
     * @param snapshotId  快照 ID，表示要从哪个快照中进行扫描
     * @param partition   需要扫描的分区
     * @param bucket      需要扫描的桶（bucket）
     * @return 返回该分区和桶中的数据文件元数据列表
     */
    private List<DataFileMeta> scanExistingFileMetas(
            long snapshotId, BinaryRow partition, int bucket) {
        List<DataFileMeta> existingFileMetas = new ArrayList<>();
        // 扫描指定快照、分区和 bucket 下的文件，并收集所有的 DataFileMeta
        scan.withSnapshot(snapshotId)
                .withPartitionBucket(partition, bucket)
                .plan()
                .files()
                .stream()
                .map(ManifestEntry::file) // 获取文件元数据
                .forEach(existingFileMetas::add);
        return existingFileMetas;
    }

    /**
     * 获取或创建用于压缩（compaction）的线程池。
     *
     * @return  返回用于执行压缩任务的线程池
     */
    private ExecutorService compactExecutor() {
        if (lazyCompactExecutor == null) {
            // 创建单线程调度执行器，用于执行压缩任务
            lazyCompactExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            new ExecutorThreadFactory(
                                    Thread.currentThread().getName() + "-compaction"));
        }
        return lazyCompactExecutor;
    }

    /**
     * 获取当前的 compaction 线程池（用于测试）。
     *
     * @return  返回 compaction 线程池
     */
    @VisibleForTesting
    public ExecutorService getCompactExecutor() {
        return lazyCompactExecutor;
    }

    /**
     * 通知新创建的写入器（子类可重写该方法）。
     *
     * @param writer  需要通知的新写入器
     */
    protected void notifyNewWriter(RecordWriter<T> writer) {}

    /**
     * 创建新的 RecordWriter。
     *
     * @param snapshotId              该写入器创建时所基于的快照 ID
     * @param partition               数据分区
     * @param bucket                  目标存储桶
     * @param restoreFiles            需要恢复的文件列表
     * @param restoredMaxSeqNumber    需要恢复的最大序列号
     * @param restoreIncrement        需要恢复的增量信息
     * @param compactExecutor         压缩任务执行线程池
     * @param deletionVectorsMaintainer  删除向量维护器
     * @return  返回创建的 RecordWriter
     */
    protected abstract RecordWriter<T> createWriter(
            @Nullable Long snapshotId,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer);

    /**
     * 强制触发缓冲区溢出，以避免批处理模式下的内存溢出（子类可重写）。
     *
     * @throws Exception 如果溢出过程中发生异常
     */
    protected void forceBufferSpill() throws Exception {}

    /**
     * {@link WriterContainer} 用于存储 RecordWriter 及其相关信息。
     * 该类用于管理写入器的快照 ID 和最近修改的提交标识符（commit identifier）。
     */
    @VisibleForTesting
    public static class WriterContainer<T> {
        public final RecordWriter<T> writer; // 记录写入器
        @Nullable public final IndexMaintainer<T> indexMaintainer; // 索引维护器（可为空）
        @Nullable public final DeletionVectorsMaintainer deletionVectorsMaintainer; // 删除向量维护器
        protected final long baseSnapshotId; // 该写入器基于的快照 ID
        protected long lastModifiedCommitIdentifier; // 最近修改的提交标识符

        /**
         * WriterContainer 构造函数。
         *
         * @param writer                 记录写入器
         * @param indexMaintainer        索引维护器（可为空）
         * @param deletionVectorsMaintainer 删除向量维护器（可为空）
         * @param baseSnapshotId         该写入器创建时所基于的快照 ID
         */
        protected WriterContainer(
                RecordWriter<T> writer,
                @Nullable IndexMaintainer<T> indexMaintainer,
                @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer,
                Long baseSnapshotId) {
            this.writer = writer;
            this.indexMaintainer = indexMaintainer;
            this.deletionVectorsMaintainer = deletionVectorsMaintainer;
            // 如果 baseSnapshotId 为空，则设置为第一个快照 ID 之前
            this.baseSnapshotId =
                    baseSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : baseSnapshotId;
            this.lastModifiedCommitIdentifier = Long.MIN_VALUE; // 设定默认的提交标识符
        }
    }

    /**
     * 获取当前存储的所有写入器（用于测试）。
     *
     * @return  返回当前写入器的映射表
     */
    @VisibleForTesting
    Map<BinaryRow, Map<Integer, WriterContainer<T>>> writers() {
        return writers;
    }

}
