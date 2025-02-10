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

package org.apache.paimon.flink.sink;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.table.sink.TableWriteImpl;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * {@link StoreSinkWrite} 的默认实现。
 * 该类用于管理 Paimon Sink 的数据写入过程，不包含持久化状态。
 */
public class StoreSinkWriteImpl implements StoreSinkWrite {

    /** 日志记录器 */
    private static final Logger LOG = LoggerFactory.getLogger(StoreSinkWriteImpl.class);

    /** 提交用户标识 */
    protected final String commitUser;

    /** Sink 状态管理对象 */
    protected final StoreSinkWriteState state;

    /** Paimon 内部 IO 管理器，用于管理数据溢写到磁盘 */
    private final IOManagerImpl paimonIOManager;

    /** 是否忽略历史文件，适用于部分更新场景 */
    private final boolean ignorePreviousFiles;

    /** 是否等待 Compaction（合并小文件） */
    private final boolean waitCompaction;

    /** 是否启用流式模式 */
    private final boolean isStreamingMode;

    /** （可选）内存段池，用于管理写入缓冲区 */
    @Nullable
    private final MemorySegmentPool memoryPool;

    /** （可选）内存池工厂 */
    @Nullable
    private final MemoryPoolFactory memoryPoolFactory;

    /** Paimon 的表写入对象 */
    protected TableWriteImpl<?> write;

    /** （可选）Flink 指标度量组 */
    @Nullable
    private final MetricGroup metricGroup;

    /**
     * 构造方法，初始化 StoreSinkWriteImpl。
     *
     * @param table               Paimon 表
     * @param commitUser          提交用户
     * @param state               Sink 状态
     * @param ioManager           IO 管理器
     * @param ignorePreviousFiles 是否忽略历史文件
     * @param waitCompaction      是否等待 Compaction（合并小文件）
     * @param isStreamingMode     是否启用流模式
     * @param memoryPool          内存池（可选）
     * @param metricGroup         Flink 指标组（可选）
     */
    public StoreSinkWriteImpl(
            FileStoreTable table,
            String commitUser,
            StoreSinkWriteState state,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean waitCompaction,
            boolean isStreamingMode,
            @Nullable MemorySegmentPool memoryPool,
            @Nullable MetricGroup metricGroup) {
        this(
                table,
                commitUser,
                state,
                ioManager,
                ignorePreviousFiles,
                waitCompaction,
                isStreamingMode,
                memoryPool,
                null,
                metricGroup);
    }

    /**
     * 构造方法（支持 MemoryPoolFactory）。
     */
    public StoreSinkWriteImpl(
            FileStoreTable table,
            String commitUser,
            StoreSinkWriteState state,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean waitCompaction,
            boolean isStreamingMode,
            MemoryPoolFactory memoryPoolFactory,
            @Nullable MetricGroup metricGroup) {
        this(
                table,
                commitUser,
                state,
                ioManager,
                ignorePreviousFiles,
                waitCompaction,
                isStreamingMode,
                null,
                memoryPoolFactory,
                metricGroup);
    }

    /**
     * 私有构造方法，统一初始化逻辑。
     */
    private StoreSinkWriteImpl(
            FileStoreTable table,
            String commitUser,
            StoreSinkWriteState state,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean waitCompaction,
            boolean isStreamingMode,
            @Nullable MemorySegmentPool memoryPool,
            @Nullable MemoryPoolFactory memoryPoolFactory,
            @Nullable MetricGroup metricGroup) {
        this.commitUser = commitUser;
        this.state = state;
        this.paimonIOManager = new IOManagerImpl(ioManager.getSpillingDirectoriesPaths());
        this.ignorePreviousFiles = ignorePreviousFiles;
        this.waitCompaction = waitCompaction;
        this.isStreamingMode = isStreamingMode;
        this.memoryPool = memoryPool;
        this.memoryPoolFactory = memoryPoolFactory;
        this.metricGroup = metricGroup;
        this.write = newTableWrite(table);
    }

    /**
     * 创建新的 TableWriteImpl 实例。
     */
    private TableWriteImpl<?> newTableWrite(FileStoreTable table) {
        checkArgument(
                !(memoryPool != null && memoryPoolFactory != null),
                "memoryPool 和 memoryPoolFactory 不能同时设置。");

        TableWriteImpl<?> tableWrite =
                table.newWrite(
                                commitUser,
                                (part, bucket) ->
                                        state.stateValueFilter().filter(table.name(), part, bucket))
                        .withIOManager(paimonIOManager)
                        .withIgnorePreviousFiles(ignorePreviousFiles)
                        .withExecutionMode(isStreamingMode)
                        .withBucketMode(table.bucketMode());

        if (metricGroup != null) {
            tableWrite.withMetricRegistry(new FlinkMetricRegistry(metricGroup));
        }

        if (memoryPoolFactory != null) {
            return tableWrite.withMemoryPoolFactory(memoryPoolFactory);
        } else {
            return tableWrite.withMemoryPool(
                    memoryPool != null
                            ? memoryPool
                            : new HeapMemorySegmentPool(
                            table.coreOptions().writeBufferSize(),
                            table.coreOptions().pageSize()));
        }
    }

    /**
     * 设置 Compact 线程池。
     */
    public void withCompactExecutor(ExecutorService compactExecutor) {
        write.withCompactExecutor(compactExecutor);
    }

    /**
     * 设置是否仅支持插入模式（Insert-Only）。
     */
    @Override
    public void withInsertOnly(boolean insertOnly) {
        write.withInsertOnly(insertOnly);
    }

    /**
     * 将数据写入 Paimon 表。
     */
    @Override
    @Nullable
    public SinkRecord write(InternalRow rowData) throws Exception {
        return write.writeAndReturn(rowData);
    }

    /**
     * 将数据写入 Paimon 表中的指定桶（Bucket）。
     */
    @Override
    @Nullable
    public SinkRecord write(InternalRow rowData, int bucket) throws Exception {
        return write.writeAndReturn(rowData, bucket);
    }

    /**
     * 将 SinkRecord 转换为日志存储格式。
     */
    @Override
    public SinkRecord toLogRecord(SinkRecord record) {
        return write.toLogRecord(record);
    }

    /**
     * 触发 Compact 任务，合并小文件。
     */
    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        write.compact(partition, bucket, fullCompaction);
    }

    /**
     * 通知新文件已生成。
     */
    @Override
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "收到 {} 个新文件，来源于快照 {}，分区 {}，桶 {}",
                    files.size(),
                    snapshotId,
                    partition,
                    bucket);
        }
        write.notifyNewFiles(snapshotId, partition, bucket, files);
    }

    /**
     * 生成 Committable 以进行提交。
     */
    @Override
    public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = new ArrayList<>();
        if (write != null) {
            try {
                for (CommitMessage committable :
                        write.prepareCommit(this.waitCompaction || waitCompaction, checkpointId)) {
                    committables.add(
                            new Committable(checkpointId, Committable.Kind.FILE, committable));
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return committables;
    }

    /**
     * 处理快照状态（目前不执行任何操作）。
     */
    @Override
    public void snapshotState() throws Exception {
        // do nothing
    }

    /**
     * 判断是否处于流模式。
     */
    @Override
    public boolean streamingMode() {
        return isStreamingMode;
    }

    /**
     * 关闭 Sink，释放资源。
     */
    @Override
    public void close() throws Exception {
        if (write != null) {
            write.close();
        }
        paimonIOManager.close();
    }

    /**
     * 替换存储表，并恢复状态。
     */
    @Override
    public void replace(FileStoreTable newTable) throws Exception {
        if (commitUser == null) {
            return;
        }

        List<? extends FileStoreWrite.State<?>> states = write.checkpoint();
        write.close();
        write = newTableWrite(newTable);
        write.restore((List) states);
    }

    /**
     * 获取当前的 TableWriteImpl 实例（用于测试）。
     */
    @VisibleForTesting
    public TableWriteImpl<?> getWrite() {
        return write;
    }
}
