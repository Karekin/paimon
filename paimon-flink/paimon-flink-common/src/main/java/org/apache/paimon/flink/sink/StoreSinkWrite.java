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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.table.sink.TableWriteImpl;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * {@link PrepareCommitOperator} 的辅助类，用于支持不同类型的 Paimon Sink。
 * 该接口定义了 Sink 写入操作的核心方法，包括数据写入、合并、快照、提交等功能。
 */
public interface StoreSinkWrite {

    /**
     * 当记录的“仅插入”状态发生变化时调用此方法。
     *
     * @param insertOnly 如果为 true，则所有后续记录都是 {@link org.apache.paimon.types.RowKind#INSERT}，
     *                   且不会有相同主键的两条记录。
     */
    void withInsertOnly(boolean insertOnly);

    /**
     * 将数据写入 Paimon 表。
     *
     * @param rowData 需要写入的数据行
     * @return 返回 SinkRecord 对象，如果写入失败或被过滤，则返回 null
     * @throws Exception 如果写入过程中发生错误
     */
    @Nullable
    SinkRecord write(InternalRow rowData) throws Exception;

    /**
     * 将数据写入 Paimon 表中的指定桶（Bucket）。
     *
     * @param rowData 需要写入的数据行
     * @param bucket  目标数据桶编号
     * @return 返回 SinkRecord 对象，如果写入失败或被过滤，则返回 null
     * @throws Exception 如果写入过程中发生错误
     */
    @Nullable
    SinkRecord write(InternalRow rowData, int bucket) throws Exception;

    /**
     * 将 SinkRecord 转换为日志存储格式。
     *
     * @param record 原始 SinkRecord 记录
     * @return 转换后的日志记录
     */
    SinkRecord toLogRecord(SinkRecord record);

    /**
     * 对 Paimon 表进行 Compact（合并操作）。
     * Compact 过程可以清理历史数据、合并小文件，提升查询性能。
     *
     * @param partition      目标分区
     * @param bucket         目标数据桶编号
     * @param fullCompaction 是否执行完全合并
     * @throws Exception 如果合并过程中发生错误
     */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /**
     * 通知新文件已生成。
     *
     * @param snapshotId 当前快照 ID
     * @param partition  目标分区
     * @param bucket     目标数据桶编号
     * @param files      新生成的数据文件列表
     */
    void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files);

    /**
     * 准备提交操作，返回需要提交的 Committable 对象。
     *
     * @param waitCompaction 是否等待 Compact 任务完成
     * @param checkpointId   当前检查点 ID
     * @return 需要提交的 Committable 记录列表
     * @throws IOException 如果提交过程中发生错误
     */
    List<Committable> prepareCommit(boolean waitCompaction, long checkpointId) throws IOException;

    /**
     * 对当前 Sink 状态进行快照。
     *
     * @throws Exception 如果快照过程中发生错误
     */
    void snapshotState() throws Exception;

    /**
     * 判断当前 Sink 是否运行在流模式（Streaming Mode）。
     *
     * @return 如果是流式模式返回 true，否则返回 false
     */
    boolean streamingMode();

    /**
     * 关闭 Sink 资源，释放相关内存和 IO 资源。
     *
     * @throws Exception 如果关闭过程中发生错误
     */
    void close() throws Exception;

    /**
     * 替换内部的 {@link TableWriteImpl} 实例，并将旧的写入状态迁移到新的写入实例中。
     *
     * <p>目前，此方法主要用于 CDC（Change Data Capture）Sink，以便处理表结构变更。
     * 当 Schema 发生变化时，新版本的 {@link TableWriteImpl} 由 {@code newWriteProvider} 提供。
     *
     * @param newTable 新的文件存储表实例
     * @throws Exception 如果替换过程中发生错误
     */
    void replace(FileStoreTable newTable) throws Exception;

    /**
     * {@link StoreSinkWrite} 的提供者接口，定义了创建 Sink 写入实例的方法。
     */
    @FunctionalInterface
    interface Provider extends Serializable {

        /**
         * 提供一个 {@link StoreSinkWrite} 实例。
         *
         * @param table         目标 Paimon 表
         * @param commitUser    提交用户（用于标识数据写入者）
         * @param state         Sink 的状态管理对象
         * @param ioManager     IO 资源管理器
         * @param memoryPool    （可选）内存池管理器
         * @param metricGroup   （可选）Flink 指标组
         * @return 返回 StoreSinkWrite 实例
         */
        StoreSinkWrite provide(
                FileStoreTable table,
                String commitUser,
                StoreSinkWriteState state,
                IOManager ioManager,
                @Nullable MemorySegmentPool memoryPool,
                @Nullable MetricGroup metricGroup);
    }

    /**
     * 使用指定的写入缓冲区（Write Buffer）创建 {@link StoreSinkWrite} 实例的提供者接口。
     */
    @FunctionalInterface
    interface WithWriteBufferProvider extends Serializable {

        /**
         * 提供一个 {@link StoreSinkWrite} 实例，并使用给定的写入缓冲区。
         *
         * @param table             目标 Paimon 表
         * @param commitUser        提交用户（用于标识数据写入者）
         * @param state             Sink 的状态管理对象
         * @param ioManager         IO 资源管理器
         * @param memoryPoolFactory 内存池工厂
         * @param metricGroup       Flink 指标组
         * @return 返回 StoreSinkWrite 实例
         */
        StoreSinkWrite provide(
                FileStoreTable table,
                String commitUser,
                StoreSinkWriteState state,
                IOManager ioManager,
                @Nullable MemoryPoolFactory memoryPoolFactory,
                MetricGroup metricGroup);
    }
}

