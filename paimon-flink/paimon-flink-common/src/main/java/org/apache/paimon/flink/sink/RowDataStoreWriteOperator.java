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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.log.LogWriteCallback;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * 一个用于写入 {@link InternalRow} 的提交准备算子。记录模式（schema）固定不变。
 *
 * <p>该类继承自 {@link TableWriteOperator}，专注于处理 {@link InternalRow} 类型数据的存储写入，
 * 支持可选的日志系统（如CDC）记录变更日志。</p>
 */
public class RowDataStoreWriteOperator extends TableWriteOperator<InternalRow> {

    // 序列化版本号，用于确保序列化一致性
    private static final long serialVersionUID = 3L;

    @Nullable
    private final LogSinkFunction logSinkFunction; // 可选日志系统Sink函数（如写入Kafka）
    private transient SimpleContext sinkContext;    // 写入上下文，提供时间信息
    @Nullable
    private transient LogWriteCallback logCallback; // 日志写入回调，用于获取写入偏移量

    /** 当前水位线（watermark），因无定时器服务需自行监听 */
    private long currentWatermark = Long.MIN_VALUE;

    /**
     * 构造函数
     *
     * @param table                  目标表（文件存储类型）
     * @param logSinkFunction        日志系统Sink函数（可为null）
     * @param storeSinkWriteProvider 存储写入提供器
     * @param initialCommitUser      初始提交者标识（用于事务提交）
     */
    public RowDataStoreWriteOperator(
            FileStoreTable table,
            @Nullable LogSinkFunction logSinkFunction,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(table, storeSinkWriteProvider, initialCommitUser);
        this.logSinkFunction = logSinkFunction;
    }

    /**
     * 初始化算子，设置运行时上下文
     */
    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Committable>> output) {
        super.setup(containingTask, config, output);
        // 若存在日志Sink函数，为其设置运行时上下文
        if (logSinkFunction != null) {
            FunctionUtils.setFunctionRuntimeContext(logSinkFunction, getRuntimeContext());
        }
    }

    /**
     * 初始化状态（如从检查点恢复）
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        // 恢复日志Sink函数的状态
        if (logSinkFunction != null) {
            StreamingFunctionUtils.restoreFunctionState(context, logSinkFunction);
        }
    }

    /**
     * 检查是否包含日志系统
     */
    @Override
    protected boolean containLogSystem() {
        return logSinkFunction != null;
    }

    /**
     * 打开算子资源，初始化日志系统
     */
    @Override
    public void open() throws Exception {
        super.open();

        // 创建写入上下文（用于提供处理时间等）
        this.sinkContext = new SimpleContext(getProcessingTimeService());

        // 初始化日志Sink函数
        if (logSinkFunction != null) {
            // 兼容Flink 1.18之前的RichFunction接口
            if (logSinkFunction instanceof RichFunction) {
                RichFunction richFunction = (RichFunction) logSinkFunction;
                richFunction.open(new Configuration()); // 使用空配置开启
            }

            // 设置日志写入回调
            logCallback = new LogWriteCallback();
            logSinkFunction.setWriteCallback(logCallback);
        }
    }

    /**
     * 处理水位线事件
     */
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);

        // 更新当前水位线并传递至日志Sink
        this.currentWatermark = mark.getTimestamp();
        if (logSinkFunction != null) {
            logSinkFunction.writeWatermark(
                    new org.apache.flink.api.common.eventtime.Watermark(mark.getTimestamp()));
        }
    }

    /**
     * 处理数据元素
     */
    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        // 提取记录的时间戳（事件时间或处理时间）
        sinkContext.timestamp = element.hasTimestamp() ? element.getTimestamp() : null;

        // 写入主存储系统
        SinkRecord record;
        try {
            record = write.write(element.getValue());
        } catch (Exception e) {
            throw new IOException(e);
        }

        // 若记录非空且启用了日志系统，同时写入日志存储
        if (record != null && logSinkFunction != null) {
            // 转换为日志记录格式（保留原始主键包含的分区字段）
            SinkRecord logRecord = write.toLogRecord(record);
            logSinkFunction.invoke(logRecord, sinkContext);
        }
    }

    /**
     * 快照当前状态（检查点机制）
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        // 快照日志Sink函数的状态
        if (logSinkFunction != null) {
            StreamingFunctionUtils.snapshotFunctionState(
                    context, getOperatorStateBackend(), logSinkFunction);
        }
    }

    /**
     * 结束写入操作（如批处理的最后阶段）
     */
    @Override
    public void finish() throws Exception {
        super.finish();
        // 通知日志Sink函数结束写入
        if (logSinkFunction != null) {
            logSinkFunction.finish();
        }
    }

    /**
     * 关闭算子，释放资源
     */
    @Override
    public void close() throws Exception {
        super.close();
        // 关闭日志Sink函数
        if (logSinkFunction != null) {
            FunctionUtils.closeFunction(logSinkFunction);
        }
    }

    /**
     * 检查点完成通知（用于精确一次语义）
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        // 传递通知至日志Sink函数（如果支持检查点监听）
        if (logSinkFunction instanceof CheckpointListener) {
            ((CheckpointListener) logSinkFunction).notifyCheckpointComplete(checkpointId);
        }
    }

    /**
     * 检查点中止通知
     */
    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        // 传递通知至日志Sink函数
        if (logSinkFunction instanceof CheckpointListener) {
            ((CheckpointListener) logSinkFunction).notifyCheckpointAborted(checkpointId);
        }
    }

    /**
     * 准备提交事务，生成可提交对象列表
     */
    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = super.prepareCommit(waitCompaction, checkpointId);

        // 处理日志系统的提交
        if (logCallback != null) {
            try {
                Objects.requireNonNull(logSinkFunction).flush(); // 确保日志数据刷出
            } catch (Exception e) {
                throw new IOException(e);
            }
            // 将日志偏移量加入提交列表
            logCallback
                    .offsets()
                    .forEach(
                            (k, v) ->
                                    committables.add(
                                            new Committable(
                                                    checkpointId,
                                                    Committable.Kind.LOG_OFFSET,
                                                    new LogOffsetCommittable(k, v))));
        }

        return committables;
    }

    /**
     * 自定义Sink上下文实现，提供时间信息
     */
    private class SimpleContext implements SinkFunction.Context {
        @Nullable
        private Long timestamp; // 当前记录的时间戳

        private final ProcessingTimeService processingTimeService; // 处理时间服务

        public SimpleContext(ProcessingTimeService processingTimeService) {
            this.processingTimeService = processingTimeService;
        }

        @Override
        public long currentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime(); // 当前处理时间
        }

        @Override
        public long currentWatermark() {
            return currentWatermark; // 当前水位线（事件时间进度）
        }

        @Override
        public Long timestamp() {
            return timestamp; // 当前记录的时间戳（事件时间）
        }
    }
}

