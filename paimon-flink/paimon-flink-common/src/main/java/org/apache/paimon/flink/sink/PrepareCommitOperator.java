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

import org.apache.paimon.flink.memory.FlinkMemorySegmentPool;
import org.apache.paimon.flink.memory.MemorySegmentAllocator;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.Options;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.flink.utils.ManagedMemoryUtils.computeManagedMemory;

/**
 * 准备提交（Prepare Commit）运算符，负责生成并发出 {@link Committable}。
 * 该运算符用于管理数据提交前的准备工作，如缓存、管理内存、生成提交信息等。
 */
public abstract class PrepareCommitOperator<IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** （可选）内存段池，用于管理数据缓冲区的内存 */
    @Nullable
    protected transient MemorySegmentPool memoryPool;

    /** （可选）内存段分配器，负责管理 Flink 任务中的内存分配 */
    @Nullable
    private transient MemorySegmentAllocator memoryAllocator;

    /** 传入的配置信息，用于控制提交逻辑 */
    private final Options options;

    /** 标志数据流是否已结束 */
    private boolean endOfInput = false;

    /**
     * 构造方法，初始化 PrepareCommitOperator。
     *
     * @param options 传入的配置项，用于控制 Sink 相关行为
     */
    public PrepareCommitOperator(Options options) {
        this.options = options;
        // 设置算子链策略，确保该算子与上游算子进行链式执行，提高效率
        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    /**
     * 初始化算子，设置内存管理器和分配器（如果启用了托管内存）。
     *
     * @param containingTask  所属的 Flink 任务
     * @param config         流算子配置
     * @param output         输出流
     */
    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);

        // 如果启用了 SINK 计算任务托管内存，则初始化内存管理器
        if (options.get(SINK_USE_MANAGED_MEMORY)) {
            MemoryManager memoryManager = containingTask.getEnvironment().getMemoryManager();
            memoryAllocator = new MemorySegmentAllocator(containingTask, memoryManager);
            memoryPool =
                    new FlinkMemorySegmentPool(
                            computeManagedMemory(this),
                            memoryManager.getPageSize(),
                            memoryAllocator);
        }
    }

    /**
     * 在检查点前触发提交，确保数据一致性。
     *
     * @param checkpointId 当前检查点 ID
     * @throws Exception 如果提交过程中发生错误
     */
    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        if (!endOfInput) {
            emitCommittables(false, checkpointId);
        }
        // 由于数据流已结束，此时不应再有记录被提交
    }

    /**
     * 处理输入数据流的结束信号，触发最终的提交。
     *
     * @throws Exception 如果提交过程中发生错误
     */
    @Override
    public void endInput() throws Exception {
        endOfInput = true;
        // 在数据流结束后，使用最大 ID 触发最终提交
        emitCommittables(true, Long.MAX_VALUE);
    }

    /**
     * 关闭算子，释放内存资源。
     *
     * @throws Exception 如果关闭过程中发生错误
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (memoryAllocator != null) {
            memoryAllocator.release();
        }
    }

    /**
     * 将数据发送给 CommitterOperator 进行最终提交。
     *
     * @param waitCompaction 是否等待数据合并（Compaction）
     * @param checkpointId 当前检查点 ID
     * @throws IOException 如果写入过程中发生错误
     */
    private void emitCommittables(boolean waitCompaction, long checkpointId) throws IOException {
        // 准备提交数据，并将其作为流记录发送到下游
        prepareCommit(waitCompaction, checkpointId)
                .forEach(committable -> output.collect(new StreamRecord<>(committable)));
    }

    /**
     * 需要由具体实现类定义的方法，用于准备提交数据（Committable）。
     *
     * @param waitCompaction 是否等待数据合并（Compaction）
     * @param checkpointId 当前检查点 ID
     * @return 提交记录的列表
     * @throws IOException 如果提交过程中发生错误
     */
    protected abstract List<OUT> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException;
}

