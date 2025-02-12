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

import org.apache.paimon.utils.Preconditions;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 用于为每个快照提交 {@link Committable} 的算子。
 * 支持流处理检查点机制和批处理场景的最终提交。
 *
 * @param <CommitT>      单个子任务的提交数据类型（例如文件路径列表）
 * @param <GlobalCommitT> 全局合并后的提交数据类型（例如全部分区路径）
 */
public class CommitterOperator<CommitT, GlobalCommitT> extends AbstractStreamOperator<CommitT>
        implements OneInputStreamOperator<CommitT, CommitT>, BoundedOneInput {

    private static final long serialVersionUID = 1L;
    // 标识输入结束的特殊检查点ID（Long最大值）
    private static final long END_INPUT_CHECKPOINT_ID = Long.MAX_VALUE;

    /** 缓存待提交数据，直到触发提交 */
    private final Deque<CommitT> inputs = new ArrayDeque<>();

    /** 是否启用流处理检查点机制 */
    private final boolean streamingCheckpointEnabled;

    /** 是否强制要求算子并行度为1（Paimon某些场景需要单并行提交） */
    private final boolean forceSingleParallelism;

    /** 初始提交者标识（用于作业首次启动） */
    private final String initialCommitUser;

    /** 按检查点ID分组的全局提交数据（使用有序Map便于范围查询） */
    protected final NavigableMap<Long, GlobalCommitT> committablesPerCheckpoint;

    /** 提交器工厂（负责创建组合提交数据的实例） */
    private final Committer.Factory<CommitT, GlobalCommitT> committerFactory;

    /** 提交状态管理器（负责状态持久化/恢复） */
    private final CommittableStateManager<GlobalCommitT> committableStateManager;

    /** 实际执行提交操作的组件 */
    protected Committer<CommitT, GlobalCommitT> committer;

    private transient long currentWatermark; // 当前水位线（事件时间）
    private transient boolean endInput;      // 标记输入是否已结束
    private transient String commitUser;     // 运行时使用的提交者标识（从状态恢复）
    private final Long endInputWatermark;    // 输入结束时的强制水位线（用于批处理）

    /**
     * 构造函数（简化版）
     */
    public CommitterOperator(
            boolean streamingCheckpointEnabled,
            boolean forceSingleParallelism,
            boolean chaining,
            String initialCommitUser,
            Committer.Factory<CommitT, GlobalCommitT> committerFactory,
            CommittableStateManager<GlobalCommitT> committableStateManager) {
        // 调用完整构造函数
        this(
                /* 参数透传 */
                streamingCheckpointEnabled,
                forceSingleParallelism,
                chaining,
                initialCommitUser,
                committerFactory,
                committableStateManager,
                null);
    }

    /**
     * 完整构造函数
     * @param endInputWatermark 强制设置的结束水位线（控制批处理提交时机）
     */
    public CommitterOperator(
            boolean streamingCheckpointEnabled,
            boolean forceSingleParallelism,
            boolean chaining,
            String initialCommitUser,
            Committer.Factory<CommitT, GlobalCommitT> committerFactory,
            CommittableStateManager<GlobalCommitT> committableStateManager,
            Long endInputWatermark) {
        this.streamingCheckpointEnabled = streamingCheckpointEnabled;
        this.forceSingleParallelism = forceSingleParallelism;
        this.initialCommitUser = initialCommitUser;
        this.committablesPerCheckpoint = new TreeMap<>(); // 有序Map便于范围提交
        this.committerFactory = checkNotNull(committerFactory);
        this.committableStateManager = committableStateManager;
        this.endInputWatermark = endInputWatermark;
        // 设置算子链策略（影响任务调度）
        setChainingStrategy(chaining ? ChainingStrategy.ALWAYS : ChainingStrategy.HEAD);
    }

    /**
     * 初始化状态（作业启动或恢复时调用）
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // 检查并行度约束（某些场景必须单并行度）
        Preconditions.checkArgument(
                !forceSingleParallelism || getRuntimeContext().getNumberOfParallelSubtasks() == 1,
                "Committer Operator parallelism in paimon MUST be one.");

        this.currentWatermark = Long.MIN_VALUE;
        this.endInput = false;
        // 从状态中恢复或初始化提交者标识（保证作业重启时一致性）
        commitUser = StateUtils.getSingleValueFromState(
                context, "commit_user_state", String.class, initialCommitUser);

        // 创建提交器实例（传入运行时上下文）
        committer = committerFactory.create(
                Committer.createContext(
                        commitUser,
                        getMetricGroup(),     // Flink指标收集
                        streamingCheckpointEnabled,
                        context.isRestored(), // 是否从检查点恢复
                        context.getOperatorStateStore()));

        // 初始化状态管理器（恢复历史提交状态）
        committableStateManager.initializeState(context, committer);
    }

    /**
     * 处理水位线事件（记录当前事件时间进度）
     */
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        // 忽略批处理结束的MAX水位线，防止干扰当前水位
        if (mark.getTimestamp() != Long.MAX_VALUE) {
            this.currentWatermark = mark.getTimestamp();
        }
    }

    /**
     * 将多个提交数据合并为全局提交对象
     */
    private GlobalCommitT toCommittables(long checkpoint, List<CommitT> inputs) throws Exception {
        return committer.combine(checkpoint, currentWatermark, inputs);
    }

    /**
     * 状态快照（检查点触发时保存状态）
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        // 将缓存的inputs按检查点分组并合并
        pollInputs();
        // 持久化当前所有待提交数据
        committableStateManager.snapshotState(context, committables(committablesPerCheckpoint));
    }

    /**
     * 提取有序Map中的提交数据集合
     */
    private List<GlobalCommitT> committables(NavigableMap<Long, GlobalCommitT> map) {
        return new ArrayList<>(map.values());
    }

    /**
     * 输入结束处理（批处理场景最终提交）
     */
    @Override
    public void endInput() throws Exception {
        endInput = true;
        // 设置强制水位线（控制批处理提交时机）
        if (endInputWatermark != null) {
            currentWatermark = endInputWatermark;
        }

        // 流处理模式由检查点触发提交，直接返回
        if (streamingCheckpointEnabled) {
            return;
        }

        // 批处理模式立即处理剩余数据
        pollInputs();
        commitUpToCheckpoint(END_INPUT_CHECKPOINT_ID);
    }

    /**
     * 检查点完成通知（提交对应检查点的数据）
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        // 如果是结束输入，使用特殊ID提交所有剩余数据
        commitUpToCheckpoint(endInput ? END_INPUT_CHECKPOINT_ID : checkpointId);
    }

    /**
     * 提交指定检查点之前的所有数据
     */
    private void commitUpToCheckpoint(long checkpointId) throws Exception {
        // 获取小于等于目标checkpointId的所有数据
        NavigableMap<Long, GlobalCommitT> headMap = committablesPerCheckpoint.headMap(checkpointId, true);
        List<GlobalCommitT> committables = committables(headMap);

        // 处理需要强制生成空提交的场景（某些存储系统要求）
        if (committables.isEmpty() && committer.forceCreatingSnapshot()) {
            committables = Collections.singletonList(toCommittables(checkpointId, Collections.emptyList()));
        }

        if (checkpointId == END_INPUT_CHECKPOINT_ID) {
            // In new versions of Flink, if a batch job fails, it might restart from some operator
            // in the middle.
            // If the job is restarted from the commit operator, endInput will be called again, and
            // the same commit messages will be committed again.
            // So when `endInput` is called, we must check if the corresponding snapshot exists.
            // However, if the snapshot does not exist, then append files must be new files. So
            // there is no need to check for duplicated append files.
            // 批处理提交需过滤重复文件（应对作业重启场景）
            committer.filterAndCommit(committables, false);
        } else {
            // 流处理直接提交
            committer.commit(committables);
        }
        headMap.clear(); // 提交成功后清除已提交数据
    }

    /**
     * 处理输入元素（透传数据并缓存）
     */
    @Override
    public void processElement(StreamRecord<CommitT> element) {
        output.collect(element); // 透传数据给下游
        this.inputs.add(element.getValue()); // 加入缓存等待提交
    }

    /**
     * 资源清理
     */
    @Override
    public void close() throws Exception {
        committablesPerCheckpoint.clear();
        inputs.clear();
        if (committer != null) {
            committer.close(); // 关闭提交器释放资源
        }
        super.close();
    }

    /**
     * 获取当前提交者标识（用于监控/调试）
     */
    public String getCommitUser() {
        return commitUser;
    }

    /**
     * 将缓存数据按检查点分组合并（核心预处理逻辑）
     */
    private void pollInputs() throws Exception {
        // 按检查点ID分组提交数据
        Map<Long, List<CommitT>> grouped = committer.groupByCheckpoint(inputs);

        for (Map.Entry<Long, List<CommitT>> entry : grouped.entrySet()) {
            Long cp = entry.getKey();
            List<CommitT> committables = entry.getValue();
            // To prevent the asynchronous completion of tasks with multiple concurrent bounded
            // stream inputs, which leads to some tasks passing a Committable with cp =
            // END_INPUT_CHECKPOINT_ID during the endInput method call of the current checkpoint,
            // while other tasks pass a Committable with END_INPUT_CHECKPOINT_ID during other
            // checkpoints hence causing an error here, we have a special handling for Committables
            // with END_INPUT_CHECKPOINT_ID: instead of throwing an error, we merge them.
            // 处理特殊结束检查点的合并场景
            if (cp != null && cp == END_INPUT_CHECKPOINT_ID
                    && committablesPerCheckpoint.containsKey(cp)) {
                // 合并多次输入的结束检查点数据
                GlobalCommitT combined = committer.combine(
                        cp, currentWatermark,
                        committablesPerCheckpoint.get(cp), committables);
                committablesPerCheckpoint.put(cp, combined);
            } else if (committablesPerCheckpoint.containsKey(cp)) {
                // 防止重复提交相同检查点（数据重复风险）
                throw new RuntimeException(
                        String.format(
                                "Repeatedly commit the same checkpoint files. \n"
                                        + "The previous files is %s, \n"
                                        + "and the subsequent files is %s",
                                committablesPerCheckpoint.get(cp), committables));
            } else {
                // 正常添加新检查点数据
                committablesPerCheckpoint.put(cp, toCommittables(cp, committables));
            }
        }
        inputs.clear(); // 清空缓存
    }
}

