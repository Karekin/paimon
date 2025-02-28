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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.flink.source.ContinuousFileSplitEnumerator;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.PendingSplitsCheckpoint;
import org.apache.paimon.flink.source.assigners.AlignedSplitAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.SnapshotNotExistPlan;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 一个对齐的连续监控型分片枚举器。它需要检查点与快照对齐。
 * <p>
 * 这里有两种对齐情况：
 * <ul>
 *   <li>快照已被消费，但检查点尚未触发：{@link AlignedSourceReader} 不会请求分片，直到检查点被触发。
 *   <li>检查点已被触发，但快照尚未生成：检查点将阻塞，直到上游表的快照被生成或超时。
 * </ul>
 */
public class AlignedContinuousFileSplitEnumerator extends ContinuousFileSplitEnumerator {

    private static final Logger LOG =
            LoggerFactory.getLogger(AlignedContinuousFileSplitEnumerator.class);

    private static final String PLACEHOLDER_SPLIT = "placeholder";
    private static final int MAX_PENDING_PLAN = 10;

    private final ArrayBlockingQueue<PlanWithNextSnapshotId> pendingPlans; // 待处理的计划队列

    private final AlignedSplitAssigner alignedAssigner; // 对齐的分片分配器

    private final long alignTimeout; // 对齐超时时间

    private final Object lock; // 锁对象，用于同步

    private long currentCheckpointId; // 当前检查点的 ID

    private Long lastConsumedSnapshotId; // 最后消费的快照 ID

    private boolean closed; // 枚举器是否已关闭

    public AlignedContinuousFileSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> remainSplits,
            @Nullable Long nextSnapshotId,
            long discoveryInterval,
            StreamTableScan scan,
            BucketMode bucketMode,
            long alignTimeout,
            int splitPerTaskMax,
            boolean shuffleBucketWithPartition) {
        super(
                context,
                remainSplits,
                nextSnapshotId,
                discoveryInterval,
                scan,
                bucketMode,
                splitPerTaskMax,
                shuffleBucketWithPartition);
        this.pendingPlans = new ArrayBlockingQueue<>(MAX_PENDING_PLAN); // 初始化待处理计划队列
        this.alignedAssigner = (AlignedSplitAssigner) super.splitAssigner; // 初始化对齐的分片分配器
        this.nextSnapshotId = nextSnapshotId; // 初始化下一个快照 ID
        this.alignTimeout = alignTimeout; // 初始化对齐超时时间
        this.lock = new Object(); // 初始化锁对象
        this.currentCheckpointId = Long.MIN_VALUE; // 初始化当前检查点 ID
        this.lastConsumedSnapshotId = null; // 初始化最后消费的快照 ID
        this.closed = false; // 初始化枚举器的关闭状态
    }

    @Override
    protected void addSplits(Collection<FileStoreSourceSplit> splits) {
        // 按照快照 ID 对分片进行分组
        Map<Long, List<FileStoreSourceSplit>> splitsBySnapshot = new TreeMap<>();
        for (FileStoreSourceSplit split : splits) {
            long snapshotId = ((DataSplit) split.split()).snapshotId();
            splitsBySnapshot.computeIfAbsent(snapshotId, snapshot -> new ArrayList<>()).add(split);
        }

        // 将分片分配给各个子任务
        for (List<FileStoreSourceSplit> previousSplits : splitsBySnapshot.values()) {
            Map<Integer, List<FileStoreSourceSplit>> subtaskSplits =
                    computeForBucket(previousSplits);
            subtaskSplits.forEach(
                    (subtask, taskSplits) ->
                            taskSplits.forEach(split -> splitAssigner.addSplit(subtask, split)));
        }
    }

    private Map<Integer, List<FileStoreSourceSplit>> computeForBucket(
            Collection<FileStoreSourceSplit> splits) {
        Map<Integer, List<FileStoreSourceSplit>> subtaskSplits = new HashMap<>();
        for (FileStoreSourceSplit split : splits) {
            subtaskSplits
                    .computeIfAbsent(assignSuggestedTask(split), subtask -> new ArrayList<>())
                    .add(split);
        }
        return subtaskSplits;
    }

    @Override
    public void close() throws IOException {
        closed = true; // 设置枚举器为关闭状态
        synchronized (lock) {
            lock.notifyAll(); // 通知所有等待的线程
        }
    }

    @Override
    public void addSplitsBack(List<FileStoreSourceSplit> splits, int subtaskId) {
        super.addSplitsBack(splits, subtaskId); // 调用父类方法将分片添加回队列
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) throws Exception {
        // 如果未对齐且枚举器未关闭，则等待快照
        if (!alignedAssigner.isAligned() && !closed) {
            synchronized (lock) {
                if (pendingPlans.isEmpty()) {
                    lock.wait(alignTimeout); // 等待对齐超时时间
                    Preconditions.checkArgument(!closed, "枚举器已被关闭。"); // 检查枚举器是否已关闭
                    Preconditions.checkArgument(
                            !pendingPlans.isEmpty(),
                            "等待来自 Paimon 数据源的快照时超时。"); // 检查是否有待处理计划
                }
            }
            // 处理等待的计划
            PlanWithNextSnapshotId pendingPlan = pendingPlans.poll();
            addSplits(splitGenerator.createSplits(Objects.requireNonNull(pendingPlan).plan()));
            nextSnapshotId = pendingPlan.nextSnapshotId();
            assignSplits();
        }
        Preconditions.checkArgument(alignedAssigner.isAligned()); // 确保已对齐
        lastConsumedSnapshotId = alignedAssigner.getNextSnapshotId(0).orElse(null);
        alignedAssigner.removeFirst(); // 移除第一个快照
        currentCheckpointId = checkpointId; // 设置当前检查点 ID

        // 发送检查点事件到源读取器
        CheckpointEvent event = new CheckpointEvent(checkpointId);
        for (int i = 0; i < context.currentParallelism(); i++) {
            context.sendEventToSourceReader(i, event);
        }
        return new PendingSplitsCheckpoint(alignedAssigner.remainingSplits(), nextSnapshotId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        if (currentCheckpointId == checkpointId) {
            throw new FlinkRuntimeException("对齐模式下不允许检查点失败。"); // 抛出异常
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        currentCheckpointId = Long.MIN_VALUE; // 重置当前检查点 ID
        Long nextSnapshot = lastConsumedSnapshotId == null ? null : lastConsumedSnapshotId + 1;
        scan.notifyCheckpointComplete(nextSnapshot); // 通知扫描检查点完成
    }

    // ------------------------------------------------------------------------

    @Override
    protected Optional<PlanWithNextSnapshotId> scanNextSnapshot() {
        if (pendingPlans.remainingCapacity() > 0) {
            Optional<PlanWithNextSnapshotId> scannedPlanOptional = super.scanNextSnapshot();
            if (scannedPlanOptional.isPresent()) {
                PlanWithNextSnapshotId scannedPlan = scannedPlanOptional.get();
                if (!(scannedPlan.plan() instanceof SnapshotNotExistPlan)) {
                    synchronized (lock) {
                        pendingPlans.add(scannedPlan); // 将计划添加到待处理队列
                        lock.notifyAll(); // 通知所有等待的线程
                    }
                }
            }
        }
        return Optional.empty();
    }

    @Override
    protected void processDiscoveredSplits(
            Optional<PlanWithNextSnapshotId> ignore, Throwable error) {
        if (error != null) {
            if (error instanceof EndOfScanException) {
                // 扫描完成
                LOG.debug("捕获 EndOfStreamException，流已结束。");
                finished = true;
            } else {
                LOG.error("枚举文件失败", error); // 记录错误日志
                throw new RuntimeException(error); // 抛出运行时异常
            }
        }

        if (alignedAssigner.remainingSnapshots() >= MAX_PENDING_PLAN) {
            assignSplits(); // 分配分片
            return;
        }

        PlanWithNextSnapshotId nextPlan = pendingPlans.poll(); // 从待处理队列中获取下一个计划
        if (nextPlan != null) {
            nextSnapshotId = nextPlan.nextSnapshotId();
            Objects.requireNonNull(nextSnapshotId); // 确保快照 ID 不为 null
            TableScan.Plan plan = nextPlan.plan();
            if (plan.splits().isEmpty()) {
                // 添加占位分片
                addSplits(
                        Collections.singletonList(
                                new FileStoreSourceSplit(
                                        PLACEHOLDER_SPLIT,
                                        new PlaceholderSplit(nextSnapshotId - 1))));
            } else {
                addSplits(splitGenerator.createSplits(plan)); // 添加分片
            }
        }
        assignSplits(); // 分配分片
    }

    @Override
    protected boolean noMoreSplits() {
        return super.noMoreSplits()
                && alignedAssigner.remainingSnapshots() == 0
                && pendingPlans.isEmpty(); // 检查是否有更多分片
    }

    @Override
    protected SplitAssigner createSplitAssigner(BucketMode bucketMode) {
        return new AlignedSplitAssigner(); // 创建对齐的分片分配器
    }
}
