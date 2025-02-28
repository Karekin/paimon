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

package org.apache.paimon.flink.source;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.source.assigners.FIFOSplitAssigner;
import org.apache.paimon.flink.source.assigners.PreAssignSplitAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.SnapshotNotExistPlan;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 连续监控的切片枚举器。
 */
public class ContinuousFileSplitEnumerator
        implements SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileSplitEnumerator.class);

    // 分片枚举器上下文
    protected final SplitEnumeratorContext<FileStoreSourceSplit> context;

    // 切片发现间隔时间
    protected final long discoveryInterval;

    // 等待分配切片的读者集合
    protected final Set<Integer> readersAwaitingSplit;

    // 文件存储源切片生成器
    protected final FileStoreSourceSplitGenerator splitGenerator;

    // 流式表扫描
    protected final StreamTableScan scan;

    // 切片分配器
    protected final SplitAssigner splitAssigner;

    // 消费者进度计算器
    protected final ConsumerProgressCalculator consumerProgressCalculator;

    // 每个任务最大切片数
    private final int splitMaxNum;

    // 是否按分区和桶进行混合
    private final boolean shuffleBucketWithPartition;

    // 下一个快照 ID
    @Nullable protected Long nextSnapshotId;

    // 是否完成扫描
    protected boolean finished = false;

    // 是否停止触发扫描
    private boolean stopTriggerScan = false;

    /**
     * 构造函数。
     *
     * @param context 分片枚举器上下文
     * @param remainSplits 剩余切片
     * @param nextSnapshotId 下一个快照 ID
     * @param discoveryInterval 切片发现间隔
     * @param scan 流式表扫描
     * @param bucketMode 数据桶模式
     * @param splitMaxPerTask 每个任务最大切片数
     * @param shuffleBucketWithPartition 是否按分区和桶进行混合
     */
    public ContinuousFileSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> remainSplits,
            @Nullable Long nextSnapshotId,
            long discoveryInterval,
            StreamTableScan scan,
            BucketMode bucketMode,
            int splitMaxPerTask,
            boolean shuffleBucketWithPartition) {
        checkArgument(discoveryInterval > 0L);
        this.context = checkNotNull(context);
        this.nextSnapshotId = nextSnapshotId;
        this.discoveryInterval = discoveryInterval;
        this.readersAwaitingSplit = new LinkedHashSet<>();
        this.splitGenerator = new FileStoreSourceSplitGenerator();
        this.scan = scan;
        this.splitAssigner = createSplitAssigner(bucketMode);
        this.splitMaxNum = context.currentParallelism() * splitMaxPerTask;
        this.shuffleBucketWithPartition = shuffleBucketWithPartition;
        // 添加剩余切片
        addSplits(remainSplits);

        this.consumerProgressCalculator =
                new ConsumerProgressCalculator(context.currentParallelism());
    }

    /**
     * 启用触发扫描。
     */
    @VisibleForTesting
    void enableTriggerScan() {
        this.stopTriggerScan = false;
    }

    /**
     * 添加切片。
     *
     * @param splits 切片集合
     */
    protected void addSplits(Collection<FileStoreSourceSplit> splits) {
        splits.forEach(this::addSplit);
    }

    /**
     * 添加单个切片。
     *
     * @param split 切片
     */
    private void addSplit(FileStoreSourceSplit split) {
        splitAssigner.addSplit(assignSuggestedTask(split), split);
    }

    @Override
    public void start() {
        // 异步调用扫描下一个快照，并处理发现的切片
        context.callAsync(
                this::scanNextSnapshot, this::processDiscoveredSplits, 0, discoveryInterval);
    }

    @Override
    public void close() throws IOException {
        // 无资源需要关闭
    }

    @Override
    public void addReader(int subtaskId) {
        // 这个源是基于完全的懒拉机制，注册时不需要做任何事情
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // 将请求切片的子任务 ID 添加到等待队列中
        readersAwaitingSplit.add(subtaskId);
        // 分配切片
        assignSplits();
        // 如果当前任务未分配到切片，则继续扫描
        if (readersAwaitingSplit.contains(subtaskId)) {
            if (stopTriggerScan) {
                return;
            }
            stopTriggerScan = true;
            // 异步调用扫描下一个快照，并处理发现的切片
            context.callAsync(this::scanNextSnapshot, this::processDiscoveredSplits);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        // 处理来自源的事件
        if (sourceEvent instanceof ReaderConsumeProgressEvent) {
            consumerProgressCalculator.updateConsumeProgress(
                    subtaskId, (ReaderConsumeProgressEvent) sourceEvent);
        } else {
            LOG.error("Received unrecognized event: {}", sourceEvent);
        }
    }

    @Override
    public void addSplitsBack(List<FileStoreSourceSplit> splits, int subtaskId) {
        LOG.debug("File Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplitsBack(subtaskId, splits);
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) throws Exception {
        // 获取剩余切片
        List<FileStoreSourceSplit> splits = new ArrayList<>(splitAssigner.remainingSplits());
        // 创建检查点
        final PendingSplitsCheckpoint checkpoint =
                new PendingSplitsCheckpoint(splits, nextSnapshotId);

        // 更新消费者进度计算器
        consumerProgressCalculator.notifySnapshotState(
                checkpointId,
                readersAwaitingSplit,
                subtask -> splitAssigner.getNextSnapshotId(subtask).orElse(nextSnapshotId),
                context.currentParallelism());

        LOG.debug("Source Checkpoint is {}", checkpoint);
        return checkpoint;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // 通知检查点完成
        consumerProgressCalculator
                .notifyCheckpointComplete(checkpointId)
                .ifPresent(scan::notifyCheckpointComplete);
    }

    // ------------------------------------------------------------------------

    // 这个方法需要同步，因为 scan 对象不是线程安全的
    protected synchronized Optional<PlanWithNextSnapshotId> scanNextSnapshot() {
        // 如果剩余切片数量超过最大限制，直接返回
        if (splitAssigner.numberOfRemainingSplits() >= splitMaxNum) {
            return Optional.empty();
        }
        // 获取下一个快照计划
        TableScan.Plan plan = scan.plan();
        // 获取下一个快照 ID
        Long nextSnapshotId = scan.checkpoint();
        return Optional.of(new PlanWithNextSnapshotId(plan, nextSnapshotId));
    }

    // 这个方法不能同步，因为它运行在协调线程中，会导致线程阻塞
    protected void processDiscoveredSplits(
            Optional<PlanWithNextSnapshotId> planWithNextSnapshotIdOptional, Throwable error) {
        if (error != null) {
            // 如果是 EndOfScanException，说明流已结束
            if (error instanceof EndOfScanException) {
                LOG.debug("Catching EndOfStreamException, the stream is finished.");
                finished = true;
                // 分配剩余切片
                assignSplits();
            } else {
                // 其他错误，抛出异常
                LOG.error("Failed to enumerate files", error);
                throw new RuntimeException(error);
            }
            return;
        }

        if (!planWithNextSnapshotIdOptional.isPresent()) {
            return;
        }
        // 获取计划和下一个快照 ID
        PlanWithNextSnapshotId planWithNextSnapshotId = planWithNextSnapshotIdOptional.get();
        nextSnapshotId = planWithNextSnapshotId.nextSnapshotId;
        TableScan.Plan plan = planWithNextSnapshotId.plan;
        // 如果计划中没有切片，停止触发扫描
        if (plan.equals(SnapshotNotExistPlan.INSTANCE)) {
            stopTriggerScan = true;
            return;
        }

        stopTriggerScan = false;
        // 如果计划中没有切片，直接返回
        if (plan.splits().isEmpty()) {
            return;
        }

        // 添加新发现的切片
        addSplits(splitGenerator.createSplits(plan));
        // 分配切片
        assignSplits();
    }

    /**
     * 分配切片的方法。
     * 该方法需要同步，因为 handleSplitRequest 和 processDiscoveredSplits 存在线程冲突。
     */
    protected synchronized void assignSplits() {
        // 创建切片分配
        Map<Integer, List<FileStoreSourceSplit>> assignment = new HashMap<>();
        Iterator<Integer> readersAwait = readersAwaitingSplit.iterator();
        // 获取已注册的读者
        Set<Integer> subtaskIds = context.registeredReaders().keySet();
        while (readersAwait.hasNext()) {
            Integer task = readersAwait.next();
            if (!subtaskIds.contains(task)) {
                readersAwait.remove();
                continue;
            }
            // 获取下一个切片
            List<FileStoreSourceSplit> splits = splitAssigner.getNext(task, null);
            if (!splits.isEmpty()) {
                assignment.put(task, splits);
                // 更新消费者进度信息
                consumerProgressCalculator.updateAssignInformation(task, splits.get(0));
            }
        }

        // 如果没有更多切片，通知读者
        if (noMoreSplits()) {
            Iterator<Integer> iterator = readersAwaitingSplit.iterator();
            while (iterator.hasNext()) {
                Integer reader = iterator.next();
                if (!assignment.containsKey(reader)) {
                    context.signalNoMoreSplits(reader);
                    iterator.remove();
                }
            }
        }
        // 移除已分配的读者
        assignment.keySet().forEach(readersAwaitingSplit::remove);
        // 分配切片
        context.assignSplits(new SplitsAssignment<>(assignment));
    }

    /**
     * 为切片分配建议的子任务 ID。
     *
     * @param split 切片
     * @return 子任务 ID
     */
    protected int assignSuggestedTask(FileStoreSourceSplit split) {
        DataSplit dataSplit = ((DataSplit) split.split());
        if (shuffleBucketWithPartition) {
            return ChannelComputer.select(
                    dataSplit.partition(), dataSplit.bucket(), context.currentParallelism());
        }
        return ChannelComputer.select(dataSplit.bucket(), context.currentParallelism());
    }

    /**
     * 创建切片分配器。
     *
     * @param bucketMode 数据桶模式
     * @return 切片分配器
     */
    protected SplitAssigner createSplitAssigner(BucketMode bucketMode) {
        return bucketMode == BucketMode.BUCKET_UNAWARE
                ? new FIFOSplitAssigner(Collections.emptyList())
                : new PreAssignSplitAssigner(1, context, Collections.emptyList());
    }

    /**
     * 判断是否还有切片需要分配。
     *
     * @return 是否还有切片
     */
    protected boolean noMoreSplits() {
        return finished;
    }

    /**
     * 扫描结果类，包含计划和下一个快照 ID。
     */
    protected static class PlanWithNextSnapshotId {
        private final TableScan.Plan plan;
        private final Long nextSnapshotId;

        public PlanWithNextSnapshotId(TableScan.Plan plan, Long nextSnapshotId) {
            this.plan = plan;
            this.nextSnapshotId = nextSnapshotId;
        }

        public TableScan.Plan plan() {
            return plan;
        }

        public Long nextSnapshotId() {
            return nextSnapshotId;
        }
    }
}
