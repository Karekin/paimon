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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.snapshot.AllDeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.BoundedChecker;
import org.apache.paimon.table.source.snapshot.CompactionChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.DeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.FollowUpScanner;
import org.apache.paimon.table.source.snapshot.InputChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingContext;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner.ScannedResult;
import org.apache.paimon.table.source.snapshot.StaticFromSnapshotStartingScanner;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.NextSnapshotFetcher;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.apache.paimon.CoreOptions.ChangelogProducer.FULL_COMPACTION;

/**
 * 流式规划的 {@link StreamTableScan} 实现。
 * 此类用于流式数据表的扫描逻辑，支持流式读取和增量处理。
 *
 * TableScan是Flink中的一个核心接口，用于描述如何从外部系统中读取数据。抽象类AbstractDataTableScan提供了基本的实现逻辑，而DataTableStreamScan则基于它实现了更具体的流式扫描逻辑。
 */
public class DataTableStreamScan extends AbstractDataTableScan implements StreamDataTableScan {

    /** 日志记录器，用于记录日志信息 */
    private static final Logger LOG = LoggerFactory.getLogger(DataTableStreamScan.class);

    /** 核心选项，包含数据表的读取配置等 */
    private final CoreOptions options;

    /** 快照管理器，用于管理快照（数据状态的快照） */
    private final SnapshotManager snapshotManager;

    /** 是否支持流式读取覆盖写入 */
    private final boolean supportStreamingReadOverwrite;

    /** 默认值分配器，用于处理数据中的默认值 */
    private final DefaultValueAssigner defaultValueAssigner;

    /** 下一个快照的提供者，用于获取后续的快照 */
    private final NextSnapshotFetcher nextSnapshotProvider;

    /** 是否已经初始化 */
    private boolean initialized = false;

    /** 初始扫描器，用于首次扫描快照 */
    private StartingScanner startingScanner;

    /** 跟进扫描器，用于后续的快照扫描 */
    private FollowUpScanner followUpScanner;

    /** 边界检查器，用于检查输入是否到达边界 */
    private BoundedChecker boundedChecker;

    /** 是否已完成全阶段扫描 */
    private boolean isFullPhaseEnd = false;

    /** 当前水位线（用于流式处理的边界标记） */
    @Nullable private Long currentWatermark;

    /** 下一个快照的ID */
    @Nullable private Long nextSnapshotId;

    /** 扫描延迟时间（单位：毫秒） */
    @Nullable private Long scanDelayMillis;

    /**
     * 构造函数，初始化流式数据表扫描器。
     *
     * @param options 核心选项
     * @param snapshotReader 快照读取器
     * @param snapshotManager 快照管理器
     * @param supportStreamingReadOverwrite 是否支持流式读取覆盖写入
     * @param defaultValueAssigner 默认值分配器
     */
    public DataTableStreamScan(
            CoreOptions options,
            SnapshotReader snapshotReader,
            SnapshotManager snapshotManager,
            boolean supportStreamingReadOverwrite,
            DefaultValueAssigner defaultValueAssigner) {
        // 调用父类构造函数
        super(options, snapshotReader);
        // 初始化核心选项
        this.options = options;
        // 初始化快照管理器
        this.snapshotManager = snapshotManager;
        // 初始化流式读取覆盖写入支持标志
        this.supportStreamingReadOverwrite = supportStreamingReadOverwrite;
        // 初始化默认值分配器
        this.defaultValueAssigner = defaultValueAssigner;
        // 初始化下一个快照提供者
        this.nextSnapshotProvider =
                new NextSnapshotFetcher(snapshotManager, options.changelogLifecycleDecoupled());
    }

    /**
     * 应用过滤器，并返回当前扫描器。
     *
     * @param predicate 过滤器条件
     * @return 当前扫描器实例
     */
    @Override
    public DataTableStreamScan withFilter(Predicate predicate) {
        // 使用默认值分配器处理过滤条件
        snapshotReader.withFilter(defaultValueAssigner.handlePredicate(predicate));
        // 返回当前实例
        return this;
    }

    /**
     * 获取起始上下文。
     *
     * @return 起始上下文实例
     */
    @Override
    public StartingContext startingContext() {
        // 如果未初始化，则初始化扫描器
        if (!initialized) {
            initScanner();
        }
        // 返回初始扫描器的起始上下文
        return startingScanner.startingContext();
    }

    /**
     * 生成扫描计划。
     *
     * @return 扫描计划
     */
    @Override
    public Plan plan() {
        // 如果未初始化，则初始化扫描器
        if (!initialized) {
            initScanner();
        }

        // 如果没有下一个快照ID，则尝试生成第一个扫描计划
        if (nextSnapshotId == null) {
            return tryFirstPlan();
        } else {
            // 否则生成后续的扫描计划
            return nextPlan();
        }
    }

    /**
     * 初始化扫描器。
     */
    private void initScanner() {
        // 初始化初始扫描器
        if (startingScanner == null) {
            startingScanner = createStartingScanner(true);
        }
        // 初始化跟进扫描器
        if (followUpScanner == null) {
            followUpScanner = createFollowUpScanner();
        }
        // 初始化边界检查器
        if (boundedChecker == null) {
            boundedChecker = createBoundedChecker();
        }
        // 初始化扫描延迟时间
        if (scanDelayMillis == null) {
            scanDelayMillis = getScanDelayMillis();
        }
        // 标记为已初始化
        initialized = true;
    }

    /**
     * 尝试生成第一个扫描计划。
     *
     * @return 扫描计划
     */
    private Plan tryFirstPlan() {
        StartingScanner.Result result;
        // 根据不同的配置条件，执行不同的快照扫描逻辑
        if (options.needLookup()) {
            // 需要查找时，应用过滤条件
            result = startingScanner.scan(snapshotReader.withLevelFilter(level -> level > 0));
            snapshotReader.withLevelFilter(Filter.alwaysTrue());
        } else if (options.changelogProducer().equals(FULL_COMPACTION)) {
            // 如果变更日志生产者是全量压缩，则扫描最高层级的快照
            result =
                    startingScanner.scan(
                            snapshotReader.withLevelFilter(
                                    level -> level == options.numLevels() - 1));
            snapshotReader.withLevelFilter(Filter.alwaysTrue());
        } else {
            // 其他情况，正常扫描
            result = startingScanner.scan(snapshotReader);
        }

        // 处理扫描结果
        if (result instanceof ScannedResult) {
            ScannedResult scannedResult = (ScannedResult) result;
            // 更新当前水位线和当前快照ID
            currentWatermark = scannedResult.currentWatermark();
            long currentSnapshotId = scannedResult.currentSnapshotId();
            LookupStrategy lookupStrategy = options.lookupStrategy();
            if (!lookupStrategy.produceChangelog && lookupStrategy.deletionVector) {
                // 如果是删除向量模式，则设置下一个快照ID为当前快照ID
                nextSnapshotId = currentSnapshotId;
            } else {
                nextSnapshotId = currentSnapshotId + 1;
            }
            // 检查是否到达输入边界
            isFullPhaseEnd =
                    boundedChecker.shouldEndInput(snapshotManager.snapshot(currentSnapshotId));
            // 返回扫描结果计划
            return scannedResult.plan();
        } else if (result instanceof StartingScanner.NextSnapshot) {
            // 如果结果是下一个快照，则更新下一个快照ID
            nextSnapshotId = ((StartingScanner.NextSnapshot) result).nextSnapshotId();
            // 检查是否到达输入边界
            isFullPhaseEnd =
                    snapshotManager.snapshotExists(nextSnapshotId - 1)
                            && boundedChecker.shouldEndInput(
                            snapshotManager.snapshot(nextSnapshotId - 1));
        }
        // 如果无法找到快照，则返回占位计划
        return SnapshotNotExistPlan.INSTANCE;
    }

    /**
     * 生成后续的扫描计划。
     *
     * @return 扫描计划
     */
    private Plan nextPlan() {
        while (true) {
            // 如果到达输入边界，则抛出扫描结束异常
            if (isFullPhaseEnd) {
                throw new EndOfScanException();
            }

            // 获取下一个快照
            Snapshot snapshot = nextSnapshotProvider.getNextSnapshot(nextSnapshotId);
            if (snapshot == null) {
                // 如果没有快照，则返回占位计划
                return SnapshotNotExistPlan.INSTANCE;
            }

            // 检查是否到达输入边界
            if (boundedChecker.shouldEndInput(snapshot)) {
                throw new EndOfScanException();
            }

            // 检查是否需要延迟扫描
            if (shouldDelaySnapshot(nextSnapshotId)) {
                return SnapshotNotExistPlan.INSTANCE;
            }

            // 处理覆盖写入快照
            if (snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE
                    && supportStreamingReadOverwrite) {
                LOG.debug("找到覆盖快照ID {}。", nextSnapshotId);
                // 获取覆盖变更计划
                SnapshotReader.Plan overwritePlan =
                        followUpScanner.getOverwriteChangesPlan(snapshot, snapshotReader);
                currentWatermark = overwritePlan.watermark();
                nextSnapshotId++;
                return overwritePlan;
            } else if (followUpScanner.shouldScanSnapshot(snapshot)) {
                // 扫描快照并生成计划
                LOG.debug("找到快照ID {}。", nextSnapshotId);
                SnapshotReader.Plan plan = followUpScanner.scan(snapshot, snapshotReader);
                currentWatermark = plan.watermark();
                nextSnapshotId++;
                return plan;
            } else {
                // 快照不需要扫描，继续下一个快照ID
                nextSnapshotId++;
            }
        }
    }

    /**
     * 检查是否需要延迟扫描快照。
     *
     * @param snapshotId 快照ID
     * @return 是否需要延迟
     */
    private boolean shouldDelaySnapshot(long snapshotId) {
        if (scanDelayMillis == null) {
            return false;
        }

        long snapshotMills = System.currentTimeMillis() - scanDelayMillis;
        // 检查快照是否存在，并且时间是否在允许范围内
        if (snapshotManager.snapshotExists(snapshotId)
                && snapshotManager.snapshot(snapshotId).timeMillis() > snapshotMills) {
            return true;
        }
        return false;
    }

    /**
     * 创建跟进扫描器。
     *
     * @return 跟进扫描器实例
     */
    private FollowUpScanner createFollowUpScanner() {
        CoreOptions.StreamScanMode type =
                options.toConfiguration().get(CoreOptions.STREAM_SCAN_MODE);
        // 根据流式扫描模式创建不同的跟进扫描器
        switch (type) {
            case COMPACT_BUCKET_TABLE:
                return new DeltaFollowUpScanner();
            case FILE_MONITOR:
                return new AllDeltaFollowUpScanner();
        }

        CoreOptions.ChangelogProducer changelogProducer = options.changelogProducer();
        FollowUpScanner followUpScanner;
        switch (changelogProducer) {
            case NONE:
                followUpScanner = new DeltaFollowUpScanner();
                break;
            case INPUT:
                followUpScanner = new InputChangelogFollowUpScanner();
                break;
            case FULL_COMPACTION:
            case LOOKUP:
                followUpScanner = new CompactionChangelogFollowUpScanner();
                break;
            default:
                throw new UnsupportedOperationException(
                        "未知的变更日志生产者 " + changelogProducer.name());
        }
        return followUpScanner;
    }

    /**
     * 创建边界检查器。
     *
     * @return 边界检查器实例
     */
    private BoundedChecker createBoundedChecker() {
        Long boundedWatermark = options.scanBoundedWatermark();
        // 根据有界水位线创建不同的边界检查器
        return boundedWatermark != null
                ? BoundedChecker.watermark(boundedWatermark)
                : BoundedChecker.neverEnd();
    }

    /**
     * 获取扫描延迟时间（毫秒）。
     *
     * @return 扫描延迟时间（毫秒）
     */
    private Long getScanDelayMillis() {
        return options.streamingReadDelay() == null
                ? null
                : options.streamingReadDelay().toMillis();
    }

    /**
     * 获取检查点位置。
     *
     * @return 检查点位置（当前快照ID）
     */
    @Nullable
    @Override
    public Long checkpoint() {
        return nextSnapshotId;
    }

    /**
     * 获取当前水位线。
     *
     * @return 当前水位线
     */
    @Nullable
    @Override
    public Long watermark() {
        return currentWatermark;
    }

    /**
     * 恢复扫描状态。
     *
     * @param nextSnapshotId 下一个快照ID
     */
    @Override
    public void restore(@Nullable Long nextSnapshotId) {
        this.nextSnapshotId = nextSnapshotId;
    }

    /**
     * 恢复扫描状态，并设置是否扫描所有快照。
     *
     * @param nextSnapshotId 下一个快照ID
     * @param scanAllSnapshot 是否扫描所有快照
     */
    @Override
    public void restore(@Nullable Long nextSnapshotId, boolean scanAllSnapshot) {
        if (nextSnapshotId != null && scanAllSnapshot) {
            // 如果需要扫描所有快照，则初始化静态从快照扫描器
            startingScanner =
                    new StaticFromSnapshotStartingScanner(snapshotManager, nextSnapshotId);
            restore(null);
        } else {
            restore(nextSnapshotId);
        }
    }

    /**
     * 通知检查点完成。
     *
     * @param nextSnapshot 下一个快照ID
     */
    @Override
    public void notifyCheckpointComplete(@Nullable Long nextSnapshot) {
        if (nextSnapshot == null) {
            return;
        }

        String consumerId = options.consumerId();
        if (consumerId != null) {
            // 重置消费者状态
            snapshotReader.consumerManager().resetConsumer(consumerId, new Consumer(nextSnapshot));
        }
    }

    /**
     * 设置数据分片。
     *
     * @param indexOfThisSubtask 当前子任务的索引
     * @param numberOfParallelSubtasks 并行子任务数量
     * @return 当前扫描器实例
     */
    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        // 配置分片信息
        snapshotReader.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }
}