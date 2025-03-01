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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.MetricUtils;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 为 {@link KeyValueFileStore} 提供的压缩管理器。
 */
public class MergeTreeCompactManager extends CompactFutureManager {

    private static final Logger LOG = LoggerFactory.getLogger(MergeTreeCompactManager.class); // 日志工具

    // 线程池，用于执行压缩任务
    private final ExecutorService executor;

    // 文件存储系统中的层级结构，管理不同级别的文件
    private final Levels levels;

    // 压缩策略，决定如何选择要压缩的文件
    private final CompactStrategy strategy;

    // 键值对的比较器，用于排序
    private final Comparator<InternalRow> keyComparator;

    // 压缩后文件的理想大小
    private final long compactionFileSize;

    // 触发压缩操作时，Level0 文件的数量阈值
    private final int numSortedRunStopTrigger;

    // 压缩重写器，用于重写压缩后的文件
    private final CompactRewriter rewriter;

    // 压缩指标的报告器，可选
    @Nullable private final CompactionMetrics.Reporter metricsReporter;

    // 删除向量维护器，可选
    @Nullable private final DeletionVectorsMaintainer dvMaintainer;

    // 是否延迟生成删除文件
    private final boolean lazyGenDeletionFile;

    /**
     * 构造函数，初始化 MergeTreeCompactManager。
     *
     * @param executor 线程池
     * @param levels 文件存储系统中的层级结构
     * @param strategy 压缩策略
     * @param keyComparator 键值对比较器
     * @param compactionFileSize 压缩后文件的理想大小
     * @param numSortedRunStopTrigger 触发压缩操作时，Level0 文件的数量阈值
     * @param rewriter 压缩重写器
     * @param metricsReporter 压缩指标的报告器
     * @param dvMaintainer 删除向量维护器
     * @param lazyGenDeletionFile 是否延迟生成删除文件
     */
    public MergeTreeCompactManager(
            ExecutorService executor,
            Levels levels,
            CompactStrategy strategy,
            Comparator<InternalRow> keyComparator,
            long compactionFileSize,
            int numSortedRunStopTrigger,
            CompactRewriter rewriter,
            @Nullable CompactionMetrics.Reporter metricsReporter,
            @Nullable DeletionVectorsMaintainer dvMaintainer,
            boolean lazyGenDeletionFile) {
        this.executor = executor;
        this.levels = levels;
        this.strategy = strategy;
        this.compactionFileSize = compactionFileSize;
        this.numSortedRunStopTrigger = numSortedRunStopTrigger;
        this.keyComparator = keyComparator;
        this.rewriter = rewriter;
        this.metricsReporter = metricsReporter;
        this.dvMaintainer = dvMaintainer;
        this.lazyGenDeletionFile = lazyGenDeletionFile;

        // 调用方法报告 Level0 文件数量，失败时忽略异常
        MetricUtils.safeCall(this::reportLevel0FileCount, LOG);
    }

    /**
     * 判断是否应该等待最新的压缩操作完成。
     *
     * @return 如果 Level0 文件数量超过阈值，返回 true
     */
    @Override
    public boolean shouldWaitForLatestCompaction() {
        return levels.numberOfSortedRuns() > numSortedRunStopTrigger;
    }

    /**
     * 判断是否应该等待准备检查点。
     *
     * @return 如果 Level0 文件数量超过阈值加一，返回 true
     */
    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        // 防止数值溢出，转换为 long 类型
        return levels.numberOfSortedRuns() > (long) numSortedRunStopTrigger + 1;
    }

    /**
     * 添加新文件到 Level0。
     *
     * @param file 新文件
     */
    @Override
    public void addNewFile(DataFileMeta file) {
        levels.addLevel0File(file);
        // 调用方法报告 Level0 文件数量
        MetricUtils.safeCall(this::reportLevel0FileCount, LOG);
    }

    /**
     * 获取所有文件。
     *
     * @return 所有文件的列表
     */
    @Override
    public List<DataFileMeta> allFiles() {
        return levels.allFiles();
    }

    /**
     * 触发压缩操作。
     *
     * @param fullCompaction 是否强制进行完全压缩
     */
    @Override
    public void triggerCompaction(boolean fullCompaction) {
        Optional<CompactUnit> optionalUnit; // 压缩单元
        List<LevelSortedRun> runs = levels.levelSortedRuns(); // 获取层级中的排序运行列表

        if (fullCompaction) {
            // 强制完全压缩
            Preconditions.checkState(
                    taskFuture == null, // 确保没有正在运行的压缩任务
                    "用户强制压缩时，发现已有压缩任务在运行");

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "触发强制完全压缩. 从以下运行中选择\n{}",
                        runs);
            }

            optionalUnit = CompactStrategy.pickFullCompaction(levels.numberOfLevels(), runs); // 选择完全压缩单元
        } else {
            if (taskFuture != null) {
                return; // 如果有正在运行的压缩任务，直接返回
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("触发正常压缩. 从以下运行中选择\n{}", runs);
            }

            optionalUnit =
                    strategy.pick(levels.numberOfLevels(), runs) // 根据策略选择压缩单元
                            .filter(unit -> unit.files().size() > 0) // 过滤空单元
                            .filter(
                                    unit ->
                                            unit.files().size() > 1
                                                    || unit.files().get(0).level()
                                                    != unit.outputLevel()); // 进一步过滤
        }

        optionalUnit.ifPresent(
                unit -> {
                    // 确定是否丢弃删除信息
                    boolean dropDelete =
                            unit.outputLevel() != 0
                                    && (unit.outputLevel() >= levels.nonEmptyHighestLevel()
                                    || dvMaintainer != null);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "提交压缩任务，包含文件 (名称, 层级, 大小): "
                                        + levels.levelSortedRuns().stream()
                                        .flatMap(lsr -> lsr.run().files().stream())
                                        .map(
                                                file ->
                                                        String.format(
                                                                "(%s, %d, %d)",
                                                                file.fileName(),
                                                                file.level(),
                                                                file.fileSize()))
                                        .collect(Collectors.joining(", ")));
                    }
                    submitCompaction(unit, dropDelete); // 提交压缩任务
                });
    }

    /**
     * 用于测试的层级访问方法。
     *
     * @return 当前的层级结构
     */
    @VisibleForTesting
    public Levels levels() {
        return levels;
    }

    /**
     * 提交压缩任务。
     *
     * @param unit 压缩单元
     * @param dropDelete 是否丢弃删除信息
     */
    private void submitCompaction(CompactUnit unit, boolean dropDelete) {
        // 压缩删除文件的生成器
        Supplier<CompactDeletionFile> compactDfSupplier = () -> null;

        if (dvMaintainer != null) {
            if (lazyGenDeletionFile) {
                // 延迟生成删除文件
                compactDfSupplier = () -> CompactDeletionFile.lazyGeneration(dvMaintainer);
            } else {
                // 立即生成删除文件
                compactDfSupplier = () -> CompactDeletionFile.generateFiles(dvMaintainer);
            }
        }

        MergeTreeCompactTask task = // 创建压缩任务
                new MergeTreeCompactTask(
                        keyComparator,
                        compactionFileSize,
                        rewriter,
                        unit,
                        dropDelete,
                        levels.maxLevel(),
                        metricsReporter,
                        compactDfSupplier);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "选择以下文件 (名称, 层级, 大小) 进行压缩: {}",
                    unit.files().stream()
                            .map(
                                    file ->
                                            String.format(
                                                    "(%s, %d, %d)",
                                                    file.fileName(), file.level(), file.fileSize()))
                            .collect(Collectors.joining(", ")));
        }

        taskFuture = executor.submit(task); // 提交任务到线程池
    }

    /**
     * 获取压缩结果并更新层级状态。
     *
     * @param blocking 是否阻塞等待结果
     * @return 压缩结果
     * @throws ExecutionException 执行异常
     * @throws InterruptedException 线程中断异常
     */
    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        Optional<CompactResult> result = innerGetCompactionResult(blocking); // 获取结果

        result.ifPresent(
                r -> { // 更新层级状态
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "使用以下更改更新层级状态:\nBefore:\n{}\nAfter:\n{}",
                                r.before(),
                                r.after());
                    }
                    levels.update(r.before(), r.after());
                    // 报告 Level0 文件数量
                    MetricUtils.safeCall(this::reportLevel0FileCount, LOG);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "层级状态更新完成. 当前运行中的任务:\n{}",
                                levels.levelSortedRuns());
                    }
                });
        return result;
    }

    /**
     * 报告 Level0 文件数量。
     */
    private void reportLevel0FileCount() {
        if (metricsReporter != null) {
            metricsReporter.reportLevel0FileCount(levels.level0().size()); // 报告文件数量
        }
    }

    /**
     * 关闭资源。
     *
     * @throws IOException 输入/输出异常
     */
    @Override
    public void close() throws IOException {
        rewriter.close(); // 关闭压缩重写器
        if (metricsReporter != null) {
            // 安全调用指标报告器的注销方法
            MetricUtils.safeCall(metricsReporter::unregister, LOG);
        }
    }
}