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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * UniversalCompaction 类实现了特定的压缩策略，用于在 LSM 树结构中处理数据文件的压缩。
 *
 * Universal Compaction Style 是一种压缩风格，适用于要求较低写放大率（write amplification）但可能增加读放大率和空间放大率的场景。
 * 参考 RocksDb 的 Universal Compaction 文档：
 * @see <url>https://github.com/facebook/rocksdb/wiki/Universal-Compaction</url>
 */
public class UniversalCompaction implements CompactStrategy {

    // 日志记录器，用于日志记录
    private static final Logger LOG = LoggerFactory.getLogger(UniversalCompaction.class);

    // 参数：最大空间放大比
    private final int maxSizeAmp;

    // 参数：文件大小比例参数，用于控制合并文件时的大小比例
    private final int sizeRatio;

    // 参数：触发压缩的运行数阈值，当运行数超过此阈值时，触发压缩
    private final int numRunCompactionTrigger;

    // 定期压缩的时间间隔，单位为毫秒
    @Nullable private final Long opCompactionInterval;

    // 上次定期压缩的时间
    @Nullable private Long lastOptimizedCompaction;

    /**
     * 构造函数，初始化 UniversalCompaction 对象。
     *
     * @param maxSizeAmp 最大空间放大比。
     * @param sizeRatio 文件大小比例参数。
     * @param numRunCompactionTrigger 触发压缩的运行数阈值。
     */
    public UniversalCompaction(int maxSizeAmp, int sizeRatio, int numRunCompactionTrigger) {
        this(maxSizeAmp, sizeRatio, numRunCompactionTrigger, null);
    }

    /**
     * 构造函数，初始化 UniversalCompaction 对象。
     *
     * @param maxSizeAmp 最大空间放大比。
     * @param sizeRatio 文件大小比例参数。
     * @param numRunCompactionTrigger 触发压缩的运行数阈值。
     * @param opCompactionInterval 定期压缩的时间间隔，单位为毫秒。
     */
    public UniversalCompaction(
            int maxSizeAmp,
            int sizeRatio,
            int numRunCompactionTrigger,
            @Nullable Duration opCompactionInterval) {
        this.maxSizeAmp = maxSizeAmp;
        this.sizeRatio = sizeRatio;
        this.numRunCompactionTrigger = numRunCompactionTrigger;
        this.opCompactionInterval =
                opCompactionInterval == null ? null : opCompactionInterval.toMillis();
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1; // 获取最大级别

        // 检查是否需要触发基于时间的定期压缩
        if (opCompactionInterval != null) {
            if (lastOptimizedCompaction == null
                    || currentTimeMillis() - lastOptimizedCompaction > opCompactionInterval) {
                LOG.debug("Universal compaction due to optimized compaction interval");
                updateLastOptimizedCompaction(); // 更新上次定期压缩时间
                return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs)); // 返回压缩单元
            }
        }

        // 检查是否需要基于空间放大比触发压缩
        CompactUnit unit = pickForSizeAmp(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size amplification");
            }
            return Optional.of(unit);
        }

        // 检查是否需要基于大小比例触发压缩
        unit = pickForSizeRatio(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size ratio");
            }
            return Optional.of(unit);
        }

        // 检查运行数是否超过阈值，触发基于文件数的压缩
        if (runs.size() > numRunCompactionTrigger) {
            int candidateCount = runs.size() - numRunCompactionTrigger + 1;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to file num");
            }
            return Optional.ofNullable(pickForSizeRatio(maxLevel, runs, candidateCount));
        }

        return Optional.empty(); // 无符合条件的压缩单元
    }

    /**
     * 根据空间放大比挑选压缩单元。
     *
     * <ul>
     *   <li>空间放大比是指除最新运行外的所有运行的总大小与最新运行大小的比例。
     *   <li>如果空间放大比超过 {@code maxSizeAmp}，则触发压缩。
     * </ul>
     *
     * @param maxLevel 最大级别
     * @param runs 运行列表
     * @return 返回压缩单元或 null
     */
    @VisibleForTesting
    CompactUnit pickForSizeAmp(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        // 计算除最新运行外的所有运行的总大小
        long candidateSize =
                runs.subList(0, runs.size() - 1).stream()
                        .map(LevelSortedRun::run)
                        .mapToLong(SortedRun::totalSize)
                        .sum();

        // 获取最新运行的总大小
        long earliestRunSize = runs.get(runs.size() - 1).run().totalSize();

        // 检查空间放大比是否超过阈值
        if (candidateSize * 100 > maxSizeAmp * earliestRunSize) {
            updateLastOptimizedCompaction(); // 更新上次定期压缩时间
            return CompactUnit.fromLevelRuns(maxLevel, runs); // 返回压缩单元
        }

        return null;
    }

    /**
     * 根据大小比例挑选压缩单元。
     *
     * <ul>
     *   <li>大小比例是指当前累积的运行大小与下一个运行大小的比率。
     *   <li>如果比率小于等于 {@code sizeRatio}，则继续累积运行。
     * </ul>
     *
     * @param maxLevel 最大级别
     * @param runs 运行列表
     * @return 返回压缩单元或 null
     */
    @VisibleForTesting
    CompactUnit pickForSizeRatio(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        return pickForSizeRatio(maxLevel, runs, 1); // 默认候选运行数为 1
    }

    /**
     * 根据大小比例挑选压缩单元。
     *
     * @param maxLevel 最大级别
     * @param runs 运行列表
     * @param candidateCount 候选运行数
     * @return 返回压缩单元或 null
     */
    private CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount) {
        return pickForSizeRatio(maxLevel, runs, candidateCount, false);
    }

    /**
     * 根据大小比例挑选压缩单元。
     *
     * <ul>
     *   <li>如果存在足够的候选运行且满足大小比例条件，则返回压缩单元。
     *   <li>否则，返回 null。
     * </ul>
     *
     * @param maxLevel 最大级别
     * @param runs 运行列表
     * @param candidateCount 候选运行数
     * @param forcePick 是否强制挑选，即使只能挑选少量运行
     * @return 返回压缩单元或 null
     */
    public CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount, boolean forcePick) {
        long candidateSize = candidateSize(runs, candidateCount); // 计算候选运行的总大小
        for (int i = candidateCount; i < runs.size(); i++) {
            LevelSortedRun next = runs.get(i); // 获取下一个运行
            // 检查候选运行的总大小与下一个运行大小是否满足比例条件
            if (candidateSize * (100.0 + sizeRatio) / 100.0 < next.run().totalSize()) {
                break;
            }
            candidateSize += next.run().totalSize(); // 继续累积运行
            candidateCount++;
        }

        // 如果强制挑选或候选运行数超过 1，则创建压缩单元
        if (forcePick || candidateCount > 1) {
            return createUnit(runs, maxLevel, candidateCount);
        }

        return null;
    }

    /**
     * 计算候选运行的总大小。
     *
     * @param runs 运行列表
     * @param candidateCount 候选运行数
     * @return 返回候选运行的总大小
     */
    private long candidateSize(List<LevelSortedRun> runs, int candidateCount) {
        long size = 0;
        for (int i = 0; i < candidateCount; i++) {
            size += runs.get(i).run().totalSize(); // 累加每个运行的总大小
        }
        return size;
    }

    /**
     * 创建压缩单元。
     *
     * <ul>
     *   <li>确定压缩单元的输出级别。
     *   <li>根据运行数和输出级别构造压缩单元。
     * </ul>
     *
     * @param runs 运行列表
     * @param maxLevel 最大级别
     * @param runCount 候选运行数
     * @return 返回压缩单元
     */
    @VisibleForTesting
    CompactUnit createUnit(List<LevelSortedRun> runs, int maxLevel, int runCount) {
        int outputLevel;
        if (runCount == runs.size()) {
            outputLevel = maxLevel; // 如果所有运行都被选中，输出到最大级别
        } else {
            // 否则，输出级别为下一个运行级别减 1
            outputLevel = Math.max(0, runs.get(runCount).level() - 1);
        }

        // 如果输出级别是 0，尝试寻找更高的级别
        if (outputLevel == 0) {
            for (int i = runCount; i < runs.size(); i++) {
                LevelSortedRun next = runs.get(i);
                runCount++;
                if (next.level() != 0) {
                    outputLevel = next.level(); // 找到第一个非 0 级别的运行
                    break;
                }
            }
        }

        if (runCount == runs.size()) {
            updateLastOptimizedCompaction(); // 更新上次定期压缩时间
            outputLevel = maxLevel; // 确保输出级别是最大级别
        }

        return CompactUnit.fromLevelRuns(outputLevel, runs.subList(0, runCount)); // 创建压缩单元
    }

    /**
     * 更新上次执行定期压缩的时间。
     */
    private void updateLastOptimizedCompaction() {
        lastOptimizedCompaction = currentTimeMillis(); // 设置为当前时间
    }

    /**
     * 获取当前时间，单位为毫秒。
     *
     * @return 返回当前时间（毫秒）
     */
    long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
