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

import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.operation.metrics.CompactionMetrics;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

/**
 * 基于合并树的压缩任务类。
 *
 * 该类实现了特定的压缩任务逻辑，用于对合并树中的数据文件进行压缩。
 */
public class MergeTreeCompactTask extends CompactTask {

    // 数据文件的最小文件大小，用于判断是否需要重写小文件
    private final long minFileSize;

    // 用于重写数据文件的压缩重写器
    private final CompactRewriter rewriter;

    // 压缩单元的输出级别
    private final int outputLevel;

    // 用于生成压缩删除文件的供应商
    private final Supplier<CompactDeletionFile> compactDfSupplier;

    // 分区后的运行列表
    private final List<List<SortedRun>> partitioned;

    // 是否删除中间文件
    private final boolean dropDelete;

    // 最大级别
    private final int maxLevel;

    // 指标：升级文件的数量
    private int upgradeFilesNum;

    /**
     * 构造函数，初始化 MergeTreeCompactTask 对象。
     *
     * @param keyComparator 数据键的比较器
     * @param minFileSize 数据文件的最小文件大小
     * @param rewriter 压缩重写器
     * @param unit 压缩单元
     * @param dropDelete 是否删除中间文件
     * @param maxLevel 最大级别
     * @param metricsReporter 指标报告器
     * @param compactDfSupplier 压缩删除文件供应商
     */
    public MergeTreeCompactTask(
            Comparator<InternalRow> keyComparator,
            long minFileSize,
            CompactRewriter rewriter,
            CompactUnit unit,
            boolean dropDelete,
            int maxLevel,
            @Nullable CompactionMetrics.Reporter metricsReporter,
            Supplier<CompactDeletionFile> compactDfSupplier) {
        super(metricsReporter); // 初始化父类
        this.minFileSize = minFileSize; // 设置最小文件大小
        this.rewriter = rewriter; // 设置重写器
        this.outputLevel = unit.outputLevel(); // 设置输出级别
        this.compactDfSupplier = compactDfSupplier; // 设置删除文件供应商
        this.partitioned = new IntervalPartition(unit.files(), keyComparator).partition(); // 分区
        this.dropDelete = dropDelete; // 设置是否删除中间文件
        this.maxLevel = maxLevel; // 设置最大级别

        this.upgradeFilesNum = 0; // 初始化升级文件数量
    }

    @Override
    protected CompactResult doCompact() throws Exception {
        List<List<SortedRun>> candidate = new ArrayList<>(); // 收集待压缩的运行
        CompactResult result = new CompactResult(); // 压缩结果

        // 检查顺序并压缩相邻且连续的文件
        for (List<SortedRun> section : partitioned) {
            if (section.size() > 1) {
                candidate.add(section); // 累积多个运行
            } else {
                SortedRun run = section.get(0); // 获取单个运行
                // 没有重叠的文件，尝试升级或重写
                for (DataFileMeta file : run.files()) {
                    if (file.fileSize() < minFileSize) {
                        // 小文件需要重写
                        candidate.add(singletonList(SortedRun.fromSingle(file))); // 添加到候选列表
                    } else {
                        // 大文件可以直接升级
                        rewrite(candidate, result); // 重写累积的文件
                        upgrade(file, result); // 升级大文件
                    }
                }
            }
        }
        rewrite(candidate, result); // 处理剩余的候选文件
        result.setDeletionFile(compactDfSupplier.get()); // 设置删除文件
        return result; // 返回压缩结果
    }

    @Override
    protected String logMetric(
            long startMillis, List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        // 记录压缩任务的指标
        return String.format(
                "%s, upgrade file num = %d",
                super.logMetric(startMillis, compactBefore, compactAfter), // 调用父类的指标记录方法
                upgradeFilesNum); // 记录升级文件的数量
    }

    /**
     * 升级数据文件到指定级别。
     *
     * <ul>
     *   <li>如果文件的级别已经是目标级别，则无需升级。
     *   <li>否则，根据条件升级或重写文件。
     * </ul>
     *
     * @param file 数据文件元数据
     * @param toUpdate 更新的压缩结果
     * @throws Exception 升级过程中可能抛出的异常
     */
    private void upgrade(DataFileMeta file, CompactResult toUpdate) throws Exception {
        if (file.level() == outputLevel) {
            return; // 文件级别已经是目标级别
        }

        if (outputLevel != maxLevel || file.deleteRowCount().map(d -> d == 0).orElse(false)) {
            // 升级文件到指定级别
            CompactResult upgradeResult = rewriter.upgrade(outputLevel, file);
            toUpdate.merge(upgradeResult); // 合并升级结果
            upgradeFilesNum++; // 增加升级文件数量计数
        } else {
            // 含有删除记录的文件不能直接升级到最大级别，需要重写
            List<List<SortedRun>> candidate = new ArrayList<>();
            candidate.add(new ArrayList<>());
            candidate.get(0).add(SortedRun.fromSingle(file)); // 添加到候选列表
            rewriteImpl(candidate, toUpdate); // 执行重写
        }
    }

    /**
     * 重写候选运行列表中的文件。
     *
     * <ul>
     *   <li>根据候选运行列表的内容决定重写策略。
     *   <li>例如，如果候选运行列表中只有一个文件，则尝试升级该文件。
     * </ul>
     *
     * @param candidate 候选运行列表
     * @param toUpdate 更新的压缩结果
     * @throws Exception 重写过程中可能抛出的异常
     */
    private void rewrite(List<List<SortedRun>> candidate, CompactResult toUpdate) throws Exception {
        if (candidate.isEmpty()) {
            return; // 无候选文件
        }
        if (candidate.size() == 1) {
            List<SortedRun> section = candidate.get(0);
            if (section.size() == 0) {
                return; // 空运行
            } else if (section.size() == 1) {
                // 仅一个运行，处理其中的文件
                for (DataFileMeta file : section.get(0).files()) {
                    upgrade(file, toUpdate); // 单独升级每个文件
                }
                candidate.clear(); // 清空候选列表
                return;
            }
        }
        rewriteImpl(candidate, toUpdate); // 调用重写实现方法
    }

    /**
     * 重写候选运行列表中的文件。
     *
     * <ul>
     *   <li>重写文件到指定级别。
     *   <li>生成新的数据文件，并更新压缩结果。
     * </ul>
     *
     * @param candidate 候选运行列表
     * @param toUpdate 更新的压缩结果
     * @throws Exception 重写过程中可能抛出的异常
     */
    private void rewriteImpl(List<List<SortedRun>> candidate, CompactResult toUpdate)
            throws Exception {
        // 重写文件
        CompactResult rewriteResult = rewriter.rewrite(outputLevel, dropDelete, candidate);
        toUpdate.merge(rewriteResult); // 合并重写结果
        candidate.clear(); // 清空候选列表
    }
}
