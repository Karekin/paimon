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

package org.apache.paimon.append;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.MetricUtils;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static java.util.Collections.emptyList;


/**
 * 该类用于管理 {@link AppendOnlyFileStore} 的分桶追加压缩操作。
 * 它继承自 {@link CompactFutureManager}，负责处理数据文件的压缩逻辑， 包括全量压缩和增量压缩。
 * */
public class BucketedAppendCompactManager extends CompactFutureManager {

    private static final Logger LOG = LoggerFactory.getLogger(BucketedAppendCompactManager.class);

    /**
     * 全量压缩的最小文件数量阈值。如果文件数量小于该值，将不会触发全量压缩。
     * */
    private static final int FULL_COMPACT_MIN_FILE = 3;

    /**
     * 执行压缩任务的线程池，用于异步执行压缩任务。
     * */
    private final ExecutorService executor;

    /**
     * 删除向量维护者，用于管理删除向量文件。
     * */
    private final DeletionVectorsMaintainer dvMaintainer;

    /**
     * 待压缩数据文件的集合，按文件的最小序列号排序。
     * */
    private final TreeSet<DataFileMeta> toCompact;

    /**
     * 触发压缩的最小文件数量。当待压缩文件数量超过该值时，才会触发增量压缩。
     * */
    private final int minFileNum;

    /**
     * 触发全量压缩的最大文件数量。当待压缩文件数量超过该值时，会触发全量压缩。
     * */
    private final int maxFileNum;

    /**
     * 目标文件大小。当文件大小超过该值时，会被认为是大文件，可能不会被压缩。
     * */
    private final long targetFileSize;

    /**
     * 压缩重写器，用于处理文件的重写逻辑。
     * */
    private final CompactRewriter rewriter;

    /**
     * 当前正在压缩的文件列表。
     * */
    private List<DataFileMeta> compacting;

    /**
     * 压缩指标报告器，用于报告压缩过程中的指标信息。
     * */
    @Nullable private final CompactionMetrics.Reporter metricsReporter;

    /**
     * 构造函数，用于初始化压缩管理器。
     * @param executor 执行压缩任务的线程池。
     * @param restored 已恢复的数据文件列表。
     * @param dvMaintainer 删除向量维护者，可选。
     * @param minFileNum 触发压缩的最小文件数量。
     * @param maxFileNum 触发全量压缩的最大文件数量。
     * @param targetFileSize 目标文件大小。
     * @param rewriter 压缩重写器。
     * @param metricsReporter 压缩指标报告器，可选。
     * */
    public BucketedAppendCompactManager(
            ExecutorService executor,
            List<DataFileMeta> restored,
            @Nullable DeletionVectorsMaintainer dvMaintainer,
            int minFileNum,
            int maxFileNum,
            long targetFileSize,
            CompactRewriter rewriter,
            @Nullable CompactionMetrics.Reporter metricsReporter) {
        this.executor = executor;
        this.dvMaintainer = dvMaintainer;
        this.toCompact = new TreeSet<>(fileComparator(false)); // 初始化待压缩文件集合，使用文件比较器
        this.toCompact.addAll(restored); // 将已恢复的文件添加到待压缩集合
        this.minFileNum = minFileNum;
        this.maxFileNum = maxFileNum;
        this.targetFileSize = targetFileSize;
        this.rewriter = rewriter;
        this.metricsReporter = metricsReporter;
    }

    /**
     * 触发压缩操作。
     * 如果是全量压缩，则调用 `triggerFullCompaction` 方法触发全量压缩。
     * 否则，调用 `triggerCompactionWithBestEffort` 方法触发增量压缩。
     * @param fullCompaction 是否触发全量压缩。
     * */
    @Override
    public void triggerCompaction(boolean fullCompaction) {
        if (fullCompaction) {
            triggerFullCompaction(); // 触发全量压缩
        } else {
            triggerCompactionWithBestEffort(); // 触发增量压缩
        }
    }

    /**
     * 触发全量压缩操作。
     * */
    private void triggerFullCompaction() {
        Preconditions.checkState(
                taskFuture == null,
                "A compaction task is still running while the user " +
                        "forces a new compaction. This is unexpected."); // 确保没有正在运行的压缩任务

        // 如果删除向量未启用，且文件数量小于 FULL_COMPACT_MIN_FILE，则不执行全量压缩
        if (toCompact.isEmpty() || (dvMaintainer == null && toCompact.size() < FULL_COMPACT_MIN_FILE)) {
            return;
        }

        taskFuture = executor.submit(
                new FullCompactTask(dvMaintainer, toCompact, targetFileSize, rewriter, metricsReporter)); // 提交全量压缩任务
        compacting = new ArrayList<>(toCompact); // 记录当前正在压缩的文件
        toCompact.clear(); // 清空待压缩文件集合
    }

    /**
     * 尽量触发增量压缩操作。
     * */
    private void triggerCompactionWithBestEffort() {
        if (taskFuture != null) {
            return; // 如果已经有压缩任务在运行，则不执行
        }

        Optional<List<DataFileMeta>> picked = pickCompactBefore(); // 从待压缩文件中挑选合适的文件
        if (picked.isPresent()) {
            compacting = picked.get(); // 记录当前正在压缩的文件
            taskFuture = executor.submit(
                    new AutoCompactTask(dvMaintainer, compacting, rewriter, metricsReporter)); // 提交增量压缩任务
        }
    }

    /**
     * 是否需要等待最新的压缩操作完成。始终返回 false。
     * @return 是否需要等待，总是返回 false。
     * */
    @Override
    public boolean shouldWaitForLatestCompaction() {
        return false;
    }

    /**
     * 是否需要等待准备 Checkpoint 操作。始终返回 false。
     * @return 是否需要等待，总是返回 false。
     * */
    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        return false;
    }

    /**
     * 添加新的数据文件到待压缩集合。
     * @param file 需要添加的新数据文件。
     * */
    @Override
    public void addNewFile(DataFileMeta file) {
        toCompact.add(file); // 添加数据文件到待压缩集合
    }

    /**
     * 获取所有数据文件，包括正在压缩的文件和待压缩文件。
     * @return 所有数据文件的列表。
     * */
    @Override
    public List<DataFileMeta> allFiles() {
        List<DataFileMeta> allFiles = new ArrayList<>();
        if (compacting != null) {
            allFiles.addAll(compacting); // 添加正在压缩的文件
        }
        allFiles.addAll(toCompact); // 添加待压缩文件
        return allFiles;
    }

    /**
     * 获取压缩结果，并更新待压缩文件集合。
     * 如果压缩后的最后一个文件小于目标文件大小，将其添加回待压缩集合。
     * @param blocking 是否阻塞等待结果。
     * @return 压缩结果，如果存在。
     * @throws ExecutionException 如果任务执行过程中发生异常。
     * @throws InterruptedException 如果线程中断。
     * */
    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        Optional<CompactResult> result = innerGetCompactionResult(blocking); // 获取压缩结果
        if (result.isPresent()) {
            CompactResult compactResult = result.get();
            if (!compactResult.after().isEmpty()) {
                DataFileMeta lastFile = compactResult.after().get(compactResult.after().size() - 1); // 获取最后一个文件
                if (lastFile.fileSize() < targetFileSize) { // 如果文件大小小于目标大小
                    toCompact.add(lastFile); // 添加回待压缩集合
                }
            }
            compacting = null; // 重置正在压缩的文件列表
        }
        return result;
    }

    /**
     * 从待压缩集合中挑选合适的文件用于增压缩。
     * 如果文件数量为空，返回空。
     * 否则，按照文件大小和数量挑选合适的文件。
     * @return 挑选的文件列表，如果存在。
     * */
    @VisibleForTesting
    Optional<List<DataFileMeta>> pickCompactBefore() {
        if (toCompact.isEmpty()) {
            return Optional.empty(); // 如果待压缩集合为空，返回空
        }

        long totalFileSize = 0L; // 总文件大小
        int fileNum = 0; // 文件数量
        LinkedList<DataFileMeta> candidates = new LinkedList<>(); // 候选文件列表

        // 遍历待压缩集合，挑选合适的文件
        while (!toCompact.isEmpty()) {
            DataFileMeta file = toCompact.pollFirst(); // 获取第一个文件
            candidates.add(file);
            totalFileSize += file.fileSize();
            fileNum++;

            // 如果满足条件，返回挑选的文件
            if ((totalFileSize >= targetFileSize && fileNum >= minFileNum) || fileNum >= maxFileNum) {
                return Optional.of(candidates);
            } else if (totalFileSize >= targetFileSize) {
                // 如果总大小超过目标大小但未达到文件数量条件，移除第一个文件
                DataFileMeta removed = candidates.pollFirst();
                assert removed != null;
                totalFileSize -= removed.fileSize();
                fileNum--;
            }
        }

        toCompact.addAll(candidates); // 如果不满足条件，将候选文件重新添加回待压缩集合
        return Optional.empty();
    }

    /**
     * 获取待压缩集合。
     * @return 待压缩集合。
     * */
    @VisibleForTesting
    TreeSet<DataFileMeta> getToCompact() {
        return toCompact;
    }

    /**
     * 关闭资源时注销指标报告器。
     * */
    @Override
    public void close() throws IOException {
        if (metricsReporter != null) {
            MetricUtils.safeCall(metricsReporter::unregister, LOG); // 注销指标报告器
        }
    }

    /**
     * 全量压缩任务的实现。
     * 该类用于处理全量压缩逻辑。
     * */
    public static class FullCompactTask extends CompactTask {

        private final DeletionVectorsMaintainer dvMaintainer; // 删除向量维护者
        private final LinkedList<DataFileMeta> toCompact; // 待压缩文件
        private final long targetFileSize; // 目标文件大小
        private final CompactRewriter rewriter; // 压缩重写器

        /**
         * 构造函数，用于初始化全量压缩任务。
         * @param dvMaintainer 删除向量维护者。
         * @param inputs 输入数据文件。
         * @param targetFileSize 目标文件大小。
         * @param rewriter 压缩重写器。
         * @param metricsReporter 压缩指标报告器。
         * */
        public FullCompactTask(
                DeletionVectorsMaintainer dvMaintainer,
                Collection<DataFileMeta> inputs,
                long targetFileSize,
                CompactRewriter rewriter,
                @Nullable CompactionMetrics.Reporter metricsReporter) {
            super(metricsReporter);
            this.dvMaintainer = dvMaintainer;
            this.toCompact = new LinkedList<>(inputs); // 初始化待压缩文件
            this.targetFileSize = targetFileSize;
            this.rewriter = rewriter;
        }

        /**
         * 执行全量压缩逻辑。
         * @return 压缩结果。
         * @throws Exception 如果发生异常。
         * */
        @Override
        protected CompactResult doCompact() throws Exception {
            // 移除较大的文件
            while (!toCompact.isEmpty()) {
                DataFileMeta file = toCompact.peekFirst();
                if (file.fileSize() >= targetFileSize && !hasDeletionFile(file)) { // 如果文件大小超过目标大小且没有删除向量
                    toCompact.poll(); // 移除文件
                    continue;
                }
                break;
            }

            // 执行压缩
            if (dvMaintainer != null) {
                // 如果删除向量启用，始终触发压缩
                return compact(dvMaintainer, toCompact, rewriter);
            } else {
                // 计算小文件数量
                int big = 0;
                int small = 0;
                for (DataFileMeta file : toCompact) {
                    if (file.fileSize() >= targetFileSize) {
                        big++;
                    } else {
                        small++;
                    }
                }
                if (small > big && toCompact.size() >= FULL_COMPACT_MIN_FILE) { // 如果小文件较多且文件数量超过阈值
                    return compact(null, toCompact, rewriter);
                } else {
                    return result(emptyList(), emptyList()); // 无文件需要压缩
                }
            }
        }

        /**
         * 判断数据文件是否有删除向量。
         * @param file 数据文件。
         * @return 是否有删除向量。
         * */
        private boolean hasDeletionFile(DataFileMeta file) {
            return dvMaintainer != null
                    && dvMaintainer.deletionVectorOf(file.fileName()).isPresent();
        }
    }

    /**
     * 增量压缩任务的实现。
     * */
    public static class AutoCompactTask extends CompactTask {

        private final DeletionVectorsMaintainer dvMaintainer; // 删除向量维护者
        private final List<DataFileMeta> toCompact; // 待压缩文件
        private final CompactRewriter rewriter; // 压缩重写器

        /**
         * 构造函数，用于初始化增量压缩任务。
         * @param dvMaintainer 删除向量维护者。
         * @param toCompact 待压缩文件。
         * @param rewriter 压缩重写器。
         * @param metricsReporter 压缩指标报告器。
         * */
        public AutoCompactTask(
                DeletionVectorsMaintainer dvMaintainer,
                List<DataFileMeta> toCompact,
                CompactRewriter rewriter,
                @Nullable CompactionMetrics.Reporter metricsReporter) {
            super(metricsReporter);
            this.dvMaintainer = dvMaintainer;
            this.toCompact = toCompact;
            this.rewriter = rewriter;
        }

        /**
         * 执行增量压缩逻辑。
         * @return 压缩结果。
         * @throws Exception 如果发生异常。
         * */
        @Override
        protected CompactResult doCompact() throws Exception {
            return compact(dvMaintainer, toCompact, rewriter); // 调用通用压缩方法
        }
    }

    /**
     * 通用压缩方法，用于执行具体的数据文件压缩逻辑。
     * 如果删除向量维护者不为空，会移除相关删除向量并更新压缩结果。
     * @param dvMaintainer 删除向量维护者。
     * @param toCompact 待压缩文件。
     * @param rewriter 压缩重写器。
     * @return 压缩结果。
     * @throws Exception 如果发生异常。
     * */
    private static CompactResult compact(
            @Nullable DeletionVectorsMaintainer dvMaintainer,
            List<DataFileMeta> toCompact,
            CompactRewriter rewriter)
            throws Exception {
        List<DataFileMeta> rewrite = rewriter.rewrite(toCompact); // 重写文件
        CompactResult result = result(toCompact, rewrite); // 创建压缩结果

        if (dvMaintainer != null) {
            toCompact.forEach(f -> dvMaintainer.removeDeletionVectorOf(f.fileName())); // 移除删除向量
            result.setDeletionFile(CompactDeletionFile.generateFiles(dvMaintainer)); // 更新删除文件
        }

        return result;
    }

    /**
     * 创建压缩结果对象。
     * @param before 压缩前的文件列表。
     * @param after 压缩后的文件列表。
     * @return 压缩结果对象。
     * */
    private static CompactResult result(List<DataFileMeta> before, List<DataFileMeta> after) {
        return new CompactResult(before, after);
    }

    /**
     * 数据文件重写器接口，用于定义文件重写逻辑。
     * */
    public interface CompactRewriter {
        List<DataFileMeta> rewrite(List<DataFileMeta> compactBefore) throws Exception;
    }

    /**
     * 数据文件比较器，用于按文件的最小序列号排序。如果文件之间有重叠，会发出警告。
     * @param ignoreOverlap 是否忽略重叠检查。
     * @return 数据文件比较器。
     * */
    public static Comparator<DataFileMeta> fileComparator(boolean ignoreOverlap) {
        return (o1, o2) -> {
            if (o1 == o2) {
                return 0;
            }

            // 检查文件是否有重叠
            if (!ignoreOverlap && isOverlap(o1, o2)) {
                LOG.warn(
                        String.format(
                                "There should no overlap in append files, but Range1(%s, %s), Range2(%s, %s),"
                                        + " check if you have multiple write jobs.",
                                o1.minSequenceNumber(),
                                o1.maxSequenceNumber(),
                                o2.minSequenceNumber(),
                                o2.maxSequenceNumber()));
            }

            // 按最小序列号排序
            return Long.compare(o1.minSequenceNumber(), o2.minSequenceNumber());
        };
    }

    /**
     * 检查两个数据文件是否有重叠。
     * @param o1 第一个数据文件。
     * @param o2 第二个数据文件。
     * @return 是否有重叠。
     * */
    private static boolean isOverlap(DataFileMeta o1, DataFileMeta o2) {
        return o2.minSequenceNumber() <= o1.maxSequenceNumber()
                && o2.maxSequenceNumber() >= o1.minSequenceNumber();
    }
}
