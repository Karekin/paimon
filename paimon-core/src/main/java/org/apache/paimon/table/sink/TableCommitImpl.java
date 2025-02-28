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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.paimon.CoreOptions.ExpireExecutionMode;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;
import static org.apache.paimon.utils.ManifestReadThreadPool.getExecutorService;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyExecute;

/**
 * 表提交器的实现类，基于 {@link FileStoreCommit} 提供快照提交和过期清理功能。
 */
public class TableCommitImpl implements InnerTableCommit {
    private static final Logger LOG = LoggerFactory.getLogger(TableCommitImpl.class);

    private final FileStoreCommit commit;
    @Nullable private final Runnable expireSnapshots;
    @Nullable private final PartitionExpire partitionExpire;
    @Nullable private final TagAutoManager tagAutoManager;
    private final Lock lock;

    @Nullable private final Duration consumerExpireTime;
    private final ConsumerManager consumerManager;

    private final ExecutorService expireMainExecutor;
    private final AtomicReference<Throwable> expireError;

    private final String tableName;

    @Nullable private Map<String, String> overwritePartition = null;
    private boolean batchCommitted = false;
    private final boolean forceCreatingSnapshot;

    public TableCommitImpl(
            FileStoreCommit commit,
            @Nullable Runnable expireSnapshots,
            @Nullable PartitionExpire partitionExpire,
            @Nullable TagAutoManager tagAutoManager,
            Lock lock,
            @Nullable Duration consumerExpireTime,
            ConsumerManager consumerManager,
            ExpireExecutionMode expireExecutionMode,
            String tableName,
            boolean forceCreatingSnapshot) {
        commit.withLock(lock); // 设置文件存储提交的锁

        if (partitionExpire != null) {
            partitionExpire.withLock(lock); // 设置分区过期的锁
            commit.withPartitionExpire(partitionExpire); // 将分区过期对象绑定到文件存储提交中
        }

        this.commit = commit;
        this.expireSnapshots = expireSnapshots;
        this.partitionExpire = partitionExpire;
        this.tagAutoManager = tagAutoManager;
        this.lock = lock;

        this.consumerExpireTime = consumerExpireTime;
        this.consumerManager = consumerManager;

        // 根据过期清理模式创建主线程的执行器
        this.expireMainExecutor =
                expireExecutionMode == ExpireExecutionMode.SYNC
                        ? MoreExecutors.newDirectExecutorService()
                        : Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                Thread.currentThread().getName() + "expire-main-thread"));
        this.expireError = new AtomicReference<>(null);

        this.tableName = tableName;
        this.forceCreatingSnapshot = forceCreatingSnapshot;
    }

    /**
     * 检查是否需要强制创建快照。
     *
     * @return 是否需要强制创建快照
     */
    public boolean forceCreatingSnapshot() {
        if (this.forceCreatingSnapshot) {
            return true; // 如果强制创建快照的标志为 true，则返回 true
        }
        if (overwritePartition != null) {
            return true; // 如果指定了覆盖分区，则返回 true
        }
        return tagAutoManager != null
                && tagAutoManager.getTagAutoCreation() != null
                && tagAutoManager.getTagAutoCreation().forceCreatingSnapshot(); // 如果标签自动管理器支持强制创建快照，则返回 true
    }

    @Override
    public TableCommitImpl withOverwrite(@Nullable Map<String, String> overwritePartitions) {
        this.overwritePartition = overwritePartitions; // 设置覆盖分区
        return this;
    }

    @Override
    public TableCommitImpl ignoreEmptyCommit(boolean ignoreEmptyCommit) {
        commit.ignoreEmptyCommit(ignoreEmptyCommit); // 设置文件存储提交是否忽略空提交
        return this;
    }

    @Override
    public InnerTableCommit withMetricRegistry(MetricRegistry registry) {
        commit.withMetrics(new CommitMetrics(registry, tableName)); // 设置文件存储提交的度量指标
        return this;
    }

    @Override
    public void commit(List<CommitMessage> commitMessages) {
        checkCommitted(); // 检查是否已经提交过
        commit(COMMIT_IDENTIFIER, commitMessages); // 提交提交消息
    }

    @Override
    public void truncateTable() {
        checkCommitted(); // 检查是否已经提交过
        commit.truncateTable(COMMIT_IDENTIFIER); // 清空表
    }

    private void checkCommitted() {
        checkState(!batchCommitted, "批处理表提交仅支持一次性提交"); // 确保批处理表提交没有被多次调用
        batchCommitted = true;
    }

    @Override
    public void commit(long identifier, List<CommitMessage> commitMessages) {
        commit(createManifestCommittable(identifier, commitMessages)); // 创建 ManifestCommittable 对象并提交
    }

    @Override
    public int filterAndCommit(Map<Long, List<CommitMessage>> commitIdentifiersAndMessages) {
        return filterAndCommitMultiple(
                commitIdentifiersAndMessages.entrySet().stream()
                        .map(e -> createManifestCommittable(e.getKey(), e.getValue()))
                        .collect(Collectors.toList())); // 根据 commitIdentifiersAndMessages 创建多个 ManifestCommittable 对象并提交
    }

    private ManifestCommittable createManifestCommittable(
            long identifier, List<CommitMessage> commitMessages) {
        ManifestCommittable committable = new ManifestCommittable(identifier); // 创建 ManifestCommittable 对象
        for (CommitMessage commitMessage : commitMessages) {
            committable.addFileCommittable(commitMessage); // 添加提交消息
        }
        return committable;
    }

    public void commit(ManifestCommittable committable) {
        commitMultiple(singletonList(committable), false); // 提交单个 ManifestCommittable 对象
    }

    public void commitMultiple(List<ManifestCommittable> committables, boolean checkAppendFiles) {
        if (overwritePartition == null) {
            for (ManifestCommittable committable : committables) {
                commit.commit(committable, new HashMap<>(), checkAppendFiles); // 提交每个 ManifestCommittable 对象
            }
            if (!committables.isEmpty()) {
                expire(committables.get(committables.size() - 1).identifier(), expireMainExecutor); // 过期清理
            }
        } else {
            ManifestCommittable committable;
            if (committables.size() > 1) {
                throw new RuntimeException(
                        "覆盖模式下出现多个 committables，这可能是错误，请报告： "
                                + committables); // 覆盖模式下不允许有多个 committables
            } else if (committables.size() == 1) {
                committable = committables.get(0);
            } else {
                committable = new ManifestCommittable(Long.MAX_VALUE); // 如果没有 committables，创建一个空的 ManifestCommittable 对象，标识符为 Long.MAX_VALUE
            }
            commit.overwrite(overwritePartition, committable, Collections.emptyMap()); // 覆盖提交
            expire(committable.identifier(), expireMainExecutor); // 过期清理
        }
    }

    public int filterAndCommitMultiple(List<ManifestCommittable> committables) {
        return filterAndCommitMultiple(committables, true); // 调用重载方法
    }

    public int filterAndCommitMultiple(
            List<ManifestCommittable> committables, boolean checkAppendFiles) {
        List<ManifestCommittable> sortedCommittables =
                committables.stream()
                        .sorted(Comparator.comparingLong(ManifestCommittable::identifier)) // 按标识符排序
                        .collect(Collectors.toList());

        List<ManifestCommittable> retryCommittables = commit.filterCommitted(sortedCommittables); // 过滤已提交的 ManifestCommittable 对象

        if (!retryCommittables.isEmpty()) {
            checkFilesExistence(retryCommittables); // 检查文件是否存在
            commitMultiple(retryCommittables, checkAppendFiles); // 提交过滤后的 ManifestCommittable 对象
        }
        return retryCommittables.size();
    }

    private void checkFilesExistence(List<ManifestCommittable> committables) {
        List<Path> files = new ArrayList<>();
        Map<Pair<BinaryRow, Integer>, DataFilePathFactory> factoryMap = new HashMap<>();
        PathFactory indexFileFactory = commit.pathFactory().indexFileFactory(); // 获取索引文件工厂

        for (ManifestCommittable committable : committables) {
            for (CommitMessage message : committable.fileCommittables()) {
                CommitMessageImpl msg = (CommitMessageImpl) message;
                DataFilePathFactory pathFactory =
                        factoryMap.computeIfAbsent(
                                Pair.of(message.partition(), message.bucket()),
                                k ->
                                        commit.pathFactory()
                                                .createDataFilePathFactory(
                                                        k.getKey(), k.getValue())); // 获取数据文件路径工厂

                Consumer<DataFileMeta> collector = f -> files.addAll(f.collectFiles(pathFactory)); // 收集文件路径

                msg.newFilesIncrement().newFiles().forEach(collector); // 收集新文件
                msg.newFilesIncrement().changelogFiles().forEach(collector); // 收集变更日志文件
                msg.compactIncrement().compactBefore().forEach(collector); // 收集压缩前的文件
                msg.compactIncrement().compactAfter().forEach(collector); // 收集压缩后的文件
                msg.indexIncrement().newIndexFiles().stream()
                        .map(IndexFileMeta::fileName)
                        .map(indexFileFactory::toPath)
                        .forEach(files::add); // 收集新的索引文件
                msg.indexIncrement().deletedIndexFiles().stream()
                        .map(IndexFileMeta::fileName)
                        .map(indexFileFactory::toPath)
                        .forEach(files::add); // 收集被删除的索引文件
            }
        }

        Predicate<Path> nonExists =
                p -> {
                    try {
                        return !commit.fileIO().exists(p); // 判断文件是否存在
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                };

        List<Path> nonExistFiles =
                Lists.newArrayList(
                        randomlyExecute(
                                getExecutorService(null),
                                f -> nonExists.test(f) ? singletonList(f) : emptyList(),
                                files)); // 检查不存在的文件

        if (nonExistFiles.size() > 0) {
            String message =
                    String.join(
                            "\n",
                            "无法从快照中恢复，因为需要重新提交的一些文件已经被删除：",
                            "    "
                                    + nonExistFiles.stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining(",")),
                            "    这很可能是由于您正在从一个包含未提交文件的老化保存点恢复造成的。"); // 生成错误信息
            throw new RuntimeException(message);
        }
    }

    private void expire(long partitionExpireIdentifier, ExecutorService executor) {
        if (expireError.get() != null) {
            throw new RuntimeException(expireError.get()); // 如果过期清理期间发生错误，抛出异常
        }

        executor.execute(
                () -> {
                    try {
                        expire(partitionExpireIdentifier); // 执行过期清理
                    } catch (Throwable t) {
                        LOG.error("执行过期清理时遇到错误。", t);
                        expireError.compareAndSet(null, t); // 记录错误
                    }
                });
    }

    private void expire(long partitionExpireIdentifier) {
        // 清理消费者，避免阻止快照过期
        if (consumerExpireTime != null) {
            consumerManager.expire(LocalDateTime.now().minus(consumerExpireTime)); // 清理老化的消费者
        }

        expireSnapshots(); // 调用过期清理回调

        if (partitionExpire != null) {
            partitionExpire.expire(partitionExpireIdentifier); // 过期清理分区
        }

        if (tagAutoManager != null) {
            tagAutoManager.run(); // 自动管理标签
        }
    }

    public void expireSnapshots() {
        if (expireSnapshots != null) {
            expireSnapshots.run(); // 执行快照过期清理
        }
    }

    @Override
    public void close() throws Exception {
        commit.close(); // 关闭文件存储提交
        IOUtils.closeQuietly(lock); // 关闭锁
        expireMainExecutor.shutdownNow(); // 关闭主线程执行器
    }

    @Override
    public void abort(List<CommitMessage> commitMessages) {
        commit.abort(commitMessages); // 回滚提交
    }

    @VisibleForTesting
    public ExecutorService getExpireMainExecutor() {
        return expireMainExecutor; // 返回主线程执行器，用于测试
    }
}
