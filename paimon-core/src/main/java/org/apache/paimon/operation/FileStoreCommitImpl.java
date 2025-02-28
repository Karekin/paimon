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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.operation.metrics.CommitStats;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.manifest.ManifestEntry.recordCount;
import static org.apache.paimon.manifest.ManifestEntry.recordCountAdd;
import static org.apache.paimon.manifest.ManifestEntry.recordCountDelete;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.InternalRowPartitionComputer.partToSimpleString;

/**
 * 文件存储提交的默认实现。
 *
 * <p>该类为用户提供了一个原子性提交方法。
 *
 * <ol>
 *   <li>在调用 {@link FileStoreCommitImpl#commit} 之前，如果用户不确定本次提交是否已完成，应先使用 {@link FileStoreCommitImpl#filterCommitted} 进行过滤。
 *   <li>提交之前，会先检查是否存在冲突，具体包括检查所有待删除文件是否仍然存在，以及修改文件与现有文件的键值范围是否有重叠。
 *   <li>之后使用外部提供的锁（如果存在）或文件系统的原子重命名功能来确保提交的原子性。
 *   <li>如果提交因冲突或异常而失败，它会尽力清理并中止提交。
 *   <li>如果原子重命名失败，它会再次尝试，但需要先从步骤2中重新读取最新的快照。
 * </ol>
 *
 * <p>注意：如果您需要修改此类，任何在提交过程中发生的异常都必须抛出，不能被忽略。建议数千次运行 {@link FileStoreCommitTest}
 * 来确保您的修改是正确的。
 */
public class FileStoreCommitImpl implements FileStoreCommit {

    // 日志记录器，用于记录日志信息
    private static final Logger LOG = LoggerFactory.getLogger(FileStoreCommitImpl.class);

    // 文件输入/输出接口，用于文件的读写操作
    private final FileIO fileIO;

    // 模式管理器，用于管理表的模式
    private final SchemaManager schemaManager;

    // 提交用户，标识提交操作的用户
    private final String commitUser;

    // 分区字段的类型
    private final RowType partitionType;

    // 分区默认名称
    private final String partitionDefaultName;

    // 文件存储路径工厂，用于生成文件路径
    private final FileStorePathFactory pathFactory;

    // 快照管理器，用于管理快照
    private final SnapshotManager snapshotManager;

    // 清单文件工厂，用于生成清单文件
    private final ManifestFile manifestFile;

    // 清单列表工厂，用于生成清单列表
    private final ManifestList manifestList;

    // 索引清单文件工厂，用于生成索引清单文件
    private final IndexManifestFile indexManifestFile;

    // 文件存储扫描器，用于扫描文件存储中的内容
    private final FileStoreScan scan;

    // 存储桶的数量
    private final int numBucket;

    // 清单文件的目标大小
    private final MemorySize manifestTargetSize;

    // 清单文件的全量压缩大小
    private final MemorySize manifestFullCompactionSize;

    // 清单文件合并的最小数量
    private final int manifestMergeMinCount;

    // 是否允许动态分区覆盖
    private final boolean dynamicPartitionOverwrite;

    // 键值比较器，用于比较键值的顺序（可能为 null）
    @Nullable private final Comparator<InternalRow> keyComparator;

    // 分支名称
    private final String branchName;

    // 清单文件读取的并行度（可能为 null）
    @Nullable private final Integer manifestReadParallelism;

    // 提交回调列表，在提交完成后调用
    private final List<CommitCallback> commitCallbacks;

    // 统计文件处理器
    private final StatsFileHandler statsFileHandler;

    // 存储桶模式
    private final BucketMode bucketMode;

    // 锁对象，用于原子性提交（可能为 null）
    @Nullable private Lock lock;

    // 是否忽略空提交
    private boolean ignoreEmptyCommit;

    // 提交指标
    private CommitMetrics commitMetrics;

    // 分区过期策略（可能为 null）
    @Nullable private PartitionExpire partitionExpire;

    // 构造函数，初始化文件存储的相关参数和组件
    public FileStoreCommitImpl(
            FileIO fileIO,
            SchemaManager schemaManager,
            String commitUser,
            RowType partitionType,
            String partitionDefaultName,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            IndexManifestFile.Factory indexManifestFileFactory,
            FileStoreScan scan,
            int numBucket,
            MemorySize manifestTargetSize,
            MemorySize manifestFullCompactionSize,
            int manifestMergeMinCount,
            boolean dynamicPartitionOverwrite,
            @Nullable Comparator<InternalRow> keyComparator,
            String branchName,
            StatsFileHandler statsFileHandler,
            BucketMode bucketMode,
            @Nullable Integer manifestReadParallelism,
            List<CommitCallback> commitCallbacks) {
        // 初始化文件输入/输出接口
        this.fileIO = fileIO;
        // 初始化模式管理器
        this.schemaManager = schemaManager;
        // 初始化提交用户
        this.commitUser = commitUser;
        // 初始化分区字段的类型
        this.partitionType = partitionType;
        // 初始化分区默认名称
        this.partitionDefaultName = partitionDefaultName;
        // 初始化文件存储路径工厂
        this.pathFactory = pathFactory;
        // 初始化快照管理器
        this.snapshotManager = snapshotManager;
        // 初始化清单文件工厂
        this.manifestFile = manifestFileFactory.create();
        // 初始化清单列表工厂
        this.manifestList = manifestListFactory.create();
        // 初始化索引清单文件工厂
        this.indexManifestFile = indexManifestFileFactory.create();
        // 初始化文件存储扫描器
        this.scan = scan;
        // 初始化存储桶的数量
        this.numBucket = numBucket;
        // 初始化清单文件的目标大小
        this.manifestTargetSize = manifestTargetSize;
        // 初始化清单文件的全量压缩大小
        this.manifestFullCompactionSize = manifestFullCompactionSize;
        // 初始化清单文件合并的最小数量
        this.manifestMergeMinCount = manifestMergeMinCount;
        // 是否允许动态分区覆盖
        this.dynamicPartitionOverwrite = dynamicPartitionOverwrite;
        // 键值比较器
        this.keyComparator = keyComparator;
        // 初始化分支名称
        this.branchName = branchName;
        // 清单文件读取的并行度
        this.manifestReadParallelism = manifestReadParallelism;
        // 初始化提交回调列表
        this.commitCallbacks = commitCallbacks;

        // 初始化锁对象为 null
        this.lock = null;
        // 默认忽略空提交
        this.ignoreEmptyCommit = true;
        // 初始化提交指标为 null
        this.commitMetrics = null;
        // 初始化统计文件处理器
        this.statsFileHandler = statsFileHandler;
        // 初始化存储桶模式
        this.bucketMode = bucketMode;
    }

    // 设置外部锁，返回自身对象以支持链式调用
    @Override
    public FileStoreCommit withLock(Lock lock) {
        this.lock = lock;
        return this;
    }

    // 设置是否忽略空提交，返回自身对象以支持链式调用
    @Override
    public FileStoreCommit ignoreEmptyCommit(boolean ignoreEmptyCommit) {
        this.ignoreEmptyCommit = ignoreEmptyCommit;
        return this;
    }

    // 设置分区过期策略，返回自身对象以支持链式调用
    @Override
    public FileStoreCommit withPartitionExpire(PartitionExpire partitionExpire) {
        this.partitionExpire = partitionExpire;
        return this;
    }

    // 过滤已被提交的变更内容
    @Override
    public List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committables) {
        // 如果变更内容空，直接返回
        if (committables.isEmpty()) {
            return committables;
        }

        // 确保变更内容按标识符排序
        for (int i = 1; i < committables.size(); i++) {
            Preconditions.checkArgument(
                    committables.get(i).identifier() > committables.get(i - 1).identifier(),
                    "提交内容必须按标识符排序。这是意料之外的情况。");
        }

        // 获取最新快照
        Optional<Snapshot> latestSnapshot = snapshotManager.latestSnapshotOfUser(commitUser);
        if (latestSnapshot.isPresent()) {
            // 过滤出比最新快照的标识符新的提交内容
            List<ManifestCommittable> result = new ArrayList<>();
            for (ManifestCommittable committable : committables) {
                if (committable.identifier() > latestSnapshot.get().commitIdentifier()) {
                    result.add(committable);
                } else {
                    // 对需要重试的提交内容调用回调
                    commitCallbacks.forEach(callback -> callback.retry(committable));
                }
            }
            return result;
        } else {
            // 如果没有历史快照，则所有提交内容都是新的
            return committables;
        }
    }

    // 提交变更内容（调用标准的提交方法）
    @Override
    public void commit(ManifestCommittable committable, Map<String, String> properties) {
        commit(committable, properties, false);
    }

    // 提交变更内容，可以选择是否检测追加文件冲突
    @Override
    public void commit(
            ManifestCommittable committable,
            Map<String, String> properties,
            boolean checkAppendFiles) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("准备提交\n" + committable.toString());
        }

        long started = System.nanoTime(); // 获取当前时间（纳秒级）
        int generatedSnapshot = 0; // 生成的快照数量
        int attempts = 0; // 提交尝试次数

        // 初始化各种文件清单
        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelog = new ArrayList<>();
        List<IndexManifestEntry> appendHashIndexFiles = new ArrayList<>();
        List<IndexManifestEntry> compactDvIndexFiles = new ArrayList<>();

        // 收集变更内容
        collectChanges(
                committable.fileCommittables(),
                appendTableFiles,
                appendChangelog,
                compactTableFiles,
                compactChangelog,
                appendHashIndexFiles,
                compactDvIndexFiles);

        try {
            // 追加文件清单
            List<SimpleFileEntry> appendSimpleEntries = SimpleFileEntry.from(appendTableFiles);

            // 如果未忽略空提交，或者存在新增文件或索引文件
            if (!ignoreEmptyCommit
                    || !appendTableFiles.isEmpty()
                    || !appendChangelog.isEmpty()
                    || !appendHashIndexFiles.isEmpty()) {
                Snapshot latestSnapshot = snapshotManager.latestSnapshot();
                // 读取最新快照
                if (latestSnapshot != null && checkAppendFiles) {
                    // 读取变更分区的所有文件条目
                    List<SimpleFileEntry> baseEntries =
                            readAllEntriesFromChangedPartitions(
                                    latestSnapshot, appendTableFiles, compactTableFiles);

                    // 检查冲突
                    noConflictsOrFail(
                            latestSnapshot.commitUser(), baseEntries, appendSimpleEntries);
                }

                // 尝试提交
                attempts +=
                        tryCommit(
                                appendTableFiles,
                                appendChangelog,
                                appendHashIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                Snapshot.CommitKind.APPEND,
                                noConflictCheck(),
                                branchName,
                                null);
                generatedSnapshot += 1; // 增加生成的快照数量
            }

            // 处理压缩部分
            if (!compactTableFiles.isEmpty()
                    || !compactChangelog.isEmpty()
                    || !compactDvIndexFiles.isEmpty()) {
                attempts +=
                        tryCommit(
                                compactTableFiles,
                                compactChangelog,
                                compactDvIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                Snapshot.CommitKind.COMPACT,
                                mustConflictCheck(),
                                branchName,
                                null);
                generatedSnapshot += 1; // 增加生成的快照数量
            }
        } finally {
            // 计算提交耗时（毫秒级）
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            if (this.commitMetrics != null) {
                // 报告提交统计信息
                reportCommit(
                        appendTableFiles,
                        appendChangelog,
                        compactTableFiles,
                        compactChangelog,
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
    }

    // 报告提交统计信息
    private void reportCommit(
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelogFiles,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelogFiles,
            long commitDuration,
            int generatedSnapshots,
            int attempts) {
        // 创建提交统计对象
        CommitStats commitStats =
                new CommitStats(
                        appendTableFiles,
                        appendChangelogFiles,
                        compactTableFiles,
                        compactChangelogFiles,
                        commitDuration,
                        generatedSnapshots,
                        attempts);
        // 报告提交统计信息
        commitMetrics.reportCommit(commitStats);
    }

    // 覆盖分区中的数据
    @Override
    public void overwrite(
            Map<String, String> partition,
            ManifestCommittable committable,
            Map<String, String> properties) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "准备覆盖分区 {}\nManifestCommittable: {}\nProperties: {}",
                    partition,
                    committable,
                    properties);
        }

        long started = System.nanoTime(); // 获取当前时间（纳秒级）
        int generatedSnapshot = 0; // 生成的快照数量
        int attempts = 0; // 提交尝试次数

        // 初始化各种文件清单
        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelog = new ArrayList<>();
        List<IndexManifestEntry> appendHashIndexFiles = new ArrayList<>();
        List<IndexManifestEntry> compactDvIndexFiles = new ArrayList<>();

        // 收集变更内容
        collectChanges(
                committable.fileCommittables(),
                appendTableFiles,
                appendChangelog,
                compactTableFiles,
                compactChangelog,
                appendHashIndexFiles,
                compactDvIndexFiles);

        // 检查是否存在变更日志文件
        if (!appendChangelog.isEmpty() || !compactChangelog.isEmpty()) {
            // 构建警告信息
            StringBuilder warnMessage =
                    new StringBuilder(
                            "覆盖模式目前不提交任何变更日志。\n"
                                    + "请确保正在覆盖的分区未被流式读取器消费。\n"
                                    + "忽略的变更日志文件如下：\n");
            for (ManifestEntry entry : appendChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            for (ManifestEntry entry : compactChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            LOG.warn(warnMessage.toString()); // 输出警告日志
        }

        try {
            boolean skipOverwrite = false; // 是否跳过覆盖
            PartitionPredicate partitionFilter = null; // 分区过滤器

            if (dynamicPartitionOverwrite) {
                if (appendTableFiles.isEmpty()) {
                    // 动态覆盖模式下，如果没有新增文件，跳过覆盖
                    skipOverwrite = true;
                } else {
                    // 动态覆盖模式下，使用变更内容生成过滤器
                    Set<BinaryRow> partitions =
                            appendTableFiles.stream()
                                    .map(ManifestEntry::partition)
                                    .collect(Collectors.toSet());
                    partitionFilter = PartitionPredicate.fromMultiple(partitionType, partitions);
                }
            } else {
                // 静态覆盖模式下，使用指定分区生成过滤器
                Predicate partitionPredicate =
                        createPartitionPredicate(partition, partitionType, partitionDefaultName);
                partitionFilter =
                        PartitionPredicate.fromPredicate(partitionType, partitionPredicate);
                // 检查所有变更是否在指定分区的范围内
                if (partitionFilter != null) {
                    for (ManifestEntry entry : appendTableFiles) {
                        if (!partitionFilter.test(entry.partition())) {
                            throw new IllegalArgumentException(
                                    "试图覆盖分区 "
                                            + partition
                                            + "，但变更内容中的分区 "
                                            + pathFactory.getPartitionString(entry.partition())
                                            + " 不属于该分区");
                        }
                    }
                }
            }

            // 进行覆盖操作
            if (!skipOverwrite) {
                attempts +=
                        tryOverwrite(
                                partitionFilter,
                                appendTableFiles,
                                appendHashIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets());
                generatedSnapshot += 1;
            }

            // 处理压缩部分
            if (!compactTableFiles.isEmpty() || !compactDvIndexFiles.isEmpty()) {
                attempts +=
                        tryCommit(
                                compactTableFiles,
                                Collections.emptyList(),
                                compactDvIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                Snapshot.CommitKind.COMPACT,
                                mustConflictCheck(),
                                branchName,
                                null);
                generatedSnapshot += 1; // 增加生成的快照数量
            }
        } finally {
            // 计算提交耗时（毫秒级）
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            if (this.commitMetrics != null) {
                // 报告提交统计信息
                reportCommit(
                        appendTableFiles,
                        Collections.emptyList(),
                        compactTableFiles,
                        Collections.emptyList(),
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
    }

    // 删除指定分区
    @Override
    public void dropPartitions(List<Map<String, String>> partitions, long commitIdentifier) {
        Preconditions.checkArgument(!partitions.isEmpty(), "分区列表不能为空。");

        if (LOG.isDebugEnabled()) {
            // 输出准备删除的分区
            LOG.debug(
                    "准备删除分区 {}",
                    partitions.stream().map(Objects::toString).collect(Collectors.joining(",")));
        }

        // 构建分区过滤器
        Predicate predicate =
                partitions.stream()
                        .map(
                                partition ->
                                        createPartitionPredicate(
                                                partition, partitionType, partitionDefaultName))
                        .reduce(PredicateBuilder::or)
                        .orElseThrow(() -> new RuntimeException("无法获取分区过滤器。"));
        PartitionPredicate partitionFilter =
                PartitionPredicate.fromPredicate(partitionType, predicate);

        // 调用覆盖写入方法，删除指定分区的数据
        tryOverwrite(
                partitionFilter,
                Collections.emptyList(),
                Collections.emptyList(),
                commitIdentifier,
                null,
                new HashMap<>());
    }

    // 清空表
    @Override
    public void truncateTable(long commitIdentifier) {
        // 调用覆盖写入方法，清空表
        tryOverwrite(
                null,
                Collections.emptyList(),
                Collections.emptyList(),
                commitIdentifier,
                null,
                new HashMap<>());
    }

    // 中止提交操作
    @Override
    public void abort(List<CommitMessage> commitMessages) {
        // 遍历提交消息，删除相关的数据文件
        Map<Pair<BinaryRow, Integer>, DataFilePathFactory> factoryMap = new HashMap<>();
        for (CommitMessage message : commitMessages) {
            DataFilePathFactory pathFactory =
                    factoryMap.computeIfAbsent(
                            Pair.of(message.partition(), message.bucket()),
                            k ->
                                    this.pathFactory.createDataFilePathFactory(
                                            k.getKey(), k.getValue()));
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            // 收集需要删除的文件
            List<DataFileMeta> toDelete = new ArrayList<>();
            toDelete.addAll(commitMessage.newFilesIncrement().newFiles());
            toDelete.addAll(commitMessage.newFilesIncrement().changelogFiles());
            toDelete.addAll(commitMessage.compactIncrement().compactAfter());
            toDelete.addAll(commitMessage.compactIncrement().changelogFiles());

            // 删除文件
            for (DataFileMeta file : toDelete) {
                fileIO.deleteQuietly(pathFactory.toPath(file.fileName()));
            }
        }
    }

    // 设置提交指标
    @Override
    public FileStoreCommit withMetrics(CommitMetrics metrics) {
        this.commitMetrics = metrics;
        return this;
    }

    // 提交统计信息
    @Override
    public void commitStatistics(Statistics stats, long commitIdentifier) {
        // 写入统计信息文件
        String statsFileName = statsFileHandler.writeStats(stats);
        // 调用 tryCommit 方法提交统计信息
        tryCommit(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                commitIdentifier,
                null,
                Collections.emptyMap(),
                Snapshot.CommitKind.ANALYZE,
                noConflictCheck(),
                branchName,
                statsFileName);
    }

    // 获取文件存储路径工厂
    @Override
    public FileStorePathFactory pathFactory() {
        return pathFactory;
    }

    // 获取文件输入/输出接口
    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    // 收集变更内容，生成各种清单条目
    private void collectChanges(
            List<CommitMessage> commitMessages,
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelog,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelog,
            List<IndexManifestEntry> appendHashIndexFiles,
            List<IndexManifestEntry> compactDvIndexFiles) {
        // 遍历提交消息，收集各种文件变更信息
        for (CommitMessage message : commitMessages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            // 新增表文件
            commitMessage
                    .newFilesIncrement()
                    .newFiles()
                    .forEach(m -> appendTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));
            // 删除文件
            commitMessage
                    .newFilesIncrement()
                    .deletedFiles()
                    .forEach(
                            m ->
                                    appendTableFiles.add(
                                            makeEntry(FileKind.DELETE, commitMessage, m)));
            // 变更日志文件
            commitMessage
                    .newFilesIncrement()
                    .changelogFiles()
                    .forEach(m -> appendChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));
            // 压缩前文件
            commitMessage
                    .compactIncrement()
                    .compactBefore()
                    .forEach(
                            m ->
                                    compactTableFiles.add(
                                            makeEntry(FileKind.DELETE, commitMessage, m)));
            // 压缩后文件
            commitMessage
                    .compactIncrement()
                    .compactAfter()
                    .forEach(m -> compactTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));
            // 变更日志文件
            commitMessage
                    .compactIncrement()
                    .changelogFiles()
                    .forEach(m -> compactChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));
            // 索引文件
            commitMessage
                    .indexIncrement()
                    .newIndexFiles()
                    .forEach(
                            f -> {
                                switch (f.indexType()) {
                                    case HASH_INDEX:
                                        appendHashIndexFiles.add(
                                                new IndexManifestEntry(
                                                        FileKind.ADD,
                                                        commitMessage.partition(),
                                                        commitMessage.bucket(),
                                                        f));
                                        break;
                                    case DELETION_VECTORS_INDEX:
                                        compactDvIndexFiles.add(
                                                new IndexManifestEntry(
                                                        FileKind.ADD,
                                                        commitMessage.partition(),
                                                        commitMessage.bucket(),
                                                        f));
                                        break;
                                    default:
                                        throw new RuntimeException("未知的索引类型：" + f.indexType());
                                }
                            });
            // 删除的索引文件
            commitMessage
                    .indexIncrement()
                    .deletedIndexFiles()
                    .forEach(
                            f -> {
                                if (f.indexType().equals(DELETION_VECTORS_INDEX)) {
                                    compactDvIndexFiles.add(
                                            new IndexManifestEntry(
                                                    FileKind.DELETE,
                                                    commitMessage.partition(),
                                                    commitMessage.bucket(),
                                                    f));
                                } else {
                                    throw new RuntimeException(
                                            "不支持删除此类型的索引：" + f.indexType());
                                }
                            });
        }
    }

    // 创建清单条目
    private ManifestEntry makeEntry(
            FileKind kind, CommitMessage commitMessage, DataFileMeta file) {
        return new ManifestEntry(
                kind, commitMessage.partition(), commitMessage.bucket(), numBucket, file);
    }

    // 尝试提交，处理各种文件变更
    private int tryCommit(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Snapshot.CommitKind commitKind,
            ConflictCheck conflictCheck,
            String branchName,
            @Nullable String statsFileName) {
        int cnt = 0; // 尝试次数
        while (true) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot(); // 获取最新快照
            cnt++; // 增加尝试次数
            // 调用 tryCommitOnce 方法尝试提交
            if (tryCommitOnce(
                    tableFiles,
                    changelogFiles,
                    indexFiles,
                    identifier,
                    watermark,
                    logOffsets,
                    commitKind,
                    latestSnapshot,
                    conflictCheck,
                    branchName,
                    statsFileName)) {
                break; // 提交成功，跳出循环
            }
        }
        return cnt; // 返回尝试次数
    }

    // 尝试覆盖写入
    private int tryOverwrite(
            @Nullable PartitionPredicate partitionFilter,
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets) {
        int cnt = 0; // 尝试次数
        while (true) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot(); // 获取最新快照
            cnt++; // 增加尝试次数

            // 准备覆盖写入的变更内容
            List<ManifestEntry> changesWithOverwrite = new ArrayList<>();
            List<IndexManifestEntry> indexChangesWithOverwrite = new ArrayList<>();
            if (latestSnapshot != null) {
                // 读取需要覆盖的分区中的现有文件
                List<ManifestEntry> currentEntries =
                        scan.withSnapshot(latestSnapshot)
                                .withPartitionFilter(partitionFilter)
                                .plan()
                                .files();
                // 生成删除条目
                for (ManifestEntry entry : currentEntries) {
                    changesWithOverwrite.add(
                            new ManifestEntry(
                                    FileKind.DELETE,
                                    entry.partition(),
                                    entry.bucket(),
                                    entry.totalBuckets(),
                                    entry.file()));
                }

                // 处理索引文件
                if (latestSnapshot.indexManifest() != null) {
                    List<IndexManifestEntry> entries =
                            indexManifestFile.read(latestSnapshot.indexManifest());
                    for (IndexManifestEntry entry : entries) {
                        if (partitionFilter == null || partitionFilter.test(entry.partition())) {
                            indexChangesWithOverwrite.add(entry.toDeleteEntry());
                        }
                    }
                }
            }
            changesWithOverwrite.addAll(changes); // 添加新的变更内容
            indexChangesWithOverwrite.addAll(indexFiles); // 添加新的索引文件变更内容

            // 调用 tryCommitOnce 方法尝试提交
            if (tryCommitOnce(
                    changesWithOverwrite,
                    Collections.emptyList(),
                    indexChangesWithOverwrite,
                    identifier,
                    watermark,
                    logOffsets,
                    Snapshot.CommitKind.OVERWRITE,
                    latestSnapshot,
                    mustConflictCheck(),
                    branchName,
                    null)) {
                break; // 提交成功，跳出循环
            }
        }
        return cnt; // 返回尝试次数
    }

    // 尝试提交一次
    @VisibleForTesting
    boolean tryCommitOnce(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Snapshot.CommitKind commitKind,
            @Nullable Snapshot latestSnapshot,
            ConflictCheck conflictCheck,
            String branchName,
            @Nullable String newStatsFileName) {
        // 生成新的快照ID
        long newSnapshotId =
                latestSnapshot == null ? Snapshot.FIRST_SNAPSHOT_ID : latestSnapshot.id() + 1;
        // 生成新快照文件的路径
        Path newSnapshotPath =
                branchName.equals(DEFAULT_MAIN_BRANCH)
                        ? snapshotManager.snapshotPath(newSnapshotId)
                        : snapshotManager.copyWithBranch(branchName).snapshotPath(newSnapshotId);

        if (LOG.isDebugEnabled()) {
            LOG.debug("准备提交表文件到快照 {}", newSnapshotId);
            // 输出表文件清单
            for (ManifestEntry entry : tableFiles) {
                LOG.debug("  * {}", entry);
            }
            LOG.debug("准备提交变更日志文件到快照 {}", newSnapshotId);
            // 输出变更日志清单
            for (ManifestEntry entry : changelogFiles) {
                LOG.debug("  * {}", entry);
            }
        }

        // 检测冲突
        if (latestSnapshot != null && conflictCheck.shouldCheck(latestSnapshot.id())) {
            // 调用 noConflictsOrFail 方法检查冲突
            noConflictsOrFail(latestSnapshot.commitUser(), latestSnapshot, tableFiles);
        }

        Snapshot newSnapshot;
        String previousChangesListName = null; // 旧的清单列表名称
        String newChangesListName = null; // 新的清单列表名称
        String changelogListName = null; // 变更日志列表名称
        String newIndexManifest = null; // 新的索引清单名称
        List<ManifestFileMeta> oldMetas = new ArrayList<>(); // 旧的清单元数据
        List<ManifestFileMeta> newMetas = new ArrayList<>(); // 新的清单元数据
        List<ManifestFileMeta> changelogMetas = new ArrayList<>(); // 变更日志清单元数据
        try {
            long previousTotalRecordCount = 0L; // 之前记录的总数
            Long currentWatermark = watermark; // 当前的水位线
            String previousIndexManifest = null; // 旧的索引清单

            if (latestSnapshot != null) {
                previousTotalRecordCount = scan.totalRecordCount(latestSnapshot); // 获取之前记录的总数
                List<ManifestFileMeta> previousManifests =
                        manifestList.readDataManifests(latestSnapshot); // 读取旧的清单元数据
                oldMetas.addAll(previousManifests); // 添加旧的清单元数据

                // 读取旧的日志偏移量
                Map<Integer, Long> latestLogOffsets = latestSnapshot.logOffsets();
                if (latestLogOffsets != null) {
                    latestLogOffsets.forEach(logOffsets::putIfAbsent);
                }

                // 更新水位线
                Long latestWatermark = latestSnapshot.watermark();
                if (latestWatermark != null) {
                    currentWatermark =
                            currentWatermark == null
                                    ? latestWatermark
                                    : Math.max(currentWatermark, latestWatermark);
                }

                previousIndexManifest = latestSnapshot.indexManifest(); // 获取旧的索引清单
            }

            // 合并清单元数据
            newMetas.addAll(
                    ManifestFileMerger.merge(
                            oldMetas,
                            manifestFile,
                            manifestTargetSize.getBytes(),
                            manifestMergeMinCount,
                            manifestFullCompactionSize.getBytes(),
                            partitionType,
                            manifestReadParallelism));

            // 写入新的清单列表
            previousChangesListName = manifestList.write(newMetas);

            // 处理新增表文件
            long deltaRecordCount = recordCountAdd(tableFiles) - recordCountDelete(tableFiles); // 新增记录数的增量
            long totalRecordCount = previousTotalRecordCount + deltaRecordCount; // 总记录数

            // 写入新增表文件到清单
            List<ManifestFileMeta> newChangesManifests = manifestFile.write(tableFiles);
            newMetas.addAll(newChangesManifests); // 添加到新的清单元数据
            newChangesListName = manifestList.write(newChangesManifests); // 写入新的清单列表

            // 写入变更日志文件到清单
            if (!changelogFiles.isEmpty()) {
                changelogMetas.addAll(manifestFile.write(changelogFiles));
                changelogListName = manifestList.write(changelogMetas); // 写入变更日志列表
            }

            // 写入索引清单
            String indexManifest =
                    indexManifestFile.writeIndexFiles(
                            previousIndexManifest, indexFiles, bucketMode);
            if (!Objects.equals(indexManifest, previousIndexManifest)) {
                newIndexManifest = indexManifest;
            }

            // 获取最新的模式ID
            long latestSchemaId = schemaManager.latest().get().id();

            // 写入新的统计信息或继承之前的
            String statsFileName = null;
            if (newStatsFileName != null) {
                statsFileName = newStatsFileName;
            } else if (latestSnapshot != null) {
                Optional<Statistics> previousStatistic = statsFileHandler.readStats(latestSnapshot);
                if (previousStatistic.isPresent() && previousStatistic.get().schemaId() == latestSchemaId) {
                    statsFileName = latestSnapshot.statistics();
                }
            }

            // 创建新快照对象
            newSnapshot =
                    new Snapshot(
                            newSnapshotId,
                            latestSchemaId,
                            previousChangesListName,
                            newChangesListName,
                            changelogListName,
                            indexManifest,
                            commitUser,
                            identifier,
                            commitKind,
                            System.currentTimeMillis(),
                            logOffsets,
                            totalRecordCount,
                            deltaRecordCount,
                            recordCount(changelogFiles),
                            currentWatermark,
                            statsFileName);
        } catch (Throwable e) {
            // 清理临时文件
            cleanUpTmpManifests(
                    previousChangesListName,
                    newChangesListName,
                    changelogListName,
                    newIndexManifest,
                    oldMetas,
                    newMetas,
                    changelogMetas);
            // 抛出异常
            throw new RuntimeException(
                    String.format(
                            "准备快照 #%d (路径 %s) 时发生异常，用户 %s ，标识 %s，类型 %s。清理。",
                            newSnapshotId,
                            newSnapshotPath.toString(),
                            commitUser,
                            identifier,
                            commitKind.name()),
                    e);
        }

        boolean success;
        try {
            // 定义一个可回调的提交任务
            Callable<Boolean> callable =
                    () -> {
                        // 尝试以原子方式写入新快照文件
                        boolean committed =
                                fileIO.tryToWriteAtomic(newSnapshotPath, newSnapshot.toJson());
                        if (committed) {
                            // 提交最新的提示
                            snapshotManager.commitLatestHint(newSnapshotId);
                        }
                        return committed;
                    };
            // 使用锁（如果存在）执行提交任务
            if (lock != null) {
                success =
                        lock.runWithLock(
                                () ->
                                        !fileIO.exists(newSnapshotPath) && callable.call());
            } else {
                success = callable.call();
            }
        } catch (Throwable e) {
            // 抛出异常，无法确定提交是否成功
            throw new RuntimeException(
                    String.format(
                            "提交快照 #%d (路径 %s) 时发生异常，用户 %s ，标识 %s，类型 %s。无法清理，因为无法确定提交的成功与否。",
                            newSnapshotId,
                            newSnapshotPath,
                            commitUser,
                            identifier,
                            commitKind.name()),
                    e);
        }

        if (success) {
            // 提交成功，输出日志并调用回调
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        String.format(
                                "成功提交快照 #%d (路径 %s)，用户 %s ，标识 %s，类型 %s。",
                                newSnapshotId,
                                newSnapshotPath,
                                commitUser,
                                identifier,
                                commitKind.name()));
            }
            commitCallbacks.forEach(callback -> callback.call(tableFiles, newSnapshot));
            return true;
        }

        // 提交失败，清理临时文件并重试
        LOG.warn(
                String.format(
                        "原子提交失败，快照 #%d (路径 %s)，用户 %s ，标识 %s，类型 %s。清理并重试。",
                        newSnapshotId,
                        newSnapshotPath,
                        commitUser,
                        identifier,
                        commitKind.name()));
        // 清理临时文件
        cleanUpTmpManifests(
                previousChangesListName,
                newChangesListName,
                changelogListName,
                newIndexManifest,
                oldMetas,
                newMetas,
                changelogMetas);
        return false;
    }

    @SafeVarargs
    // 从变更内容中读取所有分区的文件条目
    private final List<SimpleFileEntry> readAllEntriesFromChangedPartitions(
            Snapshot snapshot, List<ManifestEntry>... changes) {
        // 收集变更的分区
        List<BinaryRow> changedPartitions =
                Arrays.stream(changes)
                        .flatMap(Collection::stream)
                        .map(ManifestEntry::partition)
                        .distinct()
                        .collect(Collectors.toList());
        try {
            // 使用扫描器读取指定分区的简单文件条目
            return scan.withSnapshot(snapshot)
                    .withPartitionFilter(changedPartitions)
                    .readSimpleEntries();
        } catch (Throwable e) {
            // 抛出异常
            throw new RuntimeException("无法从变更分区中读取文件清单条目。", e);
        }
    }

    // 检查冲突并抛出异常
    private void noConflictsOrFail(
            String baseCommitUser, Snapshot latestSnapshot, List<ManifestEntry> changes) {
        List<SimpleFileEntry> baseEntries =
                readAllEntriesFromChangedPartitions(latestSnapshot, changes); // 读取基线条目
        noConflictsOrFail(
                baseCommitUser,
                baseEntries,
                SimpleFileEntry.from(changes));
    }

    // 检查冲突并抛出异常
    private void noConflictsOrFail(
            String baseCommitUser,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> changes) {
        // 合并基线条目和变更条目
        List<SimpleFileEntry> allEntries = new ArrayList<>(baseEntries);
        allEntries.addAll(changes);

        // 冲突异常的处理逻辑
        java.util.function.Consumer<Throwable> conflictHandler =
                e -> {
                    // 创建冲突异常对象
                    Pair<RuntimeException, RuntimeException> conflictException =
                            createConflictException(
                                    "文件删除冲突检测失败！放弃提交。",
                                    baseCommitUser,
                                    baseEntries,
                                    changes,
                                    e,
                                    50);
                    LOG.warn("", conflictException.getLeft()); // 输出警告日志
                    // 抛出简化后的异常
                    throw conflictException.getRight();
                };

        Collection<SimpleFileEntry> mergedEntries = null;
        try {
            // 合并文件清单条目并检查冲突
            mergedEntries = FileEntry.mergeEntries(allEntries);
        } catch (Throwable e) {
            // 调用冲突处理逻辑
            conflictHandler.accept(e);
        }

        // 检查是否有删除条目
        assertNoDelete(mergedEntries, conflictHandler);

        // 如果没有键值比较器，跳过键值冲突检测
        if (keyComparator == null) {
            return;
        }

        // 按分区、桶和级别分组处理
        Map<LevelIdentifier, List<SimpleFileEntry>> levels = new HashMap<>();
        for (SimpleFileEntry entry : mergedEntries) {
            int level = entry.level();
            if (level >= 1) {
                levels.computeIfAbsent(
                                new LevelIdentifier(entry.partition(), entry.bucket(), level),
                                lv -> new ArrayList<>())
                        .add(entry);
            }
        }

        // 检查每个级别是否有键值范围冲突
        for (List<SimpleFileEntry> entries : levels.values()) {
            entries.sort((a, b) -> keyComparator.compare(a.minKey(), b.minKey())); // 按键值排序
            for (int i = 0; i + 1 < entries.size(); i++) {
                SimpleFileEntry a = entries.get(i);
                SimpleFileEntry b = entries.get(i + 1);
                if (keyComparator.compare(a.maxKey(), b.minKey()) >= 0) {
                    // 调用冲突处理逻辑
                    Pair<RuntimeException, RuntimeException> conflictException =
                            createConflictException(
                                    "LSM冲突检测失败！放弃提交。冲突文件如下：\n"
                                            + a.identifier().toString(pathFactory)
                                            + "\n"
                                            + b.identifier().toString(pathFactory),
                                    baseCommitUser,
                                    baseEntries,
                                    changes,
                                    null,
                                    50);

                    LOG.warn("", conflictException.getLeft());
                    throw conflictException.getRight();
                }
            }
        }
    }

    // 检查是否有未添加的删除条目
    private void assertNoDelete(
            Collection<SimpleFileEntry> mergedEntries,
            java.util.function.Consumer<Throwable> conflictHandler) {
        try {
            // 遍历合并后的条目，检查是否有未添加的删除条目
            for (SimpleFileEntry entry : mergedEntries) {
                Preconditions.checkState(
                        entry.kind() != FileKind.DELETE,
                        "尝试删除不存在的文件 %s。",
                        entry.fileName());
            }
        } catch (Throwable e) {
            if (partitionExpire != null && partitionExpire.isValueExpiration()) {
                // 检查分区是否已过期
                Set<BinaryRow> deletedPartitions = new HashSet<>();
                for (SimpleFileEntry entry : mergedEntries) {
                    if (entry.kind() == FileKind.DELETE) {
                        deletedPartitions.add(entry.partition());
                    }
                }
                if (partitionExpire.isValueAllExpired(deletedPartitions)) {
                    // 生成已过期分区的字符串列表
                    List<String> expiredPartitions =
                            deletedPartitions.stream()
                                    .map(
                                            partition ->
                                                    partToSimpleString(
                                                            partitionType, partition, "-", 200))
                                    .collect(Collectors.toList());
                    throw new RuntimeException(
                            "您正在写入已过期的分区，这会导致作业失败。"
                                    + "为了避免作业失败，您可以通过过滤数据来避免写入过期分区。"
                                    + "否则，连续的过期记录将导致作业连续失败。"
                                    + "已过期的分区如下："
                                    + expiredPartitions);
                }
            }
            // 调用冲突处理逻辑
            conflictHandler.accept(e);
        }
    }

    /**
     * 构造冲突异常。返回的异常是完整的和简化的，如果条目长度超过最大限制，会生成简化的异常。
     */
    private Pair<RuntimeException, RuntimeException> createConflictException(
            String message,
            String baseCommitUser,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> changes,
            Throwable cause,
            int maxEntry) {
        // 构建冲突可能的原因
        String possibleCauses =
                String.join(
                        "\n",
                        "不要惊慌！",
                        "提交过程中出现冲突是正常的，这种失败旨在解决冲突。",
                        "冲突主要由以下情况引起：",
                        "1. 多个作业同时写入同一分区，"
                                + "或者您使用 STATEMENT SET 执行了多个插入到同一 Paimon 表的语句。",
                        "   您可能在下面看到不同的基线提交用户和当前提交用户。",
                        "   您可以使用专用的 compaction 作业（<url id=\"\" type=\"url\" status=\"\" title=\"\" wc=\"\">https://paimon.apache.org/docs/master/maintenance/dedicated-compaction#dedicated-compaction-job</url>）来支持多写入。",
                        "2. 您正在从一个旧的保存点恢复，或者您从保存点创建了多个作业。",
                        "   在这种情况下，作业会连续失败，以防止元数据损坏。",
                        "   您可以恢复到最新的保存点，或者可以将表回滚到保存点对应的快照。");
        // 构建提交用户信息
        String commitUserString =
                "基线提交用户是："
                        + baseCommitUser
                        + "; 当前提交用户是："
                        + commitUser;
        // 构建基线条目信息
        String baseEntriesString =
                "基线条目如下：\n"
                        + baseEntries.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining("\n"));
        // 构建变更条目信息
        String changesString =
                "变更条目如下：\n"
                        + changes.stream().map(Object::toString).collect(Collectors.joining("\n"));

        // 构建完整的异常对象
        RuntimeException fullException =
                new RuntimeException(
                        message
                                + "\n\n"
                                + possibleCauses
                                + "\n\n"
                                + commitUserString
                                + "\n\n"
                                + baseEntriesString
                                + "\n\n"
                                + changesString,
                        cause);
        // 构建简化的异常对象
        RuntimeException simplifiedException;
        if (baseEntries.size() > maxEntry || changes.size() > maxEntry) {
            // 简化的基线条目信息
            baseEntriesString =
                    "基线条目如下：\n"
                            + baseEntries.subList(0, Math.min(baseEntries.size(), maxEntry))
                            .stream()
                            .map(Object::toString)
                            .collect(Collectors.joining("\n"));
            // 简化的变更条目信息
            changesString =
                    "变更条目如下：\n"
                            + changes.subList(0, Math.min(changes.size(), maxEntry)).stream()
                            .map(Object::toString)
                            .collect(Collectors.joining("\n"));
            simplifiedException =
                    new RuntimeException(
                            message
                                    + "\n\n"
                                    + possibleCauses
                                    + "\n\n"
                                    + commitUserString
                                    + "\n\n"
                                    + baseEntriesString
                                    + "\n\n"
                                    + changesString
                                    + "\n\n"
                                    + "由于条目数量过多，上文可能显示不全，详细信息请查看 taskmanager.log。",
                            cause);
            return Pair.of(fullException, simplifiedException);
        } else {
            return Pair.of(fullException, fullException);
        }
    }

    // 清理临时清单文件
    private void cleanUpTmpManifests(
            String previousChangesListName,
            String newChangesListName,
            String changelogListName,
            String newIndexManifest,
            List<ManifestFileMeta> oldMetas,
            List<ManifestFileMeta> newMetas,
            List<ManifestFileMeta> changelogMetas) {
        // 清理清单列表文件
        if (previousChangesListName != null) {
            manifestList.delete(previousChangesListName);
        }
        if (newChangesListName != null) {
            manifestList.delete(newChangesListName);
        }
        if (changelogListName != null) {
            manifestList.delete(changelogListName);
        }
        if (newIndexManifest != null) {
            indexManifestFile.delete(newIndexManifest);
        }

        // 清理合并后的新清单文件
        Set<ManifestFileMeta> oldMetaSet = new HashSet<>(oldMetas);
        for (ManifestFileMeta suspect : newMetas) {
            if (!oldMetaSet.contains(suspect)) {
                manifestList.delete(suspect.fileName());
            }
        }

        // 清理变更日志清单文件
        for (ManifestFileMeta meta : changelogMetas) {
            manifestList.delete(meta.fileName());
        }
    }

    // 关闭资源
    @Override
    public void close() {
        // 关闭提交回调
        for (CommitCallback callback : commitCallbacks) {
            IOUtils.closeQuietly(callback);
        }
    }

    // 分区级别标识符
    private static class LevelIdentifier {

        private final BinaryRow partition; // 分区
        private final int bucket; // 桶
        private final int level; // 层级

        private LevelIdentifier(BinaryRow partition, int bucket, int level) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LevelIdentifier)) {
                return false;
            }
            LevelIdentifier that = (LevelIdentifier) o;
            return Objects.equals(partition, that.partition)
                    && bucket == that.bucket
                    && level == that.level;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, level);
        }
    }

    // 冲突检测策略接口
    interface ConflictCheck {
        boolean shouldCheck(long latestSnapshot);
    }

    // 使用已检查的最新快照ID的冲突检测策略
    static ConflictCheck hasConflictChecked(@Nullable Long checkedLatestSnapshotId) {
        return latestSnapshot -> !Objects.equals(latestSnapshot, checkedLatestSnapshotId);
    }

    // 不进行冲突检测的策略
    static ConflictCheck noConflictCheck() {
        return latestSnapshot -> false;
    }

    // 必须进行冲突检测的策略
    @VisibleForTesting
    static ConflictCheck mustConflictCheck() {
        return latestSnapshot -> true;
    }
}
