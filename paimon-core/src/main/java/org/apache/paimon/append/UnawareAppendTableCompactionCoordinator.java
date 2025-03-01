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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.append.AppendDeletionFileMaintainer;
import org.apache.paimon.deletionvectors.append.UnawareAppendDeletionFileMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * 用于仅追加表的压缩协调器。
 *
 * <p>注意：{@link UnawareAppendTableCompactionCoordinator} 会扫描快照中的文件，
 *      读取追加（APPEND）和压缩（COMPACT）快照，然后加载这些新文件。
 *      它会尽力为扫描到的恢复文件生成压缩任务，但为了减少内存使用，它不会长时间保留单个文件。
 *      经过十次扫描后，具有一个分区的单个文件将被忽略并从内存中移除，这意味着它将不再参与压缩，直到重新启动压缩作业。
 *
 * <p>当第三个任务在最新快照中删除文件时（包括批量删除/更新和覆盖），协调器中的文件仍然会保留并参与压缩任务。
 *      当这种情况发生时，压缩作业将在提交阶段失败，并故障转移到重新扫描最新快照中的恢复文件。
 */
public class UnawareAppendTableCompactionCoordinator {

    // 文件老化并被移除的次数阈值，如果一个分区的文件在内存中停留超过10次扫描周期，就会被清除
    protected static final int REMOVE_AGE = 10;
    // 文件年龄阈值，当文件年龄超过5时且满足压缩条件，会触发压缩
    protected static final int COMPACT_AGE = 5;

    // 快照管理器，用于管理表的快照
    private final SnapshotManager snapshotManager;
    // 快照读取器，用于从快照中读取数据
    private final SnapshotReader snapshotReader;
    // 目标文件大小，压缩后的文件大小尽量趋近于这个值
    private final long targetFileSize;
    // 触发压缩的最小文件大小，小于该值的文件会被视为小文件，可能触发压缩
    private final long compactionFileSize;
    // 触发压缩的最小文件数量，小于该值不会触发压缩
    private final int minFileNum;
    // 触发压缩的最大文件数量，超过该值会强制触发压缩
    private final int maxFileNum;
    // 是否是流模式，流模式下会持续扫描新的文件
    private final boolean streamingMode;
    // 删除向量维护器缓存，用于管理删除向量
    private final DvMaintainerCache dvMaintainerCache;

    // 下一个需要扫描的快照ID
    @Nullable private Long nextSnapshot = null;

    // 分区压缩协调器映射，每个分区对应一个协调器
    final Map<BinaryRow, PartitionCompactCoordinator> partitionCompactCoordinators =
            new HashMap<>();

    // 构造方法，初始化协调器对象
    public UnawareAppendTableCompactionCoordinator(FileStoreTable table) {
        this(table, true);
    }

    // 构造方法，允许指定是否是流模式
    public UnawareAppendTableCompactionCoordinator(FileStoreTable table, boolean isStreaming) {
        this(table, isStreaming, null);
    }

    // 构造方法，允许指定是否是流模式以及过滤器
    public UnawareAppendTableCompactionCoordinator(
            FileStoreTable table, boolean isStreaming, @Nullable Predicate filter) {
        // 确保表没有主键
        Preconditions.checkArgument(table.primaryKeys().isEmpty());
        // 初始化快照管理器
        this.snapshotManager = table.snapshotManager();
        // 初始化快照读取器
        this.snapshotReader = table.newSnapshotReader();
        // 设置过滤器
        if (filter != null) {
            snapshotReader.withFilter(filter);
        }
        // 设置流模式
        this.streamingMode = isStreaming;
        // 初始化选项
        CoreOptions options = table.coreOptions();
        // 设置目标文件大小
        this.targetFileSize = options.targetFileSize(false);
        // 设置触发压缩的最小文件大小
        this.compactionFileSize = options.compactionFileSize(false);
        // 设置触发压缩的最小文件数量
        this.minFileNum = options.compactionMinFileNum();
        // 设置触发压缩的最大文件数量
        // this is global compaction, avoid too many compaction tasks
        this.maxFileNum = options.compactionMaxFileNum().orElse(50);
        // 初始化删除向量维护器缓存
        this.dvMaintainerCache =
                options.deletionVectorsEnabled()
                        ? new DvMaintainerCache(table.store().newIndexFileHandler())
                        : null;
    }

    // 运行压缩协调器，生成压缩任务
    public List<UnawareAppendCompactionTask> run() {
        // 扫描文件
        if (scan()) {
            // 生成压缩计划
            return compactPlan();
        }

        return Collections.emptyList();
    }

    // 扫描快照中的文件
    @VisibleForTesting
    boolean scan() {
        List<DataSplit> splits;
        boolean hasResult = false;
        while (!(splits = plan()).isEmpty()) {
            hasResult = true;
            splits.forEach(split -> notifyNewFiles(split.partition(), split.dataFiles()));
            // 如果不是流模式，只扫描一次
            if (!streamingMode) {
                break;
            }
        }
        return hasResult;
    }

    // 生成数据分片
    @VisibleForTesting
    List<DataSplit> plan() {
        if (nextSnapshot == null) {
            // 初始化下一个快照ID
            nextSnapshot = snapshotManager.latestSnapshotId();
            if (nextSnapshot == null) {
                return emptyList();
            }
            // 设置扫描模式为全部扫描
            snapshotReader.withMode(ScanMode.ALL);
        } else {
            // 如果不是流模式，抛出异常
            if (!streamingMode) {
                throw new EndOfScanException();
            }
            // 设置扫描模式为增量扫描
            snapshotReader.withMode(ScanMode.DELTA);
        }

        // 检查快照是否存在
        if (!snapshotManager.snapshotExists(nextSnapshot)) {
            return emptyList();
        }

        // 获取快照
        Snapshot snapshot = snapshotManager.snapshot(nextSnapshot);
        nextSnapshot++;

        // 刷新删除向量维护器缓存
        if (dvMaintainerCache != null) {
            dvMaintainerCache.refresh();
        }
        // 定义过滤器，过滤出需要压缩的文件
        Filter<ManifestEntry> entryFilter =
                entry -> {
                    // 如果文件大小小于触发压缩的文件大小，需要压缩
                    if (entry.file().fileSize() < compactionFileSize) {
                        return true;
                    }
                    // 如果有删除向量，且文件有删除文件，需要压缩
                    if (dvMaintainerCache != null) {
                        return dvMaintainerCache
                                .dvMaintainer(entry.partition())
                                .hasDeletionFile(entry.fileName());
                    }
                    return false;
                };
        // 根据过滤器读取数据分片
        return snapshotReader
                .withManifestEntryFilter(entryFilter)
                .withSnapshot(snapshot)
                .read()
                .dataSplits();
    }

    // 通知新的文件
    @VisibleForTesting
    void notifyNewFiles(BinaryRow partition, List<DataFileMeta> files) {
        // 定义过滤器，过滤出需要压缩的文件
        java.util.function.Predicate<DataFileMeta> filter =
                file -> {
                    // 如果没有删除向量或者文件没有对应的删除文件，且文件大小小于触发压缩的文件大小，需要压缩
                    if (dvMaintainerCache == null
                            || dvMaintainerCache
                            .dvMaintainer(partition)
                            .getDeletionFile(file.fileName())
                            == null) {
                        return file.fileSize() < compactionFileSize;
                    }
                    // 如果有删除向量，且文件有对应的删除文件，总是需要压缩
                    return true;
                };
        // 过滤出需要压缩的文件
        List<DataFileMeta> toCompact = files.stream().filter(filter).collect(Collectors.toList());
        // 将文件添加到分区压缩协调器
        partitionCompactCoordinators
                .computeIfAbsent(partition, pp -> new PartitionCompactCoordinator(partition))
                .addFiles(toCompact);
    }

    // 生成压缩计划
    @VisibleForTesting
    // generate compaction task to the next stage
    List<UnawareAppendCompactionTask> compactPlan() {
        // 第一次遍历，生成压缩任务
        List<UnawareAppendCompactionTask> tasks =
                partitionCompactCoordinators.values().stream()
                        .flatMap(s -> s.plan().stream())
                        .collect(Collectors.toList());

        // 第二次遍历，清理不需要的分区协调器
        new ArrayList<>(partitionCompactCoordinators.values())
                .stream()
                .filter(PartitionCompactCoordinator::readyToRemove)
                .map(PartitionCompactCoordinator::partition)
                .forEach(partitionCompactCoordinators::remove);

        return tasks;
    }

    // 列出所有待压缩的文件
    @VisibleForTesting
    HashSet<DataFileMeta> listRestoredFiles() {
        HashSet<DataFileMeta> sets = new HashSet<>();
        partitionCompactCoordinators
                .values()
                .forEach(
                        partitionCompactCoordinator ->
                                sets.addAll(partitionCompactCoordinator.toCompact));
        return sets;
    }

    /** Coordinator for a single partition. */
    class PartitionCompactCoordinator {

        // 分区信息
        private final BinaryRow partition;
        // 需要压缩的文件集合
        private final HashSet<DataFileMeta> toCompact = new HashSet<>();
        // 文件年龄计数
        int age = 0;

        // 构造方法
        public PartitionCompactCoordinator(BinaryRow partition) {
            this.partition = partition;
        }

        // 生成压缩计划
        public List<UnawareAppendCompactionTask> plan() {
            return pickCompact();
        }

        // 获取分区信息
        public BinaryRow partition() {
            return partition;
        }

        // 生成压缩任务
        private List<UnawareAppendCompactionTask> pickCompact() {
            // 获取待压缩的文件分组
            List<List<DataFileMeta>> waitCompact = agePack();
            // 将分组转换为压缩任务
            return waitCompact.stream()
                    .map(files -> new UnawareAppendCompactionTask(partition, files))
                    .collect(Collectors.toList());
        }

        // 添加文件到待压缩集合
        public void addFiles(List<DataFileMeta> dataFileMetas) {
            // 重置年龄
            age = 0;
            // 添加文件到待压缩集合
            toCompact.addAll(dataFileMetas);
        }

        // 判断分区协调器是否可以被移除
        public boolean readyToRemove() {
            return toCompact.isEmpty() || age > REMOVE_AGE;
        }

        // 根据年龄分组文件
        private List<List<DataFileMeta>> agePack() {
            // 生成分组
            List<List<DataFileMeta>> packed;
            if (dvMaintainerCache == null) {
                // 普通模式下进行分组
                packed = pack(toCompact);
            } else {
                // 删除向量模式下进行分组
                packed = packInDeletionVectorVMode(toCompact);
            }
            // 如果没有分组，检查是否需要强制压缩
            if (packed.isEmpty()) {
                // 增加年龄，如果超过阈值且文件数大于1，强制生成分组
                if (++age > COMPACT_AGE && toCompact.size() > 1) {
                    List<DataFileMeta> all = new ArrayList<>(toCompact);
                    // 清空待压缩集合
                    toCompact.clear();
                    packed = Collections.singletonList(all);
                }
            }

            return packed;
        }

        // 普通模式下的文件分组
        private List<List<DataFileMeta>> pack(Set<DataFileMeta> toCompact) {
            // 将文件按大小排序，从小到大
            ArrayList<DataFileMeta> files = new ArrayList<>(toCompact);
            files.sort(Comparator.comparingLong(DataFileMeta::fileSize));

            // 分组结果
            List<List<DataFileMeta>> result = new ArrayList<>();
            FileBin fileBin = new FileBin();
            for (DataFileMeta fileMeta : files) {
                fileBin.addFile(fileMeta);
                if (fileBin.binReady()) {
                    result.add(new ArrayList<>(fileBin.bin));
                    // 重置文件分组
                    fileBin.reset();
                }
            }
            return result;
        }

        // 删除向量模式下的文件分组
        private List<List<DataFileMeta>> packInDeletionVectorVMode(Set<DataFileMeta> toCompact) {
            // 按索引文件分组
            Map<IndexFileMeta, List<DataFileMeta>> filesWithDV = new HashMap<>();
            Set<DataFileMeta> rest = new HashSet<>();
            for (DataFileMeta dataFile : toCompact) {
                // 获取索引文件
                IndexFileMeta indexFile =
                        dvMaintainerCache.dvMaintainer(partition).getIndexFile(dataFile.fileName());
                if (indexFile == null) {
                    // 没有索引文件的文件加入剩余集合
                    rest.add(dataFile);
                } else {
                    // 按索引文件分组
                    filesWithDV.computeIfAbsent(indexFile, f -> new ArrayList<>()).add(dataFile);
                }
            }

            // 合并分组
            List<List<DataFileMeta>> result = new ArrayList<>(filesWithDV.values());
            if (rest.size() > 1) {
                result.addAll(pack(rest));
            }
            return result;
        }

        /**
         * 用于确定文件是否达到压缩条件的文件分组辅助类
         */
        private class FileBin {
            // 文件分组
            List<DataFileMeta> bin = new ArrayList<>();
            // 文件大小总和
            long totalFileSize = 0;
            // 文件数量
            int fileNum = 0;

            // 重置文件分组
            public void reset() {
                bin.forEach(toCompact::remove);
                bin.clear();
                totalFileSize = 0;
                fileNum = 0;
            }

            // 添加文件到分组
            public void addFile(DataFileMeta file) {
                totalFileSize += file.fileSize();
                fileNum++;
                bin.add(file);
            }

            // 判断是否达到压缩条件
            public boolean binReady() {
                return (totalFileSize >= targetFileSize && fileNum >= minFileNum)
                        || fileNum >= maxFileNum;
            }
        }
    }

    // 删除向量维护器缓存
    private class DvMaintainerCache {

        // 索引文件处理器
        private final IndexFileHandler indexFileHandler;

        // 缓存，保存分区对应的删除向量维护器
        /** Should be thread safe, ManifestEntryFilter will be invoked in many threads. */
        private final Map<BinaryRow, UnawareAppendDeletionFileMaintainer> cache =
                new ConcurrentHashMap<>();

        // 构造方法
        private DvMaintainerCache(IndexFileHandler indexFileHandler) {
            this.indexFileHandler = indexFileHandler;
        }

        // 刷新缓存
        private void refresh() {
            this.cache.clear();
        }

        // 获取分区对应的删除向量维护器
        private UnawareAppendDeletionFileMaintainer dvMaintainer(BinaryRow partition) {
            UnawareAppendDeletionFileMaintainer maintainer = cache.get(partition);
            if (maintainer == null) {
                // 同步操作，防止并发问题
                synchronized (this) {
                    maintainer =
                            AppendDeletionFileMaintainer.forUnawareAppend(
                                    indexFileHandler,
                                    snapshotManager.latestSnapshotId(),
                                    partition);
                }
                // 将维护器放入缓存
                cache.put(partition, maintainer);
            }
            return maintainer;
        }
    }
}