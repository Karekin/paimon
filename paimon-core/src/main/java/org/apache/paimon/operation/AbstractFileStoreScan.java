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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.operation.metrics.ScanStats;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.getExecutorService;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * Default implementation of {@link FileStoreScan}.
 *
 * @author [Original Author]
 * @modified [Your Name]
 * @since [Version]
 */
public abstract class AbstractFileStoreScan implements FileStoreScan {

    // 存储分区类型，用于描述文件存储的分区结构
    private final RowType partitionType;

    // 快照管理器，用于管理不同版本的快照数据
    private final SnapshotManager snapshotManager;

    // 清单文件工厂，用于创建和操作清单文件
    private final ManifestFile.Factory manifestFileFactory;

    // 清单列表，用于存储和管理多个清单文件
    private final ManifestList manifestList;

    // 当前文件存储的桶（bucket）数量
    private final int numOfBuckets;

    // 是否需要检查桶的数量
    private final boolean checkNumOfBuckets;

    // 扫描清单文件的并行度配置
    private final Integer scanManifestParallelism;

    // 缓存表模式，使用并发哈希表存储不同表的模式信息
    private final ConcurrentMap<Long, TableSchema> tableSchemas;

    // 模式管理器，用于管理表的模式
    private final SchemaManager schemaManager;

    // 当前表的模式
    private final TableSchema schema;

    // 桶过滤器，用于过滤特定的桶
    protected final ScanBucketFilter bucketKeyFilter;

    // 分区过滤条件，用户过滤特定的分区
    private PartitionPredicate partitionFilter;

    // 指定的快照，用于基于特定快照进行数据扫描
    private Snapshot specifiedSnapshot = null;

    // 桶过滤器，用于过滤特定的桶
    private Filter<Integer> bucketFilter = null;

    // 指定的清单文件元数据，用于直接从清单文件进行扫描
    private List<ManifestFileMeta> specifiedManifests = null;

    // 扫描模式，包括全量扫描、增量扫描等
    protected ScanMode scanMode = ScanMode.ALL;

    // 层级过滤器，用于过滤特定层级的数据
    private Filter<Integer> levelFilter = null;

    // 清单条目过滤器，用于过滤特定的清单条目
    private Filter<ManifestEntry> manifestEntryFilter = null;

    // 文件名过滤器，用于根据文件名进行过滤
    private Filter<String> fileNameFilter = null;

    // 清单缓存过滤器，用于过滤缓存中的清单条目
    private ManifestCacheFilter manifestCacheFilter = null;

    // 扫描指标，用于记录扫描过程中的性能指标
    private ScanMetrics scanMetrics = null;

    /**
     * 构造函数，初始化文件存储扫描器。
     *
     * @param partitionType          分区类型
     * @param bucketKeyFilter        桶过滤器
     * @param snapshotManager        快照管理器
     * @param schemaManager          模式管理器
     * @param schema                 表模式
     * @param manifestFileFactory    清单文件工厂
     * @param manifestListFactory    清单列表工厂
     * @param numOfBuckets           桶数量
     * @param checkNumOfBuckets      是否检查桶数量
     * @param scanManifestParallelism 扫描清单并行度
     */
    public AbstractFileStoreScan(
            RowType partitionType,
            ScanBucketFilter bucketKeyFilter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            int numOfBuckets,
            boolean checkNumOfBuckets,
            Integer scanManifestParallelism) {
        this.partitionType = partitionType;
        this.bucketKeyFilter = bucketKeyFilter;
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.manifestFileFactory = manifestFileFactory;
        this.manifestList = manifestListFactory.create();
        this.numOfBuckets = numOfBuckets;
        this.checkNumOfBuckets = checkNumOfBuckets;
        this.tableSchemas = new ConcurrentHashMap<>();
        this.scanManifestParallelism = scanManifestParallelism;
    }

    /**
     * 设置基于谓词的分区过滤条件。
     *
     * @param predicate 过滤谓词
     * @return 当前对象
     */
    @Override
    public FileStoreScan withPartitionFilter(Predicate predicate) {
        this.partitionFilter = PartitionPredicate.fromPredicate(partitionType, predicate);
        return this;
    }

    /**
     * 设置基于多个分区的过滤条件。
     *
     * @param partitions 分区列表
     * @return 当前对象
     */
    @Override
    public FileStoreScan withPartitionFilter(List<BinaryRow> partitions) {
        this.partitionFilter = PartitionPredicate.fromMultiple(partitionType, partitions);
        return this;
    }

    /**
     * 设置基于分区过滤器的条件。
     *
     * @param predicate 分区过滤器
     * @return 当前对象
     */
    @Override
    public FileStoreScan withPartitionFilter(PartitionPredicate predicate) {
        this.partitionFilter = predicate;
        return this;
    }

    /**
     * 设置指定的桶进行过滤。
     *
     * @param bucket 桶编号
     * @return 当前对象
     */
    @Override
    public FileStoreScan withBucket(int bucket) {
        this.bucketFilter = i -> i == bucket;
        return this;
    }

    /**
     * 设置基于桶过滤器的条件。
     *
     * @param bucketFilter 桶过滤器
     * @return 当前对象
     */
    @Override
    public FileStoreScan withBucketFilter(Filter<Integer> bucketFilter) {
        this.bucketFilter = bucketFilter;
        return this;
    }

    /**
     * 设置基于分区和桶的过滤条件。
     *
     * @param partition 分区
     * @param bucket    桶编号
     * @return 当前对象
     */
    @Override
    public FileStoreScan withPartitionBucket(BinaryRow partition, int bucket) {
        if (manifestCacheFilter != null && manifestFileFactory.isCacheEnabled()) {
            checkArgument(
                    manifestCacheFilter.test(partition, bucket),
                    String.format(
                            "This is a bug! The partition %s and bucket %s is filtered!",
                            partition, bucket));
        }
        withPartitionFilter(Collections.singletonList(partition));
        withBucket(bucket);
        return this;
    }

    /**
     * 设置基于快照ID的过滤条件。
     *
     * @param snapshotId 快照ID
     * @return 当前对象
     */
    @Override
    public FileStoreScan withSnapshot(long snapshotId) {
        checkState(specifiedManifests == null, "Cannot set both snapshot and manifests.");
        this.specifiedSnapshot = snapshotManager.snapshot(snapshotId);
        return this;
    }

    /**
     * 设置基于快照对象的过滤条件。
     *
     * @param snapshot 快照对象
     * @return 当前对象
     */
    @Override
    public FileStoreScan withSnapshot(Snapshot snapshot) {
        checkState(specifiedManifests == null, "Cannot set both snapshot and manifests.");
        this.specifiedSnapshot = snapshot;
        return this;
    }

    /**
     * 设置基于清单文件元数据的过滤条件。
     *
     * @param manifests 清单文件元数据列表
     * @return 当前对象
     */
    @Override
    public FileStoreScan withManifestList(List<ManifestFileMeta> manifests) {
        checkState(specifiedSnapshot == null, "Cannot set both snapshot and manifests.");
        this.specifiedManifests = manifests;
        return this;
    }

    /**
     * 设置扫描模式。
     *
     * @param scanMode 扫描模式
     * @return 当前对象
     */
    @Override
    public FileStoreScan withKind(ScanMode scanMode) {
        this.scanMode = scanMode;
        return this;
    }

    /**
     * 设置层级过滤条件。
     *
     * @param levelFilter 层级过滤器
     * @return 当前对象
     */
    @Override
    public FileStoreScan withLevelFilter(Filter<Integer> levelFilter) {
        this.levelFilter = levelFilter;
        return this;
    }

    /**
     * 设置清单条目过滤条件。
     *
     * @param filter 清单条目过滤器
     * @return 当前对象
     */
    @Override
    public FileStoreScan withManifestEntryFilter(Filter<ManifestEntry> filter) {
        this.manifestEntryFilter = filter;
        return this;
    }

    /**
     * 设置清单缓存过滤条件。
     *
     * @param manifestFilter 清单缓存过滤器
     * @return 当前对象
     */
    @Override
    public FileStoreScan withManifestCacheFilter(ManifestCacheFilter manifestFilter) {
        this.manifestCacheFilter = manifestFilter;
        return this;
    }

    /**
     * 设置文件名过滤条件。
     *
     * @param fileNameFilter 文件名过滤器
     * @return 当前对象
     */
    @Override
    public FileStoreScan withDataFileNameFilter(Filter<String> fileNameFilter) {
        this.fileNameFilter = fileNameFilter;
        return this;
    }

    /**
     * 设置扫描指标。
     *
     * @param metrics 扫描指标对象
     * @return 当前对象
     */
    @Override
    public FileStoreScan withMetrics(ScanMetrics metrics) {
        this.scanMetrics = metrics;
        return this;
    }

    /**
     * 执行扫描计划。
     *
     * @return 扫描计划结果
     */
    @Override
    public Plan plan() {

        // 执行实际的扫描计划逻辑
        Pair<Snapshot, List<ManifestEntry>> planResult = doPlan();

        // 构建扫描计划结果对象
        final Snapshot readSnapshot = planResult.getLeft();
        final List<ManifestEntry> files = planResult.getRight();

        return new Plan() {
            @Nullable
            @Override
            public Long watermark() {
                return readSnapshot == null ? null : readSnapshot.watermark();
            }

            @Nullable
            @Override
            public Snapshot snapshot() {
                return readSnapshot;
            }

            @Override
            public List<ManifestEntry> files() {
                return files;
            }
        };
    }

    /**
     * 读取简单的文件条目。
     *
     * @return 简单的文件条目列表
     */
    @Override
    public List<SimpleFileEntry> readSimpleEntries() {
        // 读取清单文件元数据
        List<ManifestFileMeta> manifests = readManifests().getRight();
        // 合并并过滤文件条目
        Collection<SimpleFileEntry> mergedEntries =
                readAndMergeFileEntries(manifests, this::readSimpleEntries, Filter.alwaysTrue());
        return new ArrayList<>(mergedEntries);
    }

    /**
     * 读取分区条目。
     *
     * @return 分区条目列表
     */
    @Override
    public List<PartitionEntry> readPartitionEntries() {
        // 读取清单文件元数据
        List<ManifestFileMeta> manifests = readManifests().getRight();
        // 并发处理每个分区
        Map<BinaryRow, PartitionEntry> partitions = new ConcurrentHashMap<>();
        ThreadPoolExecutor executor = getExecutorService(scanManifestParallelism);
        Consumer<ManifestFileMeta> processor =
                m ->
                        PartitionEntry.merge(
                                PartitionEntry.merge(readManifestFileMeta(m)), partitions);
        randomlyOnlyExecute(executor, processor, manifests);
        return partitions.values().stream()
                .filter(p -> p.fileCount() > 0)
                .collect(Collectors.toList());
    }

    /**
     * 执行实际的扫描计划逻辑。
     *
     * @return 扫描计划结果
     */
    private Pair<Snapshot, List<ManifestEntry>> doPlan() {
        // 记录扫描开始时间
        long started = System.nanoTime();

        // 读取清单文件元数据
        Pair<Snapshot, List<ManifestFileMeta>> snapshotListPair = readManifests();
        Snapshot snapshot = snapshotListPair.getLeft();
        List<ManifestFileMeta> manifests = snapshotListPair.getRight();

        // 计算初始的数据文件数量
        long startDataFiles =
                manifests.stream().mapToLong(f -> f.numAddedFiles() - f.numDeletedFiles()).sum();

        // 合并并过滤文件条目
        Collection<ManifestEntry> mergedEntries =
                readAndMergeFileEntries(
                        manifests, this::readManifestFileMeta, this::filterUnmergedManifestEntry);

        // 进一步过滤文件条目
        List<ManifestEntry> files = new ArrayList<>();
        long skippedByPartitionAndStats = startDataFiles - mergedEntries.size();
        for (ManifestEntry file : mergedEntries) {
            if (checkNumOfBuckets && file.totalBuckets() != numOfBuckets) {
                // 检查桶数量是否匹配
                String partInfo =
                        partitionType.getFieldCount() > 0
                                ? "partition "
                                + FileStorePathFactory.getPartitionComputer(
                                        partitionType,
                                        CoreOptions.PARTITION_DEFAULT_NAME
                                                .defaultValue())
                                .generatePartValues(file.partition())
                                : "table";
                throw new RuntimeException(
                        String.format(
                                "Try to write %s with a new bucket num %d, but the previous bucket num is %d. "
                                        + "Please switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.",
                                partInfo, numOfBuckets, file.totalBuckets()));
            }

            // 过滤未合并的文件条目
            if (filterMergedManifestEntry(file)) {
                files.add(file);
            }
        }

        // 根据分区和桶过滤文件条目
        long afterBucketFilter = files.size();
        long skippedByBucketAndLevelFilter = mergedEntries.size() - files.size();
        files =
                files.stream()
                        .collect(
                                Collectors.groupingBy(
                                        // 使用 LinkedHashMap 避免无序
                                        file -> Pair.of(file.partition(), file.bucket()),
                                        LinkedHashMap::new,
                                        Collectors.toList()))
                        .values()
                        .stream()
                        .map(this::filterWholeBucketByStats)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());

        // 计算扫描统计数据
        long skippedByWholeBucketFiles = afterBucketFilter - files.size();
        long scanDuration = (System.nanoTime() - started) / 1_000_000;
        checkState(
                startDataFiles
                        - skippedByPartitionAndStats
                        - skippedByBucketAndLevelFilter
                        - skippedByWholeBucketFiles
                        == files.size());
        if (scanMetrics != null) {
            scanMetrics.reportScan(
                    new ScanStats(
                            scanDuration,
                            manifests.size(),
                            skippedByPartitionAndStats,
                            skippedByBucketAndLevelFilter,
                            skippedByWholeBucketFiles,
                            files.size()));
        }
        return Pair.of(snapshot, files);
    }

    /**
     * 合并并过滤文件条目。
     *
     * @param manifests      清单文件元数据列表
     * @param manifestReader 清单文件读取器
     * @param filterUnmergedEntry 未合并条目过滤器
     * @return 合并后的文件条目集合
     */
    public <T extends FileEntry> Collection<T> readAndMergeFileEntries(
            List<ManifestFileMeta> manifests,
            Function<ManifestFileMeta, List<T>> manifestReader,
            @Nullable Filter<T> filterUnmergedEntry) {
        // 在内存中过滤清单文件元数据
        manifests =
                manifests.stream()
                        .filter(this::filterManifestFileMeta)
                        .collect(Collectors.toList());
        Function<ManifestFileMeta, List<T>> reader =
                file -> {
                    List<T> entries = manifestReader.apply(file);
                    if (filterUnmergedEntry != null) {
                        entries =
                                entries.stream()
                                        .filter(filterUnmergedEntry::test)
                                        .collect(Collectors.toList());
                    }
                    return entries;
                };
        return FileEntry.mergeEntries(
                sequentialBatchedExecute(reader, manifests, scanManifestParallelism));
    }

    /**
     * 读取清单文件元数据。
     *
     * @return 快照和清单文件元数据列表
     */
    private Pair<Snapshot, List<ManifestFileMeta>> readManifests() {
        // 优先使用指定的清单文件元数据
        List<ManifestFileMeta> manifests = specifiedManifests;
        Snapshot snapshot = null;
        if (manifests == null) {
            // 获取指定快照或最新快照
            snapshot =
                    specifiedSnapshot == null
                            ? snapshotManager.latestSnapshot()
                            : specifiedSnapshot;
            if (snapshot == null) {
                manifests = Collections.emptyList();
            } else {
                manifests = readManifests(snapshot);
            }
        }
        return Pair.of(snapshot, manifests);
    }

    /**
     * 根据扫描模式读取清单文件。
     *
     * @param snapshot 快照对象
     * @return 清单文件元数据列表
     */
    private List<ManifestFileMeta> readManifests(Snapshot snapshot) {
        switch (scanMode) {
            case ALL:
                return manifestList.readDataManifests(snapshot);
            case DELTA:
                return manifestList.readDeltaManifests(snapshot);
            case CHANGELOG:
                if (snapshot.version() > Snapshot.TABLE_STORE_02_VERSION) {
                    return manifestList.readChangelogManifests(snapshot);
                }

                // 兼容 Paimon 0.2，读取限制删除的附加文件
                if (snapshot.commitKind() == Snapshot.CommitKind.APPEND) {
                    return manifestList.readDeltaManifests(snapshot);
                }
                throw new IllegalStateException(
                        String.format(
                                "Incremental scan does not accept %s snapshot",
                                snapshot.commitKind()));
            default:
                throw new UnsupportedOperationException("Unknown scan kind " + scanMode.name());
        }
    }

    // ------------------------------------------------------------------------
    // Start Thread Safe Methods: The following methods need to be thread safe because they will be
    // called by multiple threads
    // ------------------------------------------------------------------------

    /**
     * 根据表ID获取表模式。
     *
     * @param id 表ID
     * @return 表模式
     */
    protected TableSchema scanTableSchema(long id) {
        return tableSchemas.computeIfAbsent(
                id, key -> key == schema.id() ? schema : schemaManager.schema(id));
    }

    /**
     * 过滤清单文件元数据。
     *
     * @param manifest 清单文件元数据
     * @return 是否通过过滤
     */
    private boolean filterManifestFileMeta(ManifestFileMeta manifest) {
        if (partitionFilter == null) {
            return true;
        }

        SimpleStats stats = manifest.partitionStats();
        return partitionFilter.test(
                manifest.numAddedFiles() + manifest.numDeletedFiles(),
                stats.minValues(),
                stats.maxValues(),
                stats.nullCounts());
    }

    /**
     * 过滤未合并的清单条目。
     *
     * @param entry 清单条目
     * @return 是否通过过滤
     */
    private boolean filterUnmergedManifestEntry(ManifestEntry entry) {
        if (manifestEntryFilter != null && !manifestEntryFilter.test(entry)) {
            return false;
        }

        return filterByStats(entry);
    }

    /**
     * 根据统计信息过滤清单条目。
     *
     * @param entry 清单条目
     * @return 是否通过过滤
     */
    protected abstract boolean filterByStats(ManifestEntry entry);

    /**
     * 过滤合并后的清单条目。
     *
     * @param entry 清单条目
     * @return 是否通过过滤
     */
    private boolean filterMergedManifestEntry(ManifestEntry entry) {
        return (bucketFilter == null || bucketFilter.test(entry.bucket()))
                && bucketKeyFilter.select(entry.bucket(), entry.totalBuckets())
                && (levelFilter == null || levelFilter.test(entry.file().level()));
    }

    /**
     * 根据统计信息过滤整个桶中的条目。
     *
     * @param entries 清单条目列表
     * @return 过滤后的清单条目列表
     */
    protected abstract List<ManifestEntry> filterWholeBucketByStats(List<ManifestEntry> entries);

    /**
     * 读取清单文件元数据。
     *
     * @param manifest 清单文件元数据
     * @return 清单条目列表
     */
    private List<ManifestEntry> readManifestFileMeta(ManifestFileMeta manifest) {
        return manifestFileFactory
                .create()
                .read(
                        manifest.fileName(),
                        manifest.fileSize(),
                        createCacheRowFilter(manifestCacheFilter, numOfBuckets),
                        createEntryRowFilter(
                                partitionFilter, bucketFilter, fileNameFilter, numOfBuckets));
    }

    /**
     * 读取简单文件条目。
     *
     * @param manifest 清单文件元数据
     * @return 简单文件条目列表
     */
    private List<SimpleFileEntry> readSimpleEntries(ManifestFileMeta manifest) {
        return manifestFileFactory
                .createSimpleFileEntryReader()
                .read(
                        manifest.fileName(),
                        manifest.fileSize(),
                        // 当前过滤器条件未配置时，无需执行文件读取？
                        // 但调用方设置回调的方式，会被自身的`createEntryRowFilter`覆盖？
                        createCacheRowFilter(manifestCacheFilter, numOfBuckets),
                        createEntryRowFilter(
                                partitionFilter, bucketFilter, fileNameFilter, numOfBuckets));
    }

    /**
     * 根据清单缓存过滤器创建行过滤器。
     *
     * @param manifestCacheFilter 清单缓存过滤器
     * @param numOfBuckets        桶数量
     * @return 行过滤器
     */
    private static Filter<InternalRow> createCacheRowFilter(
            @Nullable ManifestCacheFilter manifestCacheFilter, int numOfBuckets) {
        if (manifestCacheFilter == null) {
            return Filter.alwaysTrue();
        }

        Function<InternalRow, BinaryRow> partitionGetter =
                ManifestEntrySerializer.partitionGetter();
        Function<InternalRow, Integer> bucketGetter = ManifestEntrySerializer.bucketGetter();
        Function<InternalRow, Integer> totalBucketGetter =
                ManifestEntrySerializer.totalBucketGetter();
        return row -> {
            if (numOfBuckets != totalBucketGetter.apply(row)) {
                return true;
            }

            return manifestCacheFilter.test(partitionGetter.apply(row), bucketGetter.apply(row));
        };
    }

    /**
     * 根据分区、桶和文件名过滤器创建行过滤器。
     *
     * @param partitionFilter    分区过滤器
     * @param bucketFilter       桶过滤器
     * @param fileNameFilter     文件名过滤器
     * @param numOfBuckets       桶数量
     * @return 行过滤器
     */
    private static Filter<InternalRow> createEntryRowFilter(
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Filter<Integer> bucketFilter,
            @Nullable Filter<String> fileNameFilter,
            int numOfBuckets) {
        Function<InternalRow, BinaryRow> partitionGetter =
                ManifestEntrySerializer.partitionGetter();
        Function<InternalRow, Integer> bucketGetter = ManifestEntrySerializer.bucketGetter();
        Function<InternalRow, Integer> totalBucketGetter =
                ManifestEntrySerializer.totalBucketGetter();
        Function<InternalRow, String> fileNameGetter = ManifestEntrySerializer.fileNameGetter();
        return row -> {
            if ((partitionFilter != null && !partitionFilter.test(partitionGetter.apply(row)))) {
                return false;
            }

            // 若当前桶数量与总桶数量一致，并且桶过滤器不为空，过滤当前桶
            // 合理吗？！！！ 是否有应用场景需要过滤？
            // 例如，表 100 个桶，此时增加到 200 个桶，是否需要过滤旧桶？
            // 或者，用户已经设置了基于分区和桶的过滤器？
            if (bucketFilter != null
                    && numOfBuckets == totalBucketGetter.apply(row)
                    && !bucketFilter.test(bucketGetter.apply(row))) {
                return false;
            }

            if (fileNameFilter != null && !fileNameFilter.test(fileNameGetter.apply(row))) {
                return false;
            }

            return true;
        };
    }

    // ------------------------------------------------------------------------
    // End Thread Safe Methods
    // ------------------------------------------------------------------------
}