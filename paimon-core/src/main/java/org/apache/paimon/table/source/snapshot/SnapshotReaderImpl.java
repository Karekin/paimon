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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.apache.paimon.Snapshot.FIRST_SNAPSHOT_ID;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.operation.FileStoreScan.Plan.groupByPartFiles;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** Implementation of {@link SnapshotReader}. */
public class SnapshotReaderImpl implements SnapshotReader {

    // FileStoreScan 对象，用于扫描文件存储
    private final FileStoreScan scan;
    // 表结构信息
    private final TableSchema tableSchema;
    // 核心配置选项
    private final CoreOptions options;
    // 是否启用删除向量
    private final boolean deletionVectors;
    // 快照管理器，用于管理快照操作
    private final SnapshotManager snapshotManager;
    // 消费者管理器，用于管理消费者
    private final ConsumerManager consumerManager;
    // 分裂生成器，用于生成数据分裂
    private final SplitGenerator splitGenerator;
    // 非分区过滤消费函数，用于处理非分区过滤条件
    private final BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer;
    // 默认值分配器，用于分配默认值
    private final DefaultValueAssigner defaultValueAssigner;
    // 文件存储路径工厂，用于生成文件路径
    private final FileStorePathFactory pathFactory;
    // 表名
    private final String tableName;
    // 索引文件处理器，用于处理索引文件
    private final IndexFileHandler indexFileHandler;

    // 扫描模式，默认为 ALL
    private ScanMode scanMode = ScanMode.ALL;
    // 懒加载分区比较器
    private RecordComparator lazyPartitionComparator;

    // 构造函数，初始化所有字段
    public SnapshotReaderImpl(
            FileStoreScan scan,
            TableSchema tableSchema,
            CoreOptions options,
            SnapshotManager snapshotManager,
            SplitGenerator splitGenerator,
            BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer,
            DefaultValueAssigner defaultValueAssigner,
            FileStorePathFactory pathFactory,
            String tableName,
            IndexFileHandler indexFileHandler) {
        this.scan = scan;
        this.tableSchema = tableSchema;
        this.options = options;
        // 是否启用删除向量，由配置选项决定
        this.deletionVectors = options.deletionVectorsEnabled();
        this.snapshotManager = snapshotManager;
        // 初始化消费者管理器
        this.consumerManager =
                new ConsumerManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());
        this.splitGenerator = splitGenerator;
        this.nonPartitionFilterConsumer = nonPartitionFilterConsumer;
        this.defaultValueAssigner = defaultValueAssigner;
        this.pathFactory = pathFactory;

        this.tableName = tableName;
        this.indexFileHandler = indexFileHandler;
    }

    // 获取快照管理器
    @Override
    public SnapshotManager snapshotManager() {
        return snapshotManager;
    }

    // 获取消费者管理器
    @Override
    public ConsumerManager consumerManager() {
        return consumerManager;
    }

    // 获取分裂生成器
    @Override
    public SplitGenerator splitGenerator() {
        return splitGenerator;
    }

    // 设置快照 ID
    @Override
    public SnapshotReader withSnapshot(long snapshotId) {
        scan.withSnapshot(snapshotId);
        return this;
    }

    // 设置快照对象
    @Override
    public SnapshotReader withSnapshot(Snapshot snapshot) {
        scan.withSnapshot(snapshot);
        return this;
    }

    // 设置分区过滤规则（通过分区规范）
    @Override
    public SnapshotReader withPartitionFilter(Map<String, String> partitionSpec) {
        if (partitionSpec != null) {
            // 创建分区断言
            Predicate partitionPredicate =
                    createPartitionPredicate(
                            partitionSpec,
                            tableSchema.logicalPartitionType(),
                            options.partitionDefaultName());
            scan.withPartitionFilter(partitionPredicate);
        }
        return this;
    }

    // 设置分区过滤规则（通过断言）
    @Override
    public SnapshotReader withPartitionFilter(Predicate predicate) {
        scan.withPartitionFilter(predicate);
        return this;
    }

    // 设置分区过滤规则（通过分区列表）
    @Override
    public SnapshotReader withPartitionFilter(List<BinaryRow> partitions) {
        scan.withPartitionFilter(partitions);
        return this;
    }

    // 设置数据文件过滤规则
    @Override
    public SnapshotReader withFilter(Predicate predicate) {
        List<String> partitionKeys = tableSchema.partitionKeys();
        // 获取字段在分区中的索引
        int[] fieldIdxToPartitionIdx =
                tableSchema.fields().stream()
                        .mapToInt(f -> partitionKeys.indexOf(f.name()))
                        .toArray();

        List<Predicate> partitionFilters = new ArrayList<>();
        List<Predicate> nonPartitionFilters = new ArrayList<>();
        // 分割 AND 语句，处理默认值分配后的断言
        for (Predicate p :
                PredicateBuilder.splitAnd(defaultValueAssigner.handlePredicate(predicate))) {
            // 转换字段映射
            Optional<Predicate> mapped = transformFieldMapping(p, fieldIdxToPartitionIdx);
            if (mapped.isPresent()) {
                partitionFilters.add(mapped.get());
            } else {
                nonPartitionFilters.add(p);
            }
        }

        // 应用分区过滤条件
        if (partitionFilters.size() > 0) {
            scan.withPartitionFilter(PredicateBuilder.and(partitionFilters));
        }

        // 应用非分区过滤条件
        if (nonPartitionFilters.size() > 0) {
            nonPartitionFilterConsumer.accept(scan, PredicateBuilder.and(nonPartitionFilters));
        }
        return this;
    }

    // 设置扫描模式
    @Override
    public SnapshotReader withMode(ScanMode scanMode) {
        this.scanMode = scanMode;
        scan.withKind(scanMode);
        return this;
    }

    // 设置层级过滤
    @Override
    public SnapshotReader withLevelFilter(Filter<Integer> levelFilter) {
        scan.withLevelFilter(levelFilter);
        return this;
    }

    // 设置清单条目过滤
    @Override
    public SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter) {
        scan.withManifestEntryFilter(filter);
        return this;
    }

    // 设置桶
    @Override
    public SnapshotReader withBucket(int bucket) {
        scan.withBucket(bucket);
        return this;
    }

    // 设置桶过滤
    @Override
    public SnapshotReader withBucketFilter(Filter<Integer> bucketFilter) {
        scan.withBucketFilter(bucketFilter);
        return this;
    }

    // 设置指标注册表
    @Override
    public SnapshotReader withMetricRegistry(MetricRegistry registry) {
        scan.withMetrics(new ScanMetrics(registry, tableName));
        return this;
    }

    // 设置数据文件名过滤规则
    @Override
    public SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter) {
        scan.withDataFileNameFilter(fileNameFilter);
        return this;
    }

    // 设置分片
    @Override
    public SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        if (splitGenerator.alwaysRawConvertible()) {
            // 基于文件名的分片过滤
            withDataFileNameFilter(
                    file ->
                            Math.abs(file.hashCode() % numberOfParallelSubtasks)
                                    == indexOfThisSubtask);
        } else {
            // 基于桶的分片过滤
            withBucketFilter(bucket -> bucket % numberOfParallelSubtasks == indexOfThisSubtask);
        }
        return this;
    }

    // 根据 ADD 文件获取数据拆分
    @Override
    public Plan read() {
        FileStoreScan.Plan plan = scan.plan();
        @Nullable Snapshot snapshot = plan.snapshot();

        // 按分区和桶对文件进行分组
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> files =
                groupByPartFiles(plan.files(FileKind.ADD));
        if (options.scanPlanSortPartition()) {
            // 对分区进行排序
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> newFiles = new LinkedHashMap<>();
            files.entrySet().stream()
                    .sorted((o1, o2) -> partitionComparator().compare(o1.getKey(), o2.getKey()))
                    .forEach(entry -> newFiles.put(entry.getKey(), entry.getValue()));
            files = newFiles;
        }
        // 生成拆分
        List<DataSplit> splits =
                generateSplits(snapshot, scanMode != ScanMode.ALL, splitGenerator, files);
        return new PlanImpl(
                plan.watermark(), snapshot == null ? null : snapshot.id(), (List) splits);
    }

    // 生成数据拆分
    private List<DataSplit> generateSplits(
            @Nullable Snapshot snapshot,
            boolean isStreaming,
            SplitGenerator splitGenerator,
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> groupedDataFiles) {
        List<DataSplit> splits = new ArrayList<>();
        // 读取删除索引以减少文件 IO
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> deletionIndexFilesMap =
                deletionVectors && snapshot != null
                        ? indexFileHandler.scan(
                        snapshot, DELETION_VECTORS_INDEX, groupedDataFiles.keySet())
                        : Collections.emptyMap();
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> entry :
                groupedDataFiles.entrySet()) {
            BinaryRow partition = entry.getKey();
            Map<Integer, List<DataFileMeta>> buckets = entry.getValue();
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry : buckets.entrySet()) {
                int bucket = bucketEntry.getKey();
                List<DataFileMeta> bucketFiles = bucketEntry.getValue();
                // 构建数据分片
                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(
                                        snapshot == null ? FIRST_SNAPSHOT_ID - 1 : snapshot.id())
                                .withPartition(partition)
                                .withBucket(bucket)
                                .isStreaming(isStreaming);
                // 生成分裂组
                List<SplitGenerator.SplitGroup> splitGroups =
                        isStreaming
                                ? splitGenerator.splitForStreaming(bucketFiles)
                                : splitGenerator.splitForBatch(bucketFiles);
                // 获取删除索引文件
                List<IndexFileMeta> deletionIndexFiles =
                        deletionIndexFilesMap.getOrDefault(
                                Pair.of(partition, bucket), Collections.emptyList());
                for (SplitGenerator.SplitGroup splitGroup : splitGroups) {
                    List<DataFileMeta> dataFiles = splitGroup.files;
                    // 构建分片路径
                    String bucketPath = pathFactory.bucketPath(partition, bucket).toString();
                    builder.withDataFiles(dataFiles)
                            .rawConvertible(splitGroup.rawConvertible)
                            .withBucketPath(bucketPath);
                    if (deletionVectors) {
                        // 添加删除文件信息
                        builder.withDataDeletionFiles(
                                getDeletionFiles(dataFiles, deletionIndexFiles));
                    }

                    splits.add(builder.build());
                }
            }
        }
        return splits;
    }

    // 获取分区列表
    @Override
    public List<BinaryRow> partitions() {
        return scan.listPartitions();
    }

    // 获取分区条目
    @Override
    public List<PartitionEntry> partitionEntries() {
        return scan.readPartitionEntries();
    }

    // 读取变化
    @Override
    public Plan readChanges() {
        withMode(ScanMode.DELTA);
        FileStoreScan.Plan plan = scan.plan();

        // 对删除文件和添加文件进行分组
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> beforeFiles =
                groupByPartFiles(plan.files(FileKind.DELETE));
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> dataFiles =
                groupByPartFiles(plan.files(FileKind.ADD));

        return toChangesPlan(true, plan, plan.snapshot().id() - 1, beforeFiles, dataFiles);
    }

    // 转换为变化计划
    private Plan toChangesPlan(
            boolean isStreaming,
            FileStoreScan.Plan plan,
            long beforeSnapshotId,
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> beforeFiles,
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> dataFiles) {
        Snapshot snapshot = plan.snapshot();
        List<DataSplit> splits = new ArrayList<>();
        // 合并分区和桶信息
        Map<BinaryRow, Set<Integer>> buckets = new HashMap<>();
        beforeFiles.forEach(
                (part, bucketMap) ->
                        buckets.computeIfAbsent(part, k -> new HashSet<>())
                                .addAll(bucketMap.keySet()));
        dataFiles.forEach(
                (part, bucketMap) ->
                        buckets.computeIfAbsent(part, k -> new HashSet<>())
                                .addAll(bucketMap.keySet()));
        // 读取删除索引以减少文件 IO
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> beforDeletionIndexFilesMap =
                deletionVectors
                        ? indexFileHandler.scan(
                        beforeSnapshotId, DELETION_VECTORS_INDEX, beforeFiles.keySet())
                        : Collections.emptyMap();
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> deletionIndexFilesMap =
                deletionVectors
                        ? indexFileHandler.scan(
                        snapshot, DELETION_VECTORS_INDEX, dataFiles.keySet())
                        : Collections.emptyMap();

        for (Map.Entry<BinaryRow, Set<Integer>> entry : buckets.entrySet()) {
            BinaryRow part = entry.getKey();
            for (Integer bucket : entry.getValue()) {
                // 获取删除文件和数据文件
                List<DataFileMeta> before =
                        beforeFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());
                List<DataFileMeta> data =
                        dataFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());

                // 去重
                before.removeIf(data::remove);

                // 构建数据分片
                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(snapshot.id())
                                .withPartition(part)
                                .withBucket(bucket)
                                .withBeforeFiles(before)
                                .withDataFiles(data)
                                .isStreaming(isStreaming)
                                .withBucketPath(pathFactory.bucketPath(part, bucket).toString());
                if (deletionVectors) {
                    // 添加删除文件信息
                    builder.withBeforeDeletionFiles(
                            getDeletionFiles(
                                    before,
                                    beforDeletionIndexFilesMap.getOrDefault(
                                            Pair.of(part, bucket), Collections.emptyList())));
                    builder.withDataDeletionFiles(
                            getDeletionFiles(
                                    data,
                                    deletionIndexFilesMap.getOrDefault(
                                            Pair.of(part, bucket), Collections.emptyList())));
                }
                splits.add(builder.build());
            }
        }

        return new PlanImpl(
                plan.watermark(), snapshot == null ? null : snapshot.id(), (List) splits);
    }

    // 读取增量差异
    @Override
    public Plan readIncrementalDiff(Snapshot before) {
        withMode(ScanMode.ALL);
        FileStoreScan.Plan plan = scan.plan();
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> dataFiles =
                groupByPartFiles(plan.files(FileKind.ADD));
        // 获取之前的快照文件
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> beforeFiles =
                groupByPartFiles(scan.withSnapshot(before).plan().files(FileKind.ADD));
        return toChangesPlan(false, plan, before.id(), beforeFiles, dataFiles);
    }

    // 获取分区比较器
    private RecordComparator partitionComparator() {
        if (lazyPartitionComparator == null) {
            lazyPartitionComparator =
                    CodeGenUtils.newRecordComparator(
                            tableSchema.logicalPartitionType().getFieldTypes());
        }
        return lazyPartitionComparator;
    }

    // 获取删除文件
    private List<DeletionFile> getDeletionFiles(
            List<DataFileMeta> dataFiles, List<IndexFileMeta> indexFileMetas) {
        List<DeletionFile> deletionFiles = new ArrayList<>(dataFiles.size());
        // 建立数据文件到索引文件的映射
        Map<String, IndexFileMeta> dataFileToIndexFileMeta = new HashMap<>();
        for (IndexFileMeta indexFileMeta : indexFileMetas) {
            if (indexFileMeta.deletionVectorsRanges() != null) {
                for (String dataFileName : indexFileMeta.deletionVectorsRanges().keySet()) {
                    dataFileToIndexFileMeta.put(dataFileName, indexFileMeta);
                }
            }
        }
        for (DataFileMeta file : dataFiles) {
            IndexFileMeta indexFileMeta = dataFileToIndexFileMeta.get(file.fileName());
            if (indexFileMeta != null) {
                // 获取删除范围
                Map<String, Pair<Integer, Integer>> ranges = indexFileMeta.deletionVectorsRanges();
                if (ranges != null && ranges.containsKey(file.fileName())) {
                    Pair<Integer, Integer> range = ranges.get(file.fileName());
                    deletionFiles.add(
                            new DeletionFile(
                                    indexFileHandler.filePath(indexFileMeta).toString(),
                                    range.getKey(),
                                    range.getValue()));
                    continue;
                }
            }
            deletionFiles.add(null);
        }

        return deletionFiles;
    }
}
