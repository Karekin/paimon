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
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.source.snapshot.CompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromSnapshotFullStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromSnapshotStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromTimestampStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousLatestStartingScanner;
import org.apache.paimon.table.source.snapshot.FileCreationTimeStartingScanner;
import org.apache.paimon.table.source.snapshot.FullCompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.FullStartingScanner;
import org.apache.paimon.table.source.snapshot.IncrementalStartingScanner;
import org.apache.paimon.table.source.snapshot.IncrementalTagStartingScanner;
import org.apache.paimon.table.source.snapshot.IncrementalTimeStampStartingScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromSnapshotStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromTagStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromTimestampStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromWatermarkStartingScanner;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
/**
 * 文件存储扫描的抽象层，用于提供输入分片生成功能。
 * <p>
 * 该抽象类实现了 DataTableScan 接口，其核心功能在子类中实现。
 */
public abstract class AbstractDataTableScan implements DataTableScan {

    /** 核心选项配置，包含扫描相关的各种设置 */
    private final CoreOptions options;

    /** 快照读取器，用于从文件存储中读取快照数据 */
    protected final SnapshotReader snapshotReader;

    /**
     * 构造函数，初始化 AbstractDataTableScan 对象。
     *
     * @param options 核心选项配置
     * @param snapshotReader 快照读取器
     */
    protected AbstractDataTableScan(CoreOptions options, SnapshotReader snapshotReader) {
        this.options = options;
        this.snapshotReader = snapshotReader;
    }

    /**
     * 设置桶，用于过滤特定的桶数据。
     * <p>
     * 该方法会将指定的桶设置到快照读取器中，并返回当前对象，支持链式调用。
     *
     * @param bucket 桶的编号
     * @return 当前对象实例
     */
    @VisibleForTesting
    public AbstractDataTableScan withBucket(int bucket) {
        snapshotReader.withBucket(bucket); // 设置快照读取器的桶
        return this; // 支持链式调用
    }

    /**
     * 设置桶过滤器，用于过滤特定的桶数据。
     * <p>
     * 该方法会将指定的桶过滤器应用到快照读取器中，并返回当前对象，支持链式调用。
     *
     * @param bucketFilter 桶过滤器
     * @return 当前对象实例
     */
    @Override
    public AbstractDataTableScan withBucketFilter(Filter<Integer> bucketFilter) {
        snapshotReader.withBucketFilter(bucketFilter); // 设置快照读取器的桶过滤器
        return this; // 支持链式调用
    }

    /**
     * 设置分区过滤器，用于过滤特定的分区数据。
     * <p>
     * 该方法会将指定的分区过滤器应用到快照读取器中，并返回当前对象，支持链式调用。
     *
     * @param partitionSpec 分区规范
     * @return 当前对象实例
     */
    @Override
    public AbstractDataTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        snapshotReader.withPartitionFilter(partitionSpec); // 设置快照读取器的分区过滤器
        return this; // 支持链式调用
    }

    /**
     * 设置分区过滤器，用于过滤特定的分区数据。
     * <p>
     * 该方法会将指定的分区列表应用到快照读取器中，并返回当前对象，支持链式调用。
     *
     * @param partitions 分区列表
     * @return 当前对象实例
     */
    @Override
    public AbstractDataTableScan withPartitionFilter(List<BinaryRow> partitions) {
        snapshotReader.withPartitionFilter(partitions); // 设置快照读取器的分区过滤器
        return this; // 支持链式调用
    }

    /**
     * 设置级别过滤器，用于过滤特定级别的数据。
     * <p>
     * 该方法会将指定的级别过滤器应用到快照读取器中，并返回当前对象，支持链式调用。
     *
     * @param levelFilter 级别过滤器
     * @return 当前对象实例
     */
    @Override
    public AbstractDataTableScan withLevelFilter(Filter<Integer> levelFilter) {
        snapshotReader.withLevelFilter(levelFilter); // 设置快照读取器的级别过滤器
        return this; // 支持链式调用
    }

    /**
     * 设置指标注册表，用于监控和记录指标。
     * <p>
     * 该方法会将指定的指标注册表应用到快照读取器中，并返回当前对象，支持链式调用。
     *
     * @param metricsRegistry 指标注册表
     * @return 当前对象实例
     */
    public AbstractDataTableScan withMetricsRegistry(MetricRegistry metricsRegistry) {
        snapshotReader.withMetricRegistry(metricsRegistry); // 设置快照读取器的指标注册表
        return this; // 支持链式调用
    }

    /**
     * 获取核心选项配置。
     *
     * @return 核心选项配置
     */
    public CoreOptions options() {
        return options; // 返回核心选项配置
    }

    /**
     * 创建一个起始扫描器，根据配置的启动模式和流式/批处理模式。
     *
     * @param isStreaming 是否为流式扫描
     * @return 起始扫描器实例
     */
    protected StartingScanner createStartingScanner(boolean isStreaming) {
        SnapshotManager snapshotManager = snapshotReader.snapshotManager(); // 获取快照管理器
        CoreOptions.StreamScanMode type = options.toConfiguration().get(CoreOptions.STREAM_SCAN_MODE); // 获取流式扫描模式

        // 根据不同的流式扫描模式创建不同的起始扫描器
        switch (type) {
            case COMPACT_BUCKET_TABLE:
                checkArgument(isStreaming, "在批处理模式下设置 'streaming-compact'，这是意料之外的。");
                return new ContinuousCompactorStartingScanner(snapshotManager); // 返回连续压缩起始扫描器
            case FILE_MONITOR:
                return new FullStartingScanner(snapshotManager); // 返回全量起始扫描器
        }

        // 根据消费者ID读取数据
        String consumerId = options.consumerId();
        if (isStreaming && consumerId != null && !options.consumerIgnoreProgress()) {
            ConsumerManager consumerManager = snapshotReader.consumerManager(); // 获取消费者管理器
            Optional<Consumer> consumer = consumerManager.consumer(consumerId); // 获取消费者
            if (consumer.isPresent()) { // 如果消费者存在
                return new ContinuousFromSnapshotStartingScanner(
                        snapshotManager,
                        consumer.get().nextSnapshot(), // 消费者的下一个快照
                        options.changelogLifecycleDecoupled()); // 是否解耦变更日志生命周期
            }
        }

        CoreOptions.StartupMode startupMode = options.startupMode(); // 获取启动模式
        // 根据不同的启动模式创建不同的起始扫描器
        switch (startupMode) {
            case LATEST_FULL:
                return new FullStartingScanner(snapshotManager); // 返回全量起始扫描器
            case LATEST:
                return isStreaming
                        ? new ContinuousLatestStartingScanner(snapshotManager) // 流式扫描：连续最新起始扫描器
                        : new FullStartingScanner(snapshotManager); // 批处理扫描：全量起始扫描器
            case COMPACTED_FULL:
                if (options.changelogProducer() == ChangelogProducer.FULL_COMPACTION
                        || options.toConfiguration().contains(FULL_COMPACTION_DELTA_COMMITS)) {
                    int deltaCommits = options.toConfiguration().getOptional(FULL_COMPACTION_DELTA_COMMITS).orElse(1); // 获取增量提交数
                    return new FullCompactedStartingScanner(snapshotManager, deltaCommits); // 返回全量压缩起始扫描器
                } else {
                    return new CompactedStartingScanner(snapshotManager); // 返回压缩起始扫描器
                }
            case FROM_TIMESTAMP:
                Long startupMillis = options.scanTimestampMills(); // 获取启动时间戳
                return isStreaming
                        ? new ContinuousFromTimestampStartingScanner(
                        snapshotManager,
                        startupMillis, // 时间戳
                        options.changelogLifecycleDecoupled()) // 是否解耦变更日志生命周期
                        : new StaticFromTimestampStartingScanner(snapshotManager, startupMillis); // 静态时间戳起始扫描器
            case FROM_FILE_CREATION_TIME:
                Long fileCreationTimeMills = options.scanFileCreationTimeMills(); // 获取文件创建时间戳
                return new FileCreationTimeStartingScanner(snapshotManager, fileCreationTimeMills); // 返回文件创建时间起始扫描器
            case FROM_SNAPSHOT:
                if (options.scanSnapshotId() != null) { // 如果存在快照ID
                    return isStreaming
                            ? new ContinuousFromSnapshotStartingScanner(
                            snapshotManager,
                            options.scanSnapshotId(), // 快照ID
                            options.changelogLifecycleDecoupled()) // 是否解耦变更日志生命周期
                            : new StaticFromSnapshotStartingScanner(snapshotManager, options.scanSnapshotId()); // 静态快照起始扫描器
                } else if (options.scanWatermark() != null) { // 如果存在水印
                    checkArgument(!isStreaming, "在流式模式下无法从水印进行扫描。");
                    return new StaticFromWatermarkStartingScanner(snapshotManager, options().scanWatermark()); // 返回静态水印起始扫描器
                } else if (options.scanTagName() != null) { // 如果存在标签名
                    checkArgument(!isStreaming, "在流式模式下无法从标签进行扫描。");
                    return new StaticFromTagStartingScanner(snapshotManager, options().scanTagName()); // 返回静态标签起始扫描器
                } else {
                    throw new UnsupportedOperationException("未知的快照读取模式"); // 抛出不支持的操作异常
                }
            case FROM_SNAPSHOT_FULL:
                Long scanSnapshotId = options.scanSnapshotId();
                checkNotNull(scanSnapshotId, "在启动模式为 FROM_SNAPSHOT_FULL 时，必须设置 scan.snapshot-id。");
                return isStreaming
                        ? new ContinuousFromSnapshotFullStartingScanner(snapshotManager, scanSnapshotId) // 连续全量快照起始扫描器
                        : new StaticFromSnapshotStartingScanner(snapshotManager, scanSnapshotId); // 静态全量快照起始扫描器
            case INCREMENTAL:
                checkArgument(!isStreaming, "在流式模式下无法进行增量扫描。");
                Pair<String, String> incrementalBetween = options.incrementalBetween(); // 获取增量范围
                CoreOptions.IncrementalBetweenScanMode scanType = options.incrementalBetweenScanMode(); // 获取增量扫描模式
                ScanMode scanMode;
                switch (scanType) {
                    case AUTO:
                        scanMode = options.changelogProducer() == ChangelogProducer.NONE ? ScanMode.DELTA : ScanMode.CHANGELOG; // 自动选择扫描模式
                        break;
                    case DELTA:
                        scanMode = ScanMode.DELTA; // 增量扫描模式
                        break;
                    case CHANGELOG:
                        scanMode = ScanMode.CHANGELOG; // 变更日志扫描模式
                        break;
                    default:
                        throw new UnsupportedOperationException("不支持的增量扫描类型 " + scanType.name());
                }
                if (options.toMap().get(CoreOptions.INCREMENTAL_BETWEEN.key()) != null) {
                    try {
                        return new IncrementalStartingScanner(
                                snapshotManager,
                                Long.parseLong(incrementalBetween.getLeft()), // 起始时间戳
                                Long.parseLong(incrementalBetween.getRight()), // 结束时间戳
                                scanMode); // 返回增量起始扫描器
                    } catch (NumberFormatException e) {
                        return new IncrementalTagStartingScanner(
                                snapshotManager,
                                incrementalBetween.getLeft(), // 起始标签
                                incrementalBetween.getRight()); // 结束标签
                    }
                } else {
                    return new IncrementalTimeStampStartingScanner(
                            snapshotManager,
                            Long.parseLong(incrementalBetween.getLeft()), // 起始时间戳
                            Long.parseLong(incrementalBetween.getRight()), // 结束时间戳
                            scanMode); // 返回增量时间戳起始扫描器
                }
            default:
                throw new UnsupportedOperationException("未知的启动模式 " + startupMode.name());
        }
    }

    /**
     * 列出分区条目。
     *
     * @return 分区条目列表
     */
    @Override
    public List<PartitionEntry> listPartitionEntries() {
        return snapshotReader.partitionEntries(); // 返回分区条目列表
    }
}
