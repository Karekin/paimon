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

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.StreamingReadMode;
import org.apache.paimon.annotation.Documentation.ExcludeFromDocumentation;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.Description;
import org.apache.paimon.options.description.InlineElement;
import org.apache.paimon.options.description.TextElement;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.CoreOptions.STREAMING_READ_MODE;
import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.options.description.TextElement.text;

/**
 * Flink 连接器的配置选项。
 * <p>该类定义了 Flink 连接器的各种配置选项，用于控制数据源和数据写入的行为。</p>
 */
public class FlinkConnectorOptions {

    public static final String NONE = "none"; // 无日志系统的值
    public static final String TABLE_DYNAMIC_OPTION_PREFIX = "paimon"; // 动态选项的前缀
    public static final int MIN_CLUSTERING_SAMPLE_FACTOR = 20; // 聚簇分区的最小样本因子

    /**
     * 日志系统配置选项。
     * <p>定义了日志系统的选择和相关参数。</p>
     */
    @ExcludeFromDocumentation("Confused without log system")
    public static final ConfigOption<String> LOG_SYSTEM =
            ConfigOptions.key("log.system") // 配置键
                    .stringType() // 配置值的类型为字符串
                    .defaultValue(NONE) // 默认值为 "none"
                    .withDescription(
                            Description.builder()
                                    .text("使用的日志系统用于记录表的变更。")
                                    .linebreak()
                                    .linebreak()
                                    .text("可能的值:")
                                    .linebreak()
                                    .list(
                                            TextElement.text(
                                                    "\"none\": 不使用日志系统，数据仅写入文件存储中，流式读取将直接从文件存储中读取。"))
                                    .list(
                                            TextElement.text(
                                                    "\"kafka\": Kafka 日志系统，数据将同时写入文件存储和 Kafka，流式读取将从 Kafka 中读取。如果要从文件存储中进行流式读取，请将 "
                                                            + STREAMING_READ_MODE.key()
                                                            + " 设置为 "
                                                            + StreamingReadMode.FILE.getValue()
                                                            + "。"))
                                    .build());

    /**
     * 日志系统的分区数量配置选项。
     * <p>定义了日志系统（如 Kafka）的分区数量。</p>
     */
    @ExcludeFromDocumentation("Confused without log system")
    public static final ConfigOption<Integer> LOG_SYSTEM_PARTITIONS =
            ConfigOptions.key("log.system.partitions") // 配置键
                    .intType() // 配置值的类型为整数
                    .defaultValue(1) // 默认值为 1
                    .withDescription(
                            "日志系统的分区数量。如果日志系统是 Kafka，这将是 Kafka 的分区数。");

    /**
     * 日志系统的副本数量配置选项。
     * <p>定义了日志系统（如 Kafka）的副本数量。</p>
     */
    @ExcludeFromDocumentation("Confused without log system")
    public static final ConfigOption<Integer> LOG_SYSTEM_REPLICATION =
            ConfigOptions.key("log.system.replication") // 配置键
                    .intType() // 配置值的类型为整数
                    .defaultValue(1) // 默认值为 1
                    .withDescription(
                            "日志系统的副本数量。如果日志系统是 Kafka，这将是 Kafka 的 replicationFactor。");

    /**
     * Sink 的并行度配置选项。
     * <p>定义了 Sink 的并行度。</p>
     */
    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism") // 配置键
                    .intType() // 配置值的类型为整数
                    .noDefaultValue() // 无默认值
                    .withDescription(
                            "自定义 Sink 的并行度。默认情况下，如果未定义此选项，规划器将根据全局配置为每个语句单独推导并行度。");

    /**
     * Scan 的并行度配置选项。
     * <p>定义了 Scan 的并行度。</p>
     */
    public static final ConfigOption<Integer> SCAN_PARALLELISM =
            ConfigOptions.key("scan.parallelism") // 配置键
                    .intType() // 配置值的类型为整数
                    .noDefaultValue() // 无默认值
                    .withDescription(
                            "自定义 Scan 源的并行度。默认情况下，如果未定义此选项，规划器将根据全局配置为每个语句单独推导并行度。如果启用了 scan.infer-parallelism，规划器将根据推断的并行度来推导并行度。");

    /**
     * 无感知桶的压缩作业的并行度配置选项。
     * <p>定义了无感知桶的表压缩作业的并行度。</p>
     */
    public static final ConfigOption<Integer> UNAWARE_BUCKET_COMPACTION_PARALLELISM =
            ConfigOptions.key("unaware-bucket.compaction.parallelism") // 配置键
                    .intType() // 配置值的类型为整数
                    .noDefaultValue() // 无默认值
                    .withDescription(
                            "自定义无感知桶的表压缩作业的并行度。默认情况下，如果未定义此选项，规划器将根据全局配置为每个语句单独推导并行度。");

    /**
     * 是否推断 Scan 的并行度配置选项。
     * <p>定义了是否根据分片数量或桶数量推断 Scan 的并行度。</p>
     */
    public static final ConfigOption<Boolean> INFER_SCAN_PARALLELISM =
            ConfigOptions.key("scan.infer-parallelism") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(true) // 默认值为 true
                    .withDescription(
                            "如果为 false，源的并行度将由全局并行度设置。否则，源的并行度将根据分片数量（批处理模式）或桶数量（流式模式）推断。");

    /**
     * 推断 Scan 的最大并行度配置选项。
     * <p>定义了当推断 Scan 的并行度时的最大并行度限制。</p>
     */
    public static final ConfigOption<Integer> INFER_SCAN_MAX_PARALLELISM =
            ConfigOptions.key("scan.infer-parallelism.max") // 配置键
                    .intType() // 配置值的类型为整数
                    .defaultValue(1024) // 默认值为 1024
                    .withDescription(
                            "如果 scan.infer-parallelism 设置为 true，则通过此选项限制源的并行度。");

    /**
     * 更改日志生产者全量压缩的触发间隔配置选项（已废弃）。
     * <p>定义了更改日志生产者全量压缩的触发间隔。</p>
     */
    @Deprecated
    @ExcludeFromDocumentation("Deprecated")
    public static final ConfigOption<Duration> CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL =
            key("changelog-producer.compaction-interval") // 配置键
                    .durationType() // 配置值的类型为 Duration
                    .defaultValue(Duration.ofSeconds(0)) // 默认值为 0 秒
                    .withDescription(
                            "当 "
                                    + CoreOptions.CHANGELOG_PRODUCER.key()
                                    + " 设置为 "
                                    + ChangelogProducer.FULL_COMPACTION.name()
                                    + " 时，每隔此间隔将触发全量压缩。");

    /**
     * Scan 的 Watermark 发射策略配置选项。
     * <p>定义了 Watermark 的发射策略。</p>
     */
    public static final ConfigOption<WatermarkEmitStrategy> SCAN_WATERMARK_EMIT_STRATEGY =
            key("scan.watermark.emit.strategy") // 配置键
                    .enumType(WatermarkEmitStrategy.class) // 配置值的类型为枚举
                    .defaultValue(WatermarkEmitStrategy.ON_EVENT) // 默认值为 ON_EVENT
                    .withDescription("Watermark 的生成策略。");

    /**
     * Scan 的 Watermark 对齐组配置选项。
     * <p>定义了 Watermark 的对齐组。</p>
     */
    public static final ConfigOption<String> SCAN_WATERMARK_ALIGNMENT_GROUP =
            key("scan.watermark.alignment.group") // 配置键
                    .stringType() // 配置值的类型为字符串
                    .noDefaultValue() // 无默认值
                    .withDescription("一组用于对齐 Watermark 的源。");

    /**
     * Scan 的 Watermark 空闲超时配置选项。
     * <p>定义了 Partition 空闲时被视为“空闲”并停止阻止下游操作员的 Watermark 进展的时间。</p>
     */
    public static final ConfigOption<Duration> SCAN_WATERMARK_IDLE_TIMEOUT =
            key("scan.watermark.idle-timeout") // 配置键
                    .durationType() // 配置值的类型为 Duration
                    .noDefaultValue() // 无默认值
                    .withDescription(
                            "如果一个流的 Partition 在此时间内没有记录流动，则认为该 Partition 是“空闲”，并且不会阻止下游操作员的 Watermark 进展。");

    /**
     * Scan 的 Watermark 对齐最大漂移配置选项。
     * <p>定义了 Watermark 对齐的最大漂移时间。</p>
     */
    public static final ConfigOption<Duration> SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT =
            key("scan.watermark.alignment.max-drift") // 配置键
                    .durationType() // 配置值的类型为 Duration
                    .noDefaultValue() // 无默认值
                    .withDescription(
                            "在暂停从源/任务/Partition 消费之前，对齐 Watermark 的最大漂移时间。");

    /**
     * Scan 的 Watermark 对齐更新间隔配置选项。
     * <p>定义了任务通知协调器当前 Watermark 以及协调器宣布最大对齐 Watermark 的频率。</p>
     */
    public static final ConfigOption<Duration> SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL =
            key("scan.watermark.alignment.update-interval") // 配置键
                    .durationType() // 配置值的类型为 Duration
                    .defaultValue(Duration.ofSeconds(1)) // 默认值为 1 秒
                    .withDescription(
                            "任务向协调器报告当前 Watermark 以及协调器宣布最大对齐 Watermark 的频率。");

    /**
     * Scan 的分片枚举器批量大小配置选项。
     * <p>定义了 StaticFileStoreSplitEnumerator 中每批分配给子任务的分片数量。</p>
     */
    public static final ConfigOption<Integer> SCAN_SPLIT_ENUMERATOR_BATCH_SIZE =
            key("scan.split-enumerator.batch-size") // 配置键
                    .intType() // 配置值的类型为整数
                    .defaultValue(10) // 默认值为 10
                    .withDescription(
                            "StaticFileStoreSplitEnumerator 中每批分配给子任务的分片数量，以避免超出 akka.framesize 的限制。");

    /**
     * Scan 的分片枚举器分配模式配置选项。
     * <p>定义了 StaticFileStoreSplitEnumerator 中分片的分配模式。</p>
     */
    public static final ConfigOption<SplitAssignMode> SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE =
            key("scan.split-enumerator.mode") // 配置键
                    .enumType(SplitAssignMode.class) // 配置值的类型为枚举
                    .defaultValue(SplitAssignMode.FAIR) // 默认值为 FAIR
                    .withDescription(
                            "StaticFileStoreSplitEnumerator 分配分片的模式。");

    /**
     * Sink 是否使用托管内存配置选项。
     * <p>定义了 Sink 是否使用托管内存。</p>
     */
    public static final ConfigOption<Boolean> SINK_USE_MANAGED_MEMORY =
            ConfigOptions.key("sink.use-managed-memory-allocator") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(false) // 默认值为 false
                    .withDescription(
                            "如果为 true，Flink Sink 将使用托管内存用于合并树；否则，将创建一个独立的内存分配器。");

    /**
     * Scan 是否强制移除 normalize 节点配置选项。
     * <p>定义了在流式读取时是否强制移除 normalize 节点。</p>
     */
    public static final ConfigOption<Boolean> SCAN_REMOVE_NORMALIZE =
            key("scan.remove-normalize") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(false) // 默认值为 false
                    .withFallbackKeys("log.scan.remove-normalize") // 回退键
                    .withDescription(
                            "在流式读取时是否强制移除 normalize 节点。注意： 如果下游用于计算聚合且输入不完整，这可能会导致数据错误。");

    /**
     * 流式读取时是否按照分区和桶进行混洗配置选项。
     * <p>定义了流式读取时是否按照分区和桶进行混洗。</p>
     */
    public static final ConfigOption<Boolean> STREAMING_READ_SHUFFLE_BUCKET_WITH_PARTITION =
            key("streaming-read.shuffle-bucket-with-partition") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(true) // 默认值为 true
                    .withDescription(
                            "在流式读取时是否按照分区和桶进行混洗。");

    /**
     * 用于合并树的托管内存的权重配置选项。
     * <p>定义了用于合并树的托管内存的权重。</p>
     */
    public static final ConfigOption<MemorySize> SINK_MANAGED_WRITER_BUFFER_MEMORY =
            ConfigOptions.key("sink.managed.writer-buffer-memory") // 配置键
                    .memoryType() // 配置值的类型为 MemorySize
                    .defaultValue(MemorySize.ofMebiBytes(256)) // 默认值为 256 MiB
                    .withDescription(
                            "用于写入缓冲区的托管内存的权重。Flink 将根据此权重计算写入器的内存大小，实际使用的内存取决于运行环境。");

    /**
     * 跨分区更新时 RockDB 的托管内存配置选项。
     * <p>定义了跨分区更新时 RockDB 的托管内存权重。</p>
     */
    public static final ConfigOption<MemorySize> SINK_CROSS_PARTITION_MANAGED_MEMORY =
            ConfigOptions.key("sink.cross-partition.managed-memory") // 配置键
                    .memoryType() // 配置值的类型为 MemorySize
                    .defaultValue(MemorySize.ofMebiBytes(256)) // 默认值为 256 MiB
                    .withDescription(
                            "用于 RockDB 的托管内存的权重。Flink 将根据此权重计算内存大小，实际使用的内存取决于运行环境。");

    /**
     * 是否将投影、过滤和限制下推到源中配置选项。
     * <p>定义了是否将投影、过滤和限制下推到源中。</p>
     */
    public static final ConfigOption<Boolean> SCAN_PUSH_DOWN =
            ConfigOptions.key("scan.push-down") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(true) // 默认值为 true
                    .withDescription(
                            "如果为 true，Flink 将把投影、过滤和限制下推到源中。这样做的代价是很难在作业中重用源。从 Flink 1.18 或更高版本开始，即使启用了投影下推，也可以重用源。");

    /**
     * 是否将 Flink 检查点与 Paimon 表的快照对齐配置选项。
     * <p>定义了是否将 Flink 检查点与 Paimon 表的快照对齐。</p>
     */
    public static final ConfigOption<Boolean> SOURCE_CHECKPOINT_ALIGN_ENABLED =
            ConfigOptions.key("source.checkpoint-align.enabled") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(false) // 默认值为 false
                    .withDescription(
                            "是否将 Flink 检查点与 Paimon 表的快照对齐。如果为 true，只有当快照被消费时，才会生成检查点。");

    /**
     * 对齐超时时间配置选项。
     * <p>定义了检查点对齐的超时时间。</p>
     */
    public static final ConfigOption<Duration> SOURCE_CHECKPOINT_ALIGN_TIMEOUT =
            ConfigOptions.key("source.checkpoint-align.timeout") // 配置键
                    .durationType() // 配置值的类型为 Duration
                    .defaultValue(Duration.ofSeconds(30)) // 默认值为 30 秒
                    .withDescription(
                            "如果在开始触发检查点时没有生成新的快照，则枚举器将阻塞检查点并等待新的快照。设置最大等待时间以避免无限等待，如果超时，检查点将失败。注意，此配置应小于检查点超时时间。");

    /**
     * 是否启用异步 lookup join 配置选项。
     * <p>定义了是否启用异步 lookup join。</p>
     */
    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(false) // 默认值为 false
                    .withDescription("是否启用异步 lookup join。");

    /**
     * lookup join 的每个任务的 Bootstrap 并行度配置选项。
     * <p>定义了 lookup join 的每个任务的 Bootstrap 并行度。</p>
     */
    public static final ConfigOption<Integer> LOOKUP_BOOTSTRAP_PARALLELISM =
            ConfigOptions.key("lookup.bootstrap-parallelism") // 配置键
                    .intType() // 配置值的类型为整数
                    .defaultValue(4) // 默认值为 4
                    .withDescription(
                            "lookup join 的每个任务的 Bootstrap 并行度。");

    /**
     * lookup join 的异步线程数量配置选项。
     * <p>定义了 lookup join 的异步线程数量。</p>
     */
    public static final ConfigOption<Integer> LOOKUP_ASYNC_THREAD_NUMBER =
            ConfigOptions.key("lookup.async-thread-number") // 配置键
                    .intType() // 配置值的类型为整数
                    .defaultValue(16) // 默认值为 16
                    .withDescription("lookup join 的异步线程数量。");

    /**
     * lookup join 的缓存模式配置选项。
     * <p>定义了 lookup join 的缓存模式。</p>
     */
    public static final ConfigOption<LookupCacheMode> LOOKUP_CACHE_MODE =
            ConfigOptions.key("lookup.cache") // 配置键
                    .enumType(LookupCacheMode.class) // 配置值的类型为枚举
                    .defaultValue(LookupCacheMode.AUTO) // 默认值为 AUTO
                    .withDescription("lookup join 的缓存模式。");

    /**
     * lookup 的动态分区配置选项。
     * <p>定义了 lookup 的动态分区。</p>
     */
    public static final ConfigOption<String> LOOKUP_DYNAMIC_PARTITION =
            ConfigOptions.key("lookup.dynamic-partition") // 配置键
                    .stringType() // 配置值的类型为字符串
                    .noDefaultValue() // 无默认值
                    .withDescription(
                            "lookup 的动态分区，目前只支持 'max_pt()'。");

    /**
     * lookup 的动态分区刷新间隔配置选项。
     * <p>定义了 lookup 的动态分区刷新间隔。</p>
     */
    public static final ConfigOption<Duration> LOOKUP_DYNAMIC_PARTITION_REFRESH_INTERVAL =
            ConfigOptions.key("lookup.dynamic-partition.refresh-interval") // 配置键
                    .durationType() // 配置值的类型为 Duration
                    .defaultValue(Duration.ofHours(1)) // 默认值为 1 小时
                    .withDescription(
                            "lookup 的动态分区刷新间隔，扫描所有分区并获取相应的分区。");

    /**
     * 是否在异步线程中刷新 lookup 表配置选项。
     * <p>定义了是否在异步线程中刷新 lookup 表。</p>
     */
    public static final ConfigOption<Boolean> LOOKUP_REFRESH_ASYNC =
            ConfigOptions.key("lookup.refresh.async") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(false) // 默认值为 false
                    .withDescription("是否在异步线程中刷新 lookup 表。");

    /**
     * 异步刷新的待处理快照数配置选项。
     * <p>定义了异步刷新的待处理快照数。</p>
     */
    public static final ConfigOption<Integer> LOOKUP_REFRESH_ASYNC_PENDING_SNAPSHOT_COUNT =
            ConfigOptions.key("lookup.refresh.async.pending-snapshot-count") // 配置键
                    .intType() // 配置值的类型为整数
                    .defaultValue(5) // 默认值为 5
                    .withDescription(
                            "如果待处理的快照数超过阈值，lookup 操作符将以同步方式刷新表。");

    /**
     * 是否为 savepoint 自动生成标签配置选项。
     * <p>定义了是否为 savepoint 自动生成标签。</p>
     */
    public static final ConfigOption<Boolean> SINK_AUTO_TAG_FOR_SAVEPOINT =
            ConfigOptions.key("sink.savepoint.auto-tag") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(false) // 默认值为 false
                    .withDescription(
                            "如果为 true，当 Flink savepoint 创建快照时，会自动生成一个标签。");

    /**
     * Sink committer 的 CPU 配置选项。
     * <p>定义了 Sink committer 的 CPU 配置选项。</p>
     */
    public static final ConfigOption<Double> SINK_COMMITTER_CPU =
            ConfigOptions.key("sink.committer-cpu") // 配置键
                    .doubleType() // 配置值的类型为双精度浮点数
                    .defaultValue(1.0) // 默认值为 1.0
                    .withDescription(
                            "Sink committer 的 CPU 配置选项，用于控制全局 committer 的 CPU 核心数。");

    /**
     * Sink committer 的内存配置选项。
     * <p>定义了 Sink committer 的内存配置选项。</p>
     */
    public static final ConfigOption<MemorySize> SINK_COMMITTER_MEMORY =
            ConfigOptions.key("sink.committer-memory") // 配置键
                    .memoryType() // 配置值的类型为 MemorySize
                    .noDefaultValue() // 无默认值
                    .withDescription(
                            "Sink committer 的内存配置选项，用于控制全局 committer 的堆内存。");

    /**
     * 是否允许 Sink committer 和 writer 操作符进行链式处理配置选项。
     * <p>定义了是否允许 Sink committer 和 writer 操作符进行链式处理。</p>
     */
    public static final ConfigOption<Boolean> SINK_COMMITTER_OPERATOR_CHAINING =
            ConfigOptions.key("sink.committer-operator-chaining") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(true) // 默认值为 true
                    .withDescription(
                            "是否允许 Sink committer 和 writer 操作符进行链式处理。");

    /**
     * Partition 在空闲一段时间后标记为已完成的超时时间配置选项。
     * <p>定义了 Partition 在空闲一段时间后标记为已完成的超时时间。</p>
     */
    public static final ConfigOption<Duration> PARTITION_IDLE_TIME_TO_DONE =
            key("partition.idle-time-to-done") // 配置键
                    .durationType() // 配置值的类型为 Duration
                    .noDefaultValue() // 无默认值
                    .withDescription(
                            "如果一个 Partition 在此时间段内没有新的数据，将标记为已完成状态，表示数据已准备好。");

    /**
     * Partition 的时间间隔配置选项。
     * <p>定义了 Partition 的时间间隔。</p>
     */
    public static final ConfigOption<Duration> PARTITION_TIME_INTERVAL =
            key("partition.time-interval") // 配置键
                    .durationType() // 配置值的类型为 Duration
                    .noDefaultValue() // 无默认值
                    .withDescription(
                            "您可以通过此选项指定 Partition 的时间间隔，例如，每天 Partition 为 '1 d'，每小时 Partition 为 '1 h'。");

    /**
     * 是否在输入结束时标记完成状态配置选项。
     * <p>定义了是否在输入结束时标记完成状态。</p>
     */
    public static final ConfigOption<Boolean> PARTITION_MARK_DONE_WHEN_END_INPUT =
            ConfigOptions.key("partition.end-input-to-done") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(false) // 默认值为 false
                    .withDescription(
                            "是否在输入结束时标记完成状态，指示数据已准备好。");

    /**
     * 用于聚簇分区的列配置选项。
     * <p>定义了用于聚簇分区的列。</p>
     */
    public static final ConfigOption<String> CLUSTERING_COLUMNS =
            key("sink.clustering.by-columns") // 配置键
                    .stringType() // 配置值的类型为字符串
                    .noDefaultValue() // 无默认值
                    .withDescription(
                            "指定在范围分区时用于比较的列名，格式为 'columnName1,columnName2'。如果未设置或设置为空字符串，表示未启用范围分区功能。此选项仅在没有主键且处于批处理模式的无感知桶表中有效。");

    /**
     * 聚簇分区的策略配置选项。
     * <p>定义了聚簇分区的策略。</p>
     */
    public static final ConfigOption<String> CLUSTERING_STRATEGY =
            key("sink.clustering.strategy") // 配置键
                    .stringType() // 配置值的类型为字符串
                    .defaultValue("auto") // 默认值为 "auto"
                    .withDescription(
                            "指定在范围分区时使用的比较算法，包括 'zorder', 'hilbert', 和 'order', 分别对应 z-order 曲线算法，hilbert 曲线算法，和基本类型比较算法。如果未配置，将根据 'sink.clustering.by-columns' 中的列数自动选择算法。少于1列使用 'order'，少于5列使用 'zorder'，5列或更多使用 'hilbert'。");

    /**
     * 是否在聚簇后按 Cluster 排序数据配置选项。
     * <p>定义了是否在聚簇后按 Cluster 排序数据。</p>
     */
    public static final ConfigOption<Boolean> CLUSTERING_SORT_IN_CLUSTER =
            key("sink.clustering.sort-in-cluster") // 配置键
                    .booleanType() // 配置值的类型为布尔值
                    .defaultValue(true) // 默认值为 true
                    .withDescription(
                            "是否在聚簇后按 Cluster 排序每个 Sink 任务的数据。");

    /**
     * 聚簇分区的样本因子配置选项。
     * <p>定义了聚簇分区的样本因子。</p>
     */
    public static final ConfigOption<Integer> CLUSTERING_SAMPLE_FACTOR =
            key("sink.clustering.sample-factor") // 配置键
                    .intType() // 配置值的类型为整数
                    .defaultValue(100) // 默认值为 100
                    .withDescription(
                            "指定样本因子。总样本数 S = 样本因子 F × Sink 并行度 P。最小允许的样本因子为 20。");

    /**
     * 输入结束时的 Watermark 配置选项。
     * <p>定义了输入结束时的 Watermark。</p>
     */
    public static final ConfigOption<Long> END_INPUT_WATERMARK =
            key("end-input.watermark") // 配置键
                    .longType() // 配置值的类型为长整型
                    .noDefaultValue() // 无默认值
                    .withDescription(
                            "在批处理模式或有界流中可选的 endInput Watermark。");

    /**
     * 获取所有配置选项。
     * <p>返回所有配置选项的列表。</p>
     *
     * @return 配置选项列表
     */
    public static List<ConfigOption<?>> getOptions() {
        final Field[] fields = FlinkConnectorOptions.class.getFields();
        final List<ConfigOption<?>> list = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if (ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    list.add((ConfigOption<?>) field.get(FlinkConnectorOptions.class)); // 获取字段的值
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e); // 抛出异常
                }
            }
        }
        return list;
    }

    /**
     * Lookup 缓存模式。
     * <p>定义了 Lookup 缓存的模式。</p>
     */
    public enum LookupCacheMode {
        /** 自动模式，尝试使用部分模式。 */
        AUTO,

        /** 使用全缓存模式。 */
        FULL
    }

    /**
     * Scan 的 Watermark 发射策略。
     * <p>定义了 Watermark 的发射策略。</p>
     */
    public enum WatermarkEmitStrategy implements DescribedEnum {
        ON_PERIODIC(
                "on-periodic",
                "定期发射 Watermark，间隔由 Flink 的 'pipeline.auto-watermark-interval' 控制。"),

        ON_EVENT("on-event", "每条记录发射一次 Watermark。");

        private final String value;
        private final String description;

        WatermarkEmitStrategy(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /**
     * 对于 {@link org.apache.paimon.flink.source.StaticFileStoreSplitEnumerator} 的分片分配模式。
     * <p>定义了分片分配模式。</p>
     */
    public enum SplitAssignMode implements DescribedEnum {
        FAIR(
                "fair",
                "在批处理读取时，平衡分片分布，防止少数任务读取所有数据。"),
        PREEMPTIVE(
                "preemptive",
                "根据任务的消费速度提前分配分片。");

        private final String value;
        private final String description;

        SplitAssignMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }
}
