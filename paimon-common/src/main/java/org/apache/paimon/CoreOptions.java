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

package org.apache.paimon;

import org.apache.paimon.annotation.Documentation;
import org.apache.paimon.annotation.Documentation.ExcludeFromDocumentation;
import org.apache.paimon.annotation.Documentation.Immutable;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.Description;
import org.apache.paimon.options.description.InlineElement;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.MathUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.options.MemorySize.VALUE_128_MB;
import static org.apache.paimon.options.MemorySize.VALUE_256_MB;
import static org.apache.paimon.options.description.TextElement.text;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Core options for paimon.
 * Paimon 的核心配置选项类，用于定义和管理各种配置参数。
 */
public class CoreOptions implements Serializable {

    // 默认值后缀，用于标识字段的默认值配置
    public static final String DEFAULT_VALUE_SUFFIX = "default-value";

    // 字段前缀，用于标识字段相关配置
    public static final String FIELDS_PREFIX = "fields";

    // 字段分隔符，用于分隔多个字段
    public static final String FIELDS_SEPARATOR = ",";

    // 聚合函数配置键
    public static final String AGG_FUNCTION = "aggregate-function";

    // 默认聚合函数配置键
    public static final String DEFAULT_AGG_FUNCTION = "default-aggregate-function";

    // 忽略回撤记录配置键
    public static final String IGNORE_RETRACT = "ignore-retract";

    // 嵌套键配置键
    public static final String NESTED_KEY = "nested-key";

    // 去重配置键
    public static final String DISTINCT = "distinct";

    // 列表聚合分隔符配置键
    public static final String LIST_AGG_DELIMITER = "list-agg-delimiter";

    // 文件索引配置键
    public static final String FILE_INDEX = "file-index";

    // 列配置键
    public static final String COLUMNS = "columns";

    // 桶数量配置
    public static final ConfigOption<Integer> BUCKET =
            key("bucket")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            Description.builder()
                                    .text("文件存储的桶数量。")
                                    .linebreak()
                                    .text(
                                            "应为 -1（动态桶模式）或大于 0 的值（固定桶模式）。")
                                    .build());

    // 桶键配置
    @Immutable
    public static final ConfigOption<String> BUCKET_KEY =
            key("bucket-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "指定 Paimon 的分布策略。数据根据 bucket-key 的哈希值分配到每个桶。")
                                    .linebreak()
                                    .text("如果指定多个字段，分隔符为 ','。")
                                    .linebreak()
                                    .text(
                                            "如果未指定，将使用主键；如果没有主键，则使用整行数据。")
                                    .build());

    // 表路径配置
    @ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<String> PATH =
            key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("表在文件系统中的路径。");

    // 分支名称配置
    public static final ConfigOption<String> BRANCH =
            key("branch").stringType().defaultValue("main").withDescription("指定分支名称。");

    // 文件格式配置
    public static final String FILE_FORMAT_ORC = "orc";
    public static final String FILE_FORMAT_AVRO = "avro";
    public static final String FILE_FORMAT_PARQUET = "parquet";

    public static final ConfigOption<String> FILE_FORMAT =
            key("file.format")
                    .stringType()
                    .defaultValue(FILE_FORMAT_PARQUET)
                    .withDescription(
                            "指定数据文件的消息格式，目前支持 orc、parquet 和 avro。");

    // 文件压缩策略配置
    public static final ConfigOption<Map<String, String>> FILE_COMPRESSION_PER_LEVEL =
            key("file.compression.per.level")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "为不同层级定义不同的压缩策略，可以按以下方式添加配置："
                                    + " 'file.compression.per.level' = '0:lz4,1:zstd'。");

    // 文件格式策略配置
    public static final ConfigOption<Map<String, String>> FILE_FORMAT_PER_LEVEL =
            key("file.format.per.level")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "为不同层级定义不同的文件格式，可以按以下方式添加配置："
                                    + " 'file.format.per.level' = '0:avro,3:parquet'。"
                                    + "如果未为某一层级指定文件格式，将使用由 `"
                                    + FILE_FORMAT.key()
                                    + "` 设置的默认格式。");

    // 文件压缩配置
    public static final ConfigOption<String> FILE_COMPRESSION =
            key("file.compression")
                    .stringType()
                    .defaultValue("zstd")
                    .withDescription(
                            "默认文件压缩方式。为了更快的读写速度，建议使用 zstd。");

    // 文件压缩 zstd 级别配置
    public static final ConfigOption<Integer> FILE_COMPRESSION_ZSTD_LEVEL =
            key("file.compression.zstd-level")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "默认文件压缩 zstd 级别。为了更高的压缩率，可以配置为 9，但读写速度会显著降低。");

    // 文件块大小配置
    public static final ConfigOption<MemorySize> FILE_BLOCK_SIZE =
            key("file.block-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "文件块大小，orc 的条带默认为 64 MB，parquet 的行组默认为 128 MB。");

    // 文件索引在清单中的阈值配置
    public static final ConfigOption<MemorySize> FILE_INDEX_IN_MANIFEST_THRESHOLD =
            key("file-index.in-manifest-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.parse("500 B"))
                    .withDescription("存储在清单中的文件索引字节的阈值。");

    // 文件索引读取启用配置
    public static final ConfigOption<Boolean> FILE_INDEX_READ_ENABLED =
            key("file-index.read.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("是否启用读取文件索引。");

    // 清单文件格式配置
    public static final ConfigOption<String> MANIFEST_FORMAT =
            key("manifest.format")
                    .stringType()
                    .defaultValue(CoreOptions.FILE_FORMAT_AVRO)
                    .withDescription("指定清单文件的消息格式。");

    // 清单文件压缩配置
    public static final ConfigOption<String> MANIFEST_COMPRESSION =
            key("manifest.compression")
                    .stringType()
                    .defaultValue("zstd")
                    .withDescription("清单文件的默认压缩方式。");

    // 清单文件目标大小配置
    public static final ConfigOption<MemorySize> MANIFEST_TARGET_FILE_SIZE =
            key("manifest.target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(8))
                    .withDescription("清单文件的建议大小。");

    // 清单文件完全压缩阈值配置
    public static final ConfigOption<MemorySize> MANIFEST_FULL_COMPACTION_FILE_SIZE =
            key("manifest.full-compaction-threshold-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(16))
                    .withDescription(
                            "触发清单文件完全压缩的大小阈值。");

    // 清单文件合并最小数量配置
    public static final ConfigOption<Integer> MANIFEST_MERGE_MIN_COUNT =
            key("manifest.merge-min-count")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "为了避免频繁的清单文件合并，此参数指定需要合并的最小 ManifestFileMeta 数量。");

    // 默认分区名称配置
    public static final ConfigOption<String> PARTITION_DEFAULT_NAME =
            key("partition.default-name")
                    .stringType()
                    .defaultValue("__DEFAULT_PARTITION__")
                    .withDescription(
                            "动态分区列值为 null/空字符串时的默认分区名称。");

    // 快照保留最小数量配置
    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED_MIN =
            key("snapshot.num-retained.min")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "要保留的完成快照的最小数量。应大于或等于 1。");

    // 快照保留最大数量配置
    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED_MAX =
            key("snapshot.num-retained.max")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            "要保留的完成快照的最大数量。应大于或等于最小数量。");

    // 快照保留时间配置
    public static final ConfigOption<Duration> SNAPSHOT_TIME_RETAINED =
            key("snapshot.time-retained")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("要保留的完成快照的最大时间。");


    public static final ConfigOption<Integer> CHANGELOG_NUM_RETAINED_MIN =
            key("changelog.num-retained.min")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The minimum number of completed changelog to retain. Should be greater than or equal to 1.");

    public static final ConfigOption<Integer> CHANGELOG_NUM_RETAINED_MAX =
            key("changelog.num-retained.max")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum number of completed changelog to retain. Should be greater than or equal to the minimum number.");

    public static final ConfigOption<Duration> CHANGELOG_TIME_RETAINED =
            key("changelog.time-retained")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The maximum time of completed changelog to retain.");

    // 快照过期执行模式配置
    public static final ConfigOption<ExpireExecutionMode> SNAPSHOT_EXPIRE_EXECUTION_MODE =
            key("snapshot.expire.execution-mode")
                    .enumType(ExpireExecutionMode.class)
                    .defaultValue(ExpireExecutionMode.SYNC)
                    .withDescription("指定过期的执行模式。");

    // 快照过期最大限制配置
    public static final ConfigOption<Integer> SNAPSHOT_EXPIRE_LIMIT =
            key("snapshot.expire.limit")
                    .intType()
                    .defaultValue(10)
                    .withDescription("允许同时过期的最大快照数量。");

    // 快照清理空目录配置
    public static final ConfigOption<Boolean> SNAPSHOT_CLEAN_EMPTY_DIRECTORIES =
            key("snapshot.clean-empty-directories")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys("snapshot.expire.clean-empty-directories")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "是否在过期快照时尝试清理空目录，如果启用，请注意：")
                                    .list(
                                            text("hdfs: 可能在 NameNode 中打印异常。"),
                                            text("oss/s3: 可能导致性能问题。"))
                                    .build());

    // 持续发现间隔配置
    public static final ConfigOption<Duration> CONTINUOUS_DISCOVERY_INTERVAL =
            key("continuous.discovery-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("持续读取的发现间隔。");

    // 扫描每任务最大拆分数量配置
    public static final ConfigOption<Integer> SCAN_MAX_SPLITS_PER_TASK =
            key("scan.max-splits-per-task")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "扫描时一个任务应缓存的最大拆分大小。"
                                    + "如果枚举器中缓存的拆分大小大于任务大小乘以该值，扫描器将暂停扫描。");

    // 合并引擎配置
    @Immutable
    public static final ConfigOption<MergeEngine> MERGE_ENGINE =
            key("merge-engine")
                    .enumType(MergeEngine.class)
                    .defaultValue(MergeEngine.DEDUPLICATE)
                    .withDescription("指定具有主键的表的合并引擎。");

    // 忽略删除配置
    @Immutable
    public static final ConfigOption<Boolean> IGNORE_DELETE =
            key("ignore-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys(
                            "first-row.ignore-delete",
                            "deduplicate.ignore-delete",
                            "partial-update.ignore-delete")
                    .withDescription("是否忽略删除记录。");

    // 排序引擎配置
    public static final ConfigOption<SortEngine> SORT_ENGINE =
            key("sort-engine")
                    .enumType(SortEngine.class)
                    .defaultValue(SortEngine.LOSER_TREE)
                    .withDescription("指定具有主键的表的排序引擎。");

    // 排序溢出阈值配置
    public static final ConfigOption<Integer> SORT_SPILL_THRESHOLD =
            key("sort-spill-threshold")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "如果排序读取器的最大数量超过该值，将尝试溢出。"
                                    + "这可以防止过多的读取器消耗过多内存并导致 OOM。");

    // 排序溢出缓冲区大小配置
    public static final ConfigOption<MemorySize> SORT_SPILL_BUFFER_SIZE =
            key("sort-spill-buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 mb"))
                    .withDescription("在溢出排序中将记录溢出到磁盘的数据量。");

    // 溢出压缩配置
    public static final ConfigOption<String> SPILL_COMPRESSION =
            key("spill-compression")
                    .stringType()
                    .defaultValue("zstd")
                    .withDescription(
                            "溢出的压缩方式，目前支持 zstd、lzo 和 zstd。");

    // 溢出压缩 zstd 级别配置
    public static final ConfigOption<Integer> SPILL_COMPRESSION_ZSTD_LEVEL =
            key("spill-compression.zstd-level")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "默认溢出压缩 zstd 级别。为了更高的压缩率，可以配置为 9，但读写速度会显著降低。");

    // 写入模式配置
    public static final ConfigOption<Boolean> WRITE_ONLY =
            key("write-only")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys("write.compaction-skip")
                    .withDescription(
                            "如果设置为 true，将跳过合并和快照过期。"
                                    + "此选项与专用的 compact jobs 一起使用。");

    // 源拆分目标大小配置
    public static final ConfigOption<MemorySize> SOURCE_SPLIT_TARGET_SIZE =
            key("source.split.target-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("扫描一个桶时的源拆分目标大小。");

    // 源拆分打开文件成本配置
    public static final ConfigOption<MemorySize> SOURCE_SPLIT_OPEN_FILE_COST =
            key("source.split.open-file-cost")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(4))
                    .withDescription(
                            "源文件的打开文件成本。用于避免读取过多的文件，导致源拆分非常慢。");

    // 写入缓冲区大小配置
    public static final ConfigOption<MemorySize> WRITE_BUFFER_SIZE =
            key("write-buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256 mb"))
                    .withDescription(
                            "在转换为排序的磁盘文件之前，在内存中构建的数据量。");

    // 写入缓冲区最大磁盘大小配置
    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<MemorySize> WRITE_BUFFER_MAX_DISK_SIZE =
            key("write-buffer-spill.max-disk-size")
                    .memoryType()
                    .defaultValue(MemorySize.MAX_VALUE)
                    .withDescription(
                            "写入缓冲区溢出使用的最大磁盘大小。仅在启用写入缓冲区溢出时有效");

    // 写入缓冲区是否可溢出配置
    public static final ConfigOption<Boolean> WRITE_BUFFER_SPILLABLE =
            key("write-buffer-spillable")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "是否允许写入缓冲区溢出。在使用对象存储时默认启用。");

    // 写入缓冲区用于追加配置
    public static final ConfigOption<Boolean> WRITE_BUFFER_FOR_APPEND =
            key("write-buffer-for-append")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "此选项仅适用于追加表。是否使用写入缓冲区以避免内存溢出错误。");

    // 写入最大写入器数量配置
    public static final ConfigOption<Integer> WRITE_MAX_WRITERS_TO_SPILL =
            key("write-max-writers-to-spill")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "在批量追加插入时，如果写入器数量大于此选项，我们将打开缓冲区缓存和溢出功能，以避免内存溢出。");

    // 写入清单缓存配置
    public static final ConfigOption<MemorySize> WRITE_MANIFEST_CACHE =
            key("write-manifest-cache")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(0))
                    .withDescription("用于写入初始化的清单文件读取的缓存大小。");

    // 本地排序最大文件句柄数量配置
    public static final ConfigOption<Integer> LOCAL_SORT_MAX_NUM_FILE_HANDLES =
            key("local-sort.max-num-file-handles")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "外部合并排序的最大扇入数量。它限制了同时打开的文件数量。"
                                    + "如果设置得太小，可能会导致中间合并。但如果设置得太大，"
                                    + "将同时打开过多文件，消耗内存并导致随机读取。");

    // 内存页大小配置
    public static final ConfigOption<MemorySize> PAGE_SIZE =
            key("page-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 kb"))
                    .withDescription("内存页大小。");

    // 缓存页大小配置
    public static final ConfigOption<MemorySize> CACHE_PAGE_SIZE =
            key("cache-page-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 kb"))
                    .withDescription("缓存的内存页大小。");

    // 目标文件大小配置
    public static final ConfigOption<MemorySize> TARGET_FILE_SIZE =
            key("target-file-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("文件的目标大小。")
                                    .list(
                                            text("主键表：默认值为 128 MB。"),
                                            text("追加表：默认值为 256 MB。"))
                                    .build());

    // 排序运行数量触发合并配置
    public static final ConfigOption<Integer> NUM_SORTED_RUNS_COMPACTION_TRIGGER =
            key("num-sorted-run.compaction-trigger")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "触发合并的排序运行数量。包括 level0 文件（一个文件一个排序运行）和高级运行（一个级别一个排序运行）。");

    // 排序运行数量停止触发配置
    public static final ConfigOption<Integer> NUM_SORTED_RUNS_STOP_TRIGGER =
            key("num-sorted-run.stop-trigger")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "触发停止写入的排序运行数量，"
                                    + "默认值为 'num-sorted-run.compaction-trigger' + 3。");

    // 总级别数量配置
    public static final ConfigOption<Integer> NUM_LEVELS =
            key("num-levels")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "总级别数量，例如，有 3 个级别，包括 0、1、2 级别。");

    // 提交前是否强制合并配置
    public static final ConfigOption<Boolean> COMMIT_FORCE_COMPACT =
            key("commit.force-compact")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否在提交前强制进行合并。");

    // 合并最大大小放大百分比配置
    public static final ConfigOption<Integer> COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT =
            key("compaction.max-size-amplification-percent")
                    .intType()
                    .defaultValue(200)
                    .withDescription(
                            "大小放大定义为在合并树中存储单个字节的数据所需的额外存储量（以百分比表示）。");

    // 合并大小比例配置
    public static final ConfigOption<Integer> COMPACTION_SIZE_RATIO =
            key("compaction.size-ratio")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "在比较排序运行大小时的百分比灵活性。如果候选排序运行的大小比下一个排序运行的大小小 1%，"
                                    + "则将下一个排序运行包含在候选集合中。");

    // 合并优化间隔配置
    public static final ConfigOption<Duration> COMPACTION_OPTIMIZATION_INTERVAL =
            key("compaction.optimization-interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "指定执行优化合并的间隔，此配置用于确保读取优化的系统表的查询及时性。");

    // 合并最小文件数量配置
    public static final ConfigOption<Integer> COMPACTION_MIN_FILE_NUM =
            key("compaction.min.file-num")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "对于文件集 [f_0,...,f_N]，满足 sum(size(f_i)) >= targetFileSize 以触发追加表的合并的最小文件数量。"
                                    + "此值避免几乎满文件被合并，这并不划算。");

    // 合并最大文件数量配置
    public static final ConfigOption<Integer> COMPACTION_MAX_FILE_NUM =
            key("compaction.max.file-num")
                    .intType()
                    .noDefaultValue()
                    .withFallbackKeys("compaction.early-max.file-num")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "对于文件集 [f_0,...,f_N]，即使 sum(size(f_i)) < targetFileSize，"
                                                    + "触发追加表合并的最大文件数量。此值避免挂起过多小文件。")
                                    .list(
                                            text("追加表的默认值为 '50'。"),
                                            text("分区追加表的默认值为 '5'。"))
                                    .build());

    // 变更日志生产者配置
    public static final ConfigOption<ChangelogProducer> CHANGELOG_PRODUCER =
            key("changelog-producer")
                    .enumType(ChangelogProducer.class)
                    .defaultValue(ChangelogProducer.NONE)
                    .withDescription(
                            "是否双写到变更日志文件。"
                                    + "此变更日志文件保留数据变化的详细信息，"
                                    + "可以在流式读取期间直接读取。适用于具有主键的表。");

    // 变更日志生产者行去重配置
    public static final ConfigOption<Boolean> CHANGELOG_PRODUCER_ROW_DEDUPLICATE =
            key("changelog-producer.row-deduplicate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "是否为相同记录生成 -U、+U 变更日志。此配置仅在 changelog-producer 为 lookup 或 full-compaction 时有效。");

    // 序列号字段配置
    @Immutable
    public static final ConfigOption<String> SEQUENCE_FIELD =
            key("sequence.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "主键表的序列号生成字段，"
                                    + "序列号确定哪条数据是最新的。");

    // 部分更新删除记录配置
    @Immutable
    public static final ConfigOption<Boolean> PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE =
            key("partial-update.remove-record-on-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "在部分更新引擎中，当收到 -D 记录时，是否删除整行。");

    // 行类型字段配置
    @Immutable
    public static final ConfigOption<String> ROWKIND_FIELD =
            key("rowkind.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "主键表的行类型生成字段，"
                                    + "行类型确定数据是 '+I'、'-U'、'+U' 还是 '-D'。");

    // 扫描模式配置
    public static final ConfigOption<StartupMode> SCAN_MODE =
            key("scan.mode")
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.DEFAULT)
                    .withFallbackKeys("log.scan")
                    .withDescription("指定源的扫描行为。");

    // 扫描时间戳配置
    public static final ConfigOption<String> SCAN_TIMESTAMP =
            key("scan.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "在使用 'from-timestamp' 扫描模式时的可选时间戳，将自动转换为 Unix 毫秒时间戳，使用本地时区。");

    // 扫描时间戳毫秒配置
    public static final ConfigOption<Long> SCAN_TIMESTAMP_MILLIS =
            key("scan.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withFallbackKeys("log.scan.timestamp-millis")
                    .withDescription(
                            "在使用 'from-timestamp' 扫描模式时的可选时间戳。"
                                    + "如果不存在早于该时间的快照，将选择最早的快照。");

    // 扫描水位线配置
    public static final ConfigOption<Long> SCAN_WATERMARK =
            key("scan.watermark")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "在使用 'from-snapshot' 扫描模式时的可选水位线。"
                                    + "如果不存在晚于该水位线的快照，将抛出异常。");

    // 扫描文件创建时间毫秒配置
    public static final ConfigOption<Long> SCAN_FILE_CREATION_TIME_MILLIS =
            key("scan.file-creation-time-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "配置该时间后，仅读取该时间之后创建的数据文件。"
                                    + "它独立于快照，但过滤不精确（取决于是否发生合并）。");

    // 扫描快照 ID 配置
    public static final ConfigOption<Long> SCAN_SNAPSHOT_ID =
            key("scan.snapshot-id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "在使用 'from-snapshot' 或 'from-snapshot-full' 扫描模式时的可选快照 ID。");

    // 扫描标签名称配置
    public static final ConfigOption<String> SCAN_TAG_NAME =
            key("scan.tag-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "在使用 'from-snapshot' 扫描模式时的可选标签名称。");

    // 扫描版本配置
    @ExcludeFromDocumentation("Confused without log system")
    public static final ConfigOption<String> SCAN_VERSION =
            key("scan.version")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "用于 'VERSION AS OF' 语法的指定时间旅行版本字符串。"
                                    + "如果存在该版本的标签和快照，将使用标签。");

    // 扫描有界水位线配置
    public static final ConfigOption<Long> SCAN_BOUNDED_WATERMARK =
            key("scan.bounded.watermark")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "有界流模式的结束条件 'watermark'。"
                                    + "当遇到更大的水位线快照时，流读取将结束。");

    // 扫描清单并行度配置
    public static final ConfigOption<Integer> SCAN_MANIFEST_PARALLELISM =
            key("scan.manifest.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "扫描清单文件的并行度，默认值为 CPU 处理器的数量。"
                                    + "注意：增加此参数会增加扫描清单文件时的内存使用。"
                                    + "如果在扫描清单文件时遇到内存溢出异常，可以考虑降低此参数。");

    // 流式读取快照延迟配置
    public static final ConfigOption<Duration> STREAMING_READ_SNAPSHOT_DELAY =
            key("streaming.read.snapshot.delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("扫描增量快照时的延迟时长。");

    // 日志一致性模式配置
    @ExcludeFromDocumentation("Confused without log system")
    public static final ConfigOption<LogConsistency> LOG_CONSISTENCY =
            key("log.consistency")
                    .enumType(LogConsistency.class)
                    .defaultValue(LogConsistency.TRANSACTIONAL)
                    .withDescription("指定表的日志一致性模式。");

    // 日志变更日志模式配置
    @ExcludeFromDocumentation("Confused without log system")
    public static final ConfigOption<LogChangelogMode> LOG_CHANGELOG_MODE =
            key("log.changelog-mode")
                    .enumType(LogChangelogMode.class)
                    .defaultValue(LogChangelogMode.AUTO)
                    .withDescription("指定表的日志变更日志模式。");

    // 日志键格式配置
    @ExcludeFromDocumentation("Confused without log system")
    public static final ConfigOption<String> LOG_KEY_FORMAT =
            key("log.key.format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription("指定具有主键的日志系统的键消息格式。");

    // 日志格式配置
    @ExcludeFromDocumentation("Confused without log system")
    public static final ConfigOption<String> LOG_FORMAT =
            key("log.format")
                    .stringType()
                    .defaultValue("debezium-json")
                    .withDescription("指定日志系统的消息格式。");

    // 自动创建配置
    public static final ConfigOption<Boolean> AUTO_CREATE =
            key("auto-create")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "是否在读写表时创建底层存储。");

    // 流式读取覆盖配置
    public static final ConfigOption<Boolean> STREAMING_READ_OVERWRITE =
            key("streaming-read-overwrite")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "是否在流式模式下读取覆盖的更改。"
                                    + "当 changelog producer 为 full-compaction 或 lookup 时，不能设置为 true，"
                                    + "因为它将读取重复的更改。");

    // 动态分区覆盖配置
    public static final ConfigOption<Boolean> DYNAMIC_PARTITION_OVERWRITE =
            key("dynamic-partition-overwrite")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "仅在覆盖分区表时覆盖动态分区，"
                                    + "适用于具有动态分区列的分区表。");

    // 分区过期策略配置
    public static final ConfigOption<PartitionExpireStrategy> PARTITION_EXPIRATION_STRATEGY =
            key("partition.expiration-strategy")
                    .enumType(PartitionExpireStrategy.class)
                    .defaultValue(PartitionExpireStrategy.VALUES_TIME)
                    .withDescription(
                            "该策略确定如何提取分区时间并与当前时间进行比较。");

    // 分区过期时间配置
    public static final ConfigOption<Duration> PARTITION_EXPIRATION_TIME =
            key("partition.expiration-time")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "分区的过期间隔。如果分区的生命周期超过该值，分区将过期。"
                                    + "分区时间从分区值中提取。");

    // 分区过期检查间隔配置
    public static final ConfigOption<Duration> PARTITION_EXPIRATION_CHECK_INTERVAL =
            key("partition.expiration-check-interval")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("分区过期的检查间隔。");

    // 分区时间戳格式化器配置
    public static final ConfigOption<String> PARTITION_TIMESTAMP_FORMATTER =
            key("partition.timestamp-formatter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "从字符串格式化时间戳的格式化器。"
                                                    + "可以与 'partition.timestamp-pattern' 一起使用，"
                                                    + "使用指定的值创建格式化器。")
                                    .list(
                                            text("默认格式化器为 'yyyy-MM-dd HH:mm:ss' 和 'yyyy-MM-dd'。"),
                                            text(
                                                    "支持多个分区字段，如 '$year-$month-$day $hour:00:00'。"),
                                            text(
                                                    "partition.timestamp-formatter 与 Java 的 DateTimeFormatter 兼容。"))
                                    .build());

    // 分区时间戳模式配置
    public static final ConfigOption<String> PARTITION_TIMESTAMP_PATTERN =
            key("partition.timestamp-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "可以从分区中获取时间戳的模式。"
                                                    + "formatter 模式由 'partition.timestamp-formatter' 定义。")
                                    .list(
                                            text("默认情况下，从第一个字段读取。"),
                                            text(
                                                    "如果分区中的时间戳是单个字段（如 'dt'），可以使用 '$dt'。"),
                                            text(
                                                    "如果时间戳分布在多个字段中，"
                                                            + "如 year、month、day 和 hour，可以使用 '$year-$month-$day $hour:00:00'。"),
                                            text(
                                                    "如果时间戳在 dt 和 hour 字段中，可以使用 '$dt $hour:00:00'。"))
                                    .build());

    // 扫描计划排序分区配置
    public static final ConfigOption<Boolean> SCAN_PLAN_SORT_PARTITION =
            key("scan.plan-sort-partition")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "是否按分区字段对计划文件进行排序，这允许您根据分区顺序读取，"
                                                    + "即使分区写入是无序的。")
                                    .linebreak()
                                    .text(
                                            "建议在 'append-only' 表的流式读取中使用。"
                                                    + "默认情况下，流式读取将首先读取完整快照。"
                                                    + "为了避免分区读取无序，可以打开此选项。")
                                    .build());

    // 主键配置
    @Immutable
    public static final ConfigOption<String> PRIMARY_KEY =
            key("primary-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "通过表选项定义主键，不能在 DDL 和表选项中同时定义主键。");

    // 分区配置
    @Immutable
    public static final ConfigOption<String> PARTITION =
            key("partition")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "通过表选项定义分区，不能在 DDL 和表选项中同时定义分区。");

    // 查找本地文件类型配置
    public static final ConfigOption<LookupLocalFileType> LOOKUP_LOCAL_FILE_TYPE =
            key("lookup.local-file-type")
                    .enumType(LookupLocalFileType.class)
                    .defaultValue(LookupLocalFileType.HASH)
                    .withDescription("查找的本地文件类型。");

    // 查找哈希负载因子配置
    public static final ConfigOption<Float> LOOKUP_HASH_LOAD_FACTOR =
            key("lookup.hash-load-factor")
                    .floatType()
                    .defaultValue(0.75F)
                    .withDescription("查找的索引负载因子。");

    // 查找缓存文件保留时间配置
    public static final ConfigOption<Duration> LOOKUP_CACHE_FILE_RETENTION =
            key("lookup.cache-file-retention")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription(
                            "查找缓存文件的保留时间。"
                                    + "文件过期后，如果需要访问，将从 DFS 重新读取以在本地磁盘上构建索引。");

    // 查找缓存最大磁盘大小配置
    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<MemorySize> LOOKUP_CACHE_MAX_DISK_SIZE =
            key("lookup.cache-max-disk-size")
                    .memoryType()
                    .defaultValue(MemorySize.MAX_VALUE)
                    .withDescription(
                            "查找缓存的最大磁盘大小，可以使用此选项限制本地磁盘的使用。");

    // 查找缓存溢出压缩配置
    public static final ConfigOption<String> LOOKUP_CACHE_SPILL_COMPRESSION =
            key("lookup.cache-spill-compression")
                    .stringType()
                    .defaultValue("zstd")
                    .withDescription(
                            "查找缓存的溢出压缩，目前支持 zstd、none、lz4 和 lzo。");

    // 查找缓存最大内存大小配置
    public static final ConfigOption<MemorySize> LOOKUP_CACHE_MAX_MEMORY_SIZE =
            key("lookup.cache-max-memory-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256 mb"))
                    .withDescription("查找缓存的最大内存大小。");

    // 查找缓存布隆过滤器启用配置
    public static final ConfigOption<Boolean> LOOKUP_CACHE_BLOOM_FILTER_ENABLED =
            key("lookup.cache.bloom.filter.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("是否启用查找缓存的布隆过滤器。");

    // 查找缓存布隆过滤器误报概率配置
    public static final ConfigOption<Double> LOOKUP_CACHE_BLOOM_FILTER_FPP =
            key("lookup.cache.bloom.filter.fpp")
                    .doubleType()
                    .defaultValue(0.05)
                    .withDescription("查找缓存布隆过滤器的默认误报概率。");

    // 读取批大小配置
    public static final ConfigOption<Integer> READ_BATCH_SIZE =
            key("read.batch-size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("orc 和 parquet 的读取批大小。");

    // 消费者 ID 配置
    public static final ConfigOption<String> CONSUMER_ID =
            key("consumer-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "用于在存储中记录消费偏移量的消费者 ID。");

    // 完全合并 delta 提交配置
    public static final ConfigOption<Integer> FULL_COMPACTION_DELTA_COMMITS =
            key("full-compaction.delta-commits")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "在 delta 提交后不断触发完全合并。");

    // 流式扫描模式配置
    @ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<StreamScanMode> STREAM_SCAN_MODE =
            key("stream-scan-mode")
                    .enumType(StreamScanMode.class)
                    .defaultValue(StreamScanMode.NONE)
                    .withDescription(
                            "仅用于强制 TableScan 构造合适的 'StartingUpScanner' 和 'FollowUpScanner'，"
                                    + "用于内部流式扫描。");

    // 流式读取模式配置
    public static final ConfigOption<StreamingReadMode> STREAMING_READ_MODE =
            key("streaming-read-mode")
                    .enumType(StreamingReadMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "指定流式读取模式，用于读取表文件或日志的数据。");

    // 消费者过期时间配置
    public static final ConfigOption<Duration> CONSUMER_EXPIRATION_TIME =
            key("consumer.expiration-time")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "消费者文件的过期间隔。"
                                    + "如果文件的生命周期超过该值，将过期。");

    // 消费者一致性模式配置
    public static final ConfigOption<ConsumerMode> CONSUMER_CONSISTENCY_MODE =
            key("consumer.mode")
                    .enumType(ConsumerMode.class)
                    .defaultValue(ConsumerMode.EXACTLY_ONCE)
                    .withDescription("指定表的消费者一致性模式。");

    // 消费者忽略进度配置
    public static final ConfigOption<Boolean> CONSUMER_IGNORE_PROGRESS =
            key("consumer.ignore-progress")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否忽略新启动作业的消费者进度。");

    // 动态桶目标行数配置
    public static final ConfigOption<Long> DYNAMIC_BUCKET_TARGET_ROW_NUM =
            key("dynamic-bucket.target-row-num")
                    .longType()
                    .defaultValue(2_000_000L)
                    .withDescription(
                            "如果桶为 -1，对于主键表，是动态桶模式，"
                                    + "此选项控制每个桶的目标行数。");

    // 动态桶初始桶数配置
    public static final ConfigOption<Integer> DYNAMIC_BUCKET_INITIAL_BUCKETS =
            key("dynamic-bucket.initial-buckets")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "动态桶模式下分配器操作符的初始桶数。");

    // 动态桶分配器并行度配置
    public static final ConfigOption<Integer> DYNAMIC_BUCKET_ASSIGNER_PARALLELISM =
            key("dynamic-bucket.assigner-parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "动态桶模式下分配器操作符的并行度，"
                                    + "它与初始化的桶数有关，太小会导致分配器处理速度不足。");

    // 增量之间配置
    public static final ConfigOption<String> INCREMENTAL_BETWEEN =
            key("incremental-between")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "读取开始快照（不包含）和结束快照之间的增量更改，"
                                    + "例如，'5,10' 表示快照 5 和快照 10 之间的更改。");

    // 增量之间扫描模式配置
    public static final ConfigOption<IncrementalBetweenScanMode> INCREMENTAL_BETWEEN_SCAN_MODE =
            key("incremental-between-scan-mode")
                    .enumType(IncrementalBetweenScanMode.class)
                    .defaultValue(IncrementalBetweenScanMode.AUTO)
                    .withDescription(
                            "读取开始快照（不包含）和结束快照之间的增量更改时的扫描类型。");

    // 增量之间时间戳配置
    public static final ConfigOption<String> INCREMENTAL_BETWEEN_TIMESTAMP =
            key("incremental-between-timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "读取开始时间戳（不包含）和结束时间戳之间的增量更改，"
                                    + "例如，'t1,t2' 表示时间戳 t1 和时间戳 t2 之间的更改。");

    // 结束输入检查分区过期配置
    public static final ConfigOption<Boolean> END_INPUT_CHECK_PARTITION_EXPIRE =
            key("end-input.check-partition-expire")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "在批处理模式或有界流中可选的结束输入检查分区过期。");

    // 元数据统计模式配置
    public static final String STATS_MODE_SUFFIX = "stats-mode";

    public static final ConfigOption<String> METADATA_STATS_MODE =
            key("metadata." + STATS_MODE_SUFFIX)
                    .stringType()
                    .defaultValue("truncate(16)")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "元数据统计信息的收集模式。"
                                                    + "可用 none、counts、truncate(16)、full。")
                                    .linebreak()
                                    .list(
                                            text(
                                                    "\"none\": 禁用元数据统计信息收集。"))
                                    .list(
                                            text(
                                                    "\"counts\": 仅收集 null 计数。"))
                                    .list(
                                            text(
                                                    "\"full\": 收集 null 计数、最小值/最大值。"))
                                    .list(
                                            text(
                                                    "\"truncate(16)\": 收集 null 计数、最小值/最大值，"
                                                            + "截断长度为 16。"))
                                    .list(
                                            text(
                                                    "字段级别的统计模式可以通过 "
                                                            + FIELDS_PREFIX
                                                            + "."
                                                            + "{field_name}."
                                                            + STATS_MODE_SUFFIX
                                                            + " 指定。"))
                                    .build());

    // 提交回调配置
    public static final ConfigOption<String> COMMIT_CALLBACKS =
            key("commit.callbacks")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "提交成功后调用的一系列提交回调类。"
                                    + "类名用逗号连接 "
                                    + "(示例：com.test.CallbackA,com.sample.CallbackB)。");

    // 提交回调参数配置
    public static final ConfigOption<String> COMMIT_CALLBACK_PARAM =
            key("commit.callback.#.param")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "类 # 构造函数的参数字符串。"
                                    + "回调类应自行解析参数。");

    // 标签回调配置
    public static final ConfigOption<String> TAG_CALLBACKS =
            key("tag.callbacks")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "提交成功后调用的一系列标签回调类。"
                                    + "类名用逗号连接 "
                                    + "(示例：com.test.CallbackA,com.sample.CallbackB)。");

    // 标签回调参数配置
    public static final ConfigOption<String> TAG_CALLBACK_PARAM =
            key("tag.callback.#.param")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "类 # 构造函数的参数字符串。"
                                    + "回调类应自行解析参数。");

    // 分区完成操作配置
    public static final ConfigOption<String> PARTITION_MARK_DONE_ACTION =
            key("partition.mark-done-action")
                    .stringType()
                    .defaultValue("success-file")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "标记分区完成的操作，用于通知下游应用程序分区已写入完成，"
                                                    + "分区已准备好读取。")
                                    .linebreak()
                                    .text("1. 'success-file': 向目录添加 '_success' 文件。")
                                    .linebreak()
                                    .text(
                                            "2. 'done-partition': 向元存储添加 'xxx.done' 分区。")
                                    .linebreak()
                                    .text(
                                            "3. 'mark-event': 向元存储标记分区事件。")
                                    .linebreak()
                                    .text(
                                            "可以同时配置多个：'done-partition,success-file,mark-event'。")
                                    .build());

    // 元存储分区表配置
    public static final ConfigOption<Boolean> METASTORE_PARTITIONED_TABLE =
            key("metastore.partitioned-table")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "是否在元存储中将此表创建为分区表。"
                                    + "例如，如果希望在 Hive 中列出 Paimon 表的所有分区，"
                                    + "需要在 Hive 元存储中创建此表为分区表。"
                                    + "此配置选项不影响默认文件系统元存储。");

    // 元存储标签到分区配置
    public static final ConfigOption<String> METASTORE_TAG_TO_PARTITION =
            key("metastore.tag-to-partition")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "是否在元存储中将非分区表的标签映射为分区。"
                                    + "这允许 Hive 引擎以分区表视图查看此表，并使用分区字段读取特定分区（特定标签）。");

    // 元存储标签到分区预览配置
    public static final ConfigOption<TagCreationMode> METASTORE_TAG_TO_PARTITION_PREVIEW =
            key("metastore.tag-to-partition.preview")
                    .enumType(TagCreationMode.class)
                    .defaultValue(TagCreationMode.NONE)
                    .withDescription(
                            "是否在元存储中预览生成快照的标签。"
                                    + "这允许 Hive 引擎在创建之前查询特定标签。");

    // 标签自动创建配置
    public static final ConfigOption<TagCreationMode> TAG_AUTOMATIC_CREATION =
            key("tag.automatic-creation")
                    .enumType(TagCreationMode.class)
                    .defaultValue(TagCreationMode.NONE)
                    .withDescription("是否自动创建标签。以及如何生成标签。");

    // 标签创建周期配置
    public static final ConfigOption<TagCreationPeriod> TAG_CREATION_PERIOD =
            key("tag.creation-period")
                    .enumType(TagCreationPeriod.class)
                    .defaultValue(TagCreationPeriod.DAILY)
                    .withDescription("生成标签的频率。");

    // 标签创建延迟配置
    public static final ConfigOption<Duration> TAG_CREATION_DELAY =
            key("tag.creation-delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "周期结束后创建标签的延迟时长。"
                                    + "这可以允许一些迟到的数据进入标签。");

    // 标签周期格式化器配置
    public static final ConfigOption<TagPeriodFormatter> TAG_PERIOD_FORMATTER =
            key("tag.period-formatter")
                    .enumType(TagPeriodFormatter.class)
                    .defaultValue(TagPeriodFormatter.WITH_DASHES)
                    .withDescription("标签周期的日期格式。");

    // 标签保留最大数量配置
    public static final ConfigOption<Integer> TAG_NUM_RETAINED_MAX =
            key("tag.num-retained.max")
                    .intType()
                    .noDefaultValue()
                    .withDescription("要保留的标签的最大数量。仅影响自动创建的标签。");

    // 标签默认保留时间配置
    public static final ConfigOption<Duration> TAG_DEFAULT_TIME_RETAINED =
            key("tag.default-time-retained")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "新创建标签的最大保留时间。"
                                    + "它影响自动创建的标签和手动创建（通过存储过程）的标签。");

    // 标签自动完成配置
    public static final ConfigOption<Boolean> TAG_AUTOMATIC_COMPLETION =
            key("tag.automatic-completion")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否自动完成缺失的标签。");

    // 快照水位线空闲超时配置
    public static final ConfigOption<Duration> SNAPSHOT_WATERMARK_IDLE_TIMEOUT =
            key("snapshot.watermark-idle-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "在水位线中，如果源在指定的超时持续时间内保持空闲，"
                                    + "将触发快照推进并促进标签创建。");

    // Parquet 禁用字典编码配置
    public static final ConfigOption<Integer> PARQUET_ENABLE_DICTIONARY =
            key("parquet.enable.dictionary")
                    .intType()
                    .noDefaultValue()
                    .withDescription("关闭 Parquet 中所有字段的字典编码。");

    // 沉淀水位线时区配置
    public static final ConfigOption<String> SINK_WATERMARK_TIME_ZONE =
            key("sink.watermark-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription(
                            "将 long 水位线值解析为 TIMESTAMP 值的时区。"
                                    + "默认值为 'UTC'，这意味着水位线定义在 TIMESTAMP 列或未定义。"
                                    + "如果水位线定义在 TIMESTAMP_LTZ 列，水位线的时区是用户配置的时区，"
                                    + "值应为用户配置的本地时区。选项值可以是全名，"
                                    + "例如 'America/Los_Angeles'，或自定义时区 ID，例如 'GMT-08:00'。");

    // 本地合并缓冲区大小配置
    public static final ConfigOption<MemorySize> LOCAL_MERGE_BUFFER_SIZE =
            key("local-merge-buffer-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "本地合并将在将输入记录缓冲并合并后，"
                                    + "再进行桶内 shuffle 并写入 sink。"
                                    + "缓冲区将在满时刷新。"
                                    + "主要用于解决主键的数据倾斜问题。"
                                    + "尝试此功能时，建议从 64 mb 开始。");

    // 跨分区更新索引 TTL 配置
    public static final ConfigOption<Duration> CROSS_PARTITION_UPSERT_INDEX_TTL =
            key("cross-partition-upsert.index-ttl")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "跨分区更新（主键不包含所有分区字段）的 RocksDB 索引的 TTL，"
                                    + "这可以避免维护过多索引，导致性能越来越差，"
                                    + "但请注意，这可能导致数据重复。");

    // 跨分区更新引导并行度配置
    public static final ConfigOption<Integer> CROSS_PARTITION_UPSERT_BOOTSTRAP_PARALLELISM =
            key("cross-partition-upsert.bootstrap-parallelism")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "跨分区更新的单个任务的引导并行度。");

    // Zorder 可变长度贡献配置
    public static final ConfigOption<Integer> ZORDER_VAR_LENGTH_CONTRIBUTION =
            key("zorder.var-length-contribution")
                    .intType()
                    .defaultValue(8)
                    .withDescription(
                            "类型 (CHAR, VARCHAR, BINARY, VARBINARY) 为 zorder 排序贡献的字节数。");

    // 文件读取器异步阈值配置
    public static final ConfigOption<MemorySize> FILE_READER_ASYNC_THRESHOLD =
            key("file-reader-async-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(10))
                    .withDescription("读取文件的异步阈值。");

    // 提交时是否强制创建快照配置
    public static final ConfigOption<Boolean> COMMIT_FORCE_CREATE_SNAPSHOT =
            key("commit.force-create-snapshot")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("提交时是否强制创建快照。");

    // 删除向量启用配置
    public static final ConfigOption<Boolean> DELETION_VECTORS_ENABLED =
            key("deletion-vectors.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "是否启用删除向量模式。在此模式下，写入数据时生成包含删除向量的索引文件，"
                                    + "这标记了要删除的数据。在读取操作期间，通过应用这些索引文件，可以避免合并。");

    // 删除向量索引文件目标大小配置
    public static final ConfigOption<MemorySize> DELETION_VECTOR_INDEX_FILE_TARGET_SIZE =
            key("deletion-vector.index-file.target-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(2))
                    .withDescription("删除向量索引文件的目标大小。");

    // 删除是否强制生成变更日志配置
    public static final ConfigOption<Boolean> DELETION_FORCE_PRODUCE_CHANGELOG =
            key("delete.force-produce-changelog")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "删除 sql 中强制生成变更日志，"
                                    + "或者可以使用 'streaming-read-overwrite' 从覆盖提交中读取变更日志。");

    // 排序压缩范围策略配置
    public static final ConfigOption<RangeStrategy> SORT_RANG_STRATEGY =
            key("sort-compaction.range-strategy")
                    .enumType(RangeStrategy.class)
                    .defaultValue(RangeStrategy.QUANTITY)
                    .withDescription(
                            "排序压缩的范围策略，默认值为数量。"
                                    + "如果分配给排序任务的数据大小不均匀，可能导致性能瓶颈，"
                                    + "可以将配置设置为大小。");

    // 排序压缩本地样本放大倍数配置
    public static final ConfigOption<Integer> SORT_COMPACTION_SAMPLE_MAGNIFICATION =
            key("sort-compaction.local-sample.magnification")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "排序压缩的本地样本放大倍数。本地样本的大小为 sink 并行度 * 放大倍数。");

    // 记录级别过期时间配置
    public static final ConfigOption<Duration> RECORD_LEVEL_EXPIRE_TIME =
            key("record-level.expire-time")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "主键表的记录级别过期时间，过期发生在合并期间，"
                                    + "没有强烈保证及时过期记录。"
                                    + "还必须指定 'record-level.time-field'。");

    // 记录级别时间字段配置
    public static final ConfigOption<String> RECORD_LEVEL_TIME_FIELD =
            key("record-level.time-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("记录级别过期的时间字段。");

    // 记录级别时间字段类型配置
    public static final ConfigOption<TimeFieldType> RECORD_LEVEL_TIME_FIELD_TYPE =
            key("record-level.time-field-type")
                    .enumType(TimeFieldType.class)
                    .defaultValue(TimeFieldType.SECONDS_INT)
                    .withDescription(
                            "记录级别过期的时间字段类型，可以是 seconds-int 或 millis-long。");

    // 字段默认聚合函数配置
    public static final ConfigOption<String> FIELDS_DEFAULT_AGG_FUNC =
            key(FIELDS_PREFIX + "." + DEFAULT_AGG_FUNCTION)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "部分更新和聚合合并函数的字段默认聚合函数。");

    // 提交用户前缀配置
    public static final ConfigOption<String> COMMIT_USER_PREFIX =
            key("commit.user-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("指定提交用户的前缀。");

    // 是否强制使用查找进行压缩配置
    @Immutable
    public static final ConfigOption<Boolean> FORCE_LOOKUP =
            key("force-lookup")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否强制在压缩中使用查找。");

    // 查找等待配置
    public static final ConfigOption<Boolean> LOOKUP_WAIT =
            key("lookup-wait")
                    .booleanType()
                    .defaultValue(true)
                    .withFallbackKeys("changelog-producer.lookup-wait")
                    .withDescription("当需要查找时，提交将等待压缩完成查找。");

    // 元数据 Iceberg 兼容配置
    public static final ConfigOption<Boolean> METADATA_ICEBERG_COMPATIBLE =
            key("metadata.iceberg-compatible")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "设置为 true 时，在提交快照后生成 Iceberg 元数据，"
                                    + "以便 Iceberg 读取器可以读取 Paimon 的原始文件。");

    // 删除文件线程数配置
    public static final ConfigOption<Integer> DELETE_FILE_THREAD_NUM =
            key("delete-file.thread-num")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "删除文件的最大并发数。"
                                    + "默认值为 Java 虚拟机可用的处理器数量。");

    // 扫描回退分支配置
    public static final ConfigOption<String> SCAN_FALLBACK_BRANCH =
            key("scan.fallback-branch")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "当批处理作业查询表时，如果当前分支中不存在分区，"
                                    + "读取器将尝试从回退分支中获取该分区。");

    // 异步文件写入配置
    public static final ConfigOption<Boolean> ASYNC_FILE_WRITE =
            key("async-file-write")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("写入文件时是否启用异步 IO。");

    // 选项实例
    private final Options options;

    // 构造函数
    public CoreOptions(Map<String, String> options) {
        this(Options.fromMap(options));
    }

    public CoreOptions(Options options) {
        this.options = options;
    }

    // 从 Map 创建 CoreOptions 实例
    public static CoreOptions fromMap(Map<String, String> options) {
        return new CoreOptions(options);
    }

    // 转换为 Options 实例
    public Options toConfiguration() {
        return options;
    }

    // 转换为 Map 实例
    public Map<String, String> toMap() {
        return options.toMap();
    }

    // 获取桶数量
    public int bucket() {
        return options.get(BUCKET);
    }

    // 获取路径
    public Path path() {
        return path(options.toMap());
    }

    // 获取分支名称
    public String branch() {
        return branch(options.toMap());
    }

    // 静态方法：获取分支名称
    public static String branch(Map<String, String> options) {
        if (options.containsKey(BRANCH.key())) {
            return options.get(BRANCH.key());
        }
        return BRANCH.defaultValue();
    }

    // 静态方法：获取路径
    public static Path path(Map<String, String> options) {
        return new Path(options.get(PATH.key()));
    }

    // 静态方法：获取路径
    public static Path path(Options options) {
        return new Path(options.get(PATH));
    }

    // 获取文件格式类型
    public String formatType() {
        return normalizeFileFormat(options.get(FILE_FORMAT));
    }

    // 创建文件格式实例
    public FileFormat fileFormat() {
        return createFileFormat(options, FILE_FORMAT);
    }

    // 创建清单文件格式实例
    public FileFormat manifestFormat() {
        return createFileFormat(options, MANIFEST_FORMAT);
    }

    // 获取清单文件压缩方式
    public String manifestCompression() {
        return options.get(MANIFEST_COMPRESSION);
    }

    // 获取清单文件目标大小
    public MemorySize manifestTargetSize() {
        return options.get(MANIFEST_TARGET_FILE_SIZE);
    }

    // 获取清单文件完全压缩阈值大小
    public MemorySize manifestFullCompactionThresholdSize() {
        return options.get(MANIFEST_FULL_COMPACTION_FILE_SIZE);
    }

    // 获取写入清单缓存大小
    public MemorySize writeManifestCache() {
        return options.get(WRITE_MANIFEST_CACHE);
    }

    // 获取分区默认名称
    public String partitionDefaultName() {
        return options.get(PARTITION_DEFAULT_NAME);
    }

    // 判断是否按大小排序
    public boolean sortBySize() {
        return options.get(SORT_RANG_STRATEGY) == RangeStrategy.SIZE;
    }

    // 获取本地样本放大倍数
    public Integer getLocalSampleMagnification() {
        return options.get(SORT_COMPACTION_SAMPLE_MAGNIFICATION);
    }

    // 创建文件格式实例
    public static FileFormat createFileFormat(Options options, ConfigOption<String> formatOption) {
        String formatIdentifier = normalizeFileFormat(options.get(formatOption));
        return FileFormat.getFileFormat(options, formatIdentifier);
    }

    // 获取按层级的文件压缩配置
    public Map<Integer, String> fileCompressionPerLevel() {
        Map<String, String> levelCompressions = options.get(FILE_COMPRESSION_PER_LEVEL);
        return levelCompressions.entrySet().stream()
                .collect(Collectors.toMap(e -> Integer.valueOf(e.getKey()), Map.Entry::getValue));
    }

    // 获取按层级的文件格式配置
    public Map<Integer, String> fileFormatPerLevel() {
        Map<String, String> levelFormats = options.get(FILE_FORMAT_PER_LEVEL);
        return levelFormats.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                e -> Integer.valueOf(e.getKey()),
                                e -> normalizeFileFormat(e.getValue())));
    }

    // 标准化文件格式名称
    private static String normalizeFileFormat(String fileFormat) {
        return fileFormat.toLowerCase();
    }

    // 获取字段默认聚合函数
    public String fieldsDefaultFunc() {
        return options.get(FIELDS_DEFAULT_AGG_FUNC);
    }

    public static String createCommitUser(Options options) {
        String commitUserPrefix = options.get(COMMIT_USER_PREFIX);
        return commitUserPrefix == null
                ? UUID.randomUUID().toString()
                : commitUserPrefix + "_" + UUID.randomUUID();
    }

    public boolean definedAggFunc() {
        if (options.contains(FIELDS_DEFAULT_AGG_FUNC)) {
            return true;
        }

        for (String key : options.toMap().keySet()) {
            if (key.startsWith(FIELDS_PREFIX) && key.endsWith(AGG_FUNCTION)) {
                return true;
            }
        }
        return false;
    }

    public String fieldAggFunc(String fieldName) {
        return options.get(
                key(FIELDS_PREFIX + "." + fieldName + "." + AGG_FUNCTION)
                        .stringType()
                        .noDefaultValue());
    }


    // 判断字段聚合是否忽略回撤
    public boolean fieldAggIgnoreRetract(String fieldName) {
        return options.get(
                key(FIELDS_PREFIX + "." + fieldName + "." + IGNORE_RETRACT)
                        .booleanType()
                        .defaultValue(false));
    }

    // 获取字段嵌套更新聚合键
    public List<String> fieldNestedUpdateAggNestedKey(String fieldName) {
        String keyString =
                options.get(
                        key(FIELDS_PREFIX + "." + fieldName + "." + NESTED_KEY)
                                .stringType()
                                .noDefaultValue());
        if (keyString == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(keyString.split(","));
    }

    // 判断字段聚合是否去重
    public boolean fieldCollectAggDistinct(String fieldName) {
        return options.get(
                key(FIELDS_PREFIX + "." + fieldName + "." + DISTINCT)
                        .booleanType()
                        .defaultValue(false));
    }

    // 获取字段列表聚合分隔符
    public String fieldListAggDelimiter(String fieldName) {
        return options.get(
                key(FIELDS_PREFIX + "." + fieldName + "." + LIST_AGG_DELIMITER)
                        .stringType()
                        .defaultValue(","));
    }

    // 获取文件压缩方式
    @Nullable
    public String fileCompression() {
        return options.get(FILE_COMPRESSION);
    }

    // 获取文件读取器异步阈值
    public MemorySize fileReaderAsyncThreshold() {
        return options.get(FILE_READER_ASYNC_THRESHOLD);
    }

    // 获取快照保留最小数量
    public int snapshotNumRetainMin() {
        return options.get(SNAPSHOT_NUM_RETAINED_MIN);
    }

    // 获取快照保留最大数量
    public int snapshotNumRetainMax() {
        return options.get(SNAPSHOT_NUM_RETAINED_MAX);
    }

    // 获取快照保留时间
    public Duration snapshotTimeRetain() {
        return options.get(SNAPSHOT_TIME_RETAINED);
    }

    // 获取变更日志保留最小数量
    public int changelogNumRetainMin() {
        return options.getOptional(CHANGELOG_NUM_RETAINED_MIN)
                .orElse(options.get(SNAPSHOT_NUM_RETAINED_MIN));
    }

    // 获取变更日志保留最大数量
    public int changelogNumRetainMax() {
        return options.getOptional(CHANGELOG_NUM_RETAINED_MAX)
                .orElse(options.get(SNAPSHOT_NUM_RETAINED_MAX));
    }

    // 获取变更日志保留时间
    public Duration changelogTimeRetain() {
        return options.getOptional(CHANGELOG_TIME_RETAINED)
                .orElse(options.get(SNAPSHOT_TIME_RETAINED));
    }

    // 判断变更日志生命周期是否解耦
    public boolean changelogLifecycleDecoupled() {
        return changelogNumRetainMax() > snapshotNumRetainMax()
                || changelogTimeRetain().compareTo(snapshotTimeRetain()) > 0
                || changelogNumRetainMin() > snapshotNumRetainMin();
    }

    // 获取快照过期执行模式
    public ExpireExecutionMode snapshotExpireExecutionMode() {
        return options.get(SNAPSHOT_EXPIRE_EXECUTION_MODE);
    }

    // 获取快照过期最大限制
    public int snapshotExpireLimit() {
        return options.get(SNAPSHOT_EXPIRE_LIMIT);
    }

    // 获取是否清理空目录
    public boolean cleanEmptyDirectories() {
        return options.get(SNAPSHOT_CLEAN_EMPTY_DIRECTORIES);
    }

    // 获取删除文件线程数
    public int deleteFileThreadNum() {
        return options.getOptional(DELETE_FILE_THREAD_NUM)
                .orElseGet(() -> Runtime.getRuntime().availableProcessors());
    }

    // 获取是否在结束输入时检查分区过期
    public boolean endInputCheckPartitionExpire() {
        return options.get(END_INPUT_CHECK_PARTITION_EXPIRE);
    }

    // 获取过期配置
    public ExpireConfig expireConfig() {
        return ExpireConfig.builder()
                .snapshotRetainMax(snapshotNumRetainMax())
                .snapshotRetainMin(snapshotNumRetainMin())
                .snapshotTimeRetain(snapshotTimeRetain())
                .snapshotMaxDeletes(snapshotExpireLimit())
                .changelogRetainMax(options.getOptional(CHANGELOG_NUM_RETAINED_MAX).orElse(null))
                .changelogRetainMin(options.getOptional(CHANGELOG_NUM_RETAINED_MIN).orElse(null))
                .changelogTimeRetain(options.getOptional(CHANGELOG_TIME_RETAINED).orElse(null))
                .changelogMaxDeletes(snapshotExpireLimit())
                .build();
    }

    // 获取清单文件合并最小数量
    public int manifestMergeMinCount() {
        return options.get(MANIFEST_MERGE_MIN_COUNT);
    }

    // 获取合并引擎
    public MergeEngine mergeEngine() {
        return options.get(MERGE_ENGINE);
    }

    // 获取是否忽略删除
    public boolean ignoreDelete() {
        return options.get(IGNORE_DELETE);
    }

    // 获取排序引擎
    public SortEngine sortEngine() {
        return options.get(SORT_ENGINE);
    }

    // 获取排序溢出阈值
    public int sortSpillThreshold() {
        Integer maxSortedRunNum = options.get(SORT_SPILL_THRESHOLD);
        if (maxSortedRunNum == null) {
            maxSortedRunNum = MathUtils.incrementSafely(numSortedRunStopTrigger());
        }
        checkArgument(maxSortedRunNum > 1, "排序溢出阈值不能小于 2。");
        return maxSortedRunNum;
    }

    // 获取目标拆分大小
    public long splitTargetSize() {
        return options.get(SOURCE_SPLIT_TARGET_SIZE).getBytes();
    }

    // 获取拆分打开文件成本
    public long splitOpenFileCost() {
        return options.get(SOURCE_SPLIT_OPEN_FILE_COST).getBytes();
    }

    // 获取写入缓冲区大小
    public long writeBufferSize() {
        return options.get(WRITE_BUFFER_SIZE).getBytes();
    }

    // 判断写入缓冲区是否可溢出
    public boolean writeBufferSpillable(boolean usingObjectStore, boolean isStreaming) {
        // 如果不是流式模式，默认启用溢出
        return options.getOptional(WRITE_BUFFER_SPILLABLE).orElse(usingObjectStore || !isStreaming);
    }

    // 获取写入缓冲区溢出磁盘大小
    public MemorySize writeBufferSpillDiskSize() {
        return options.get(WRITE_BUFFER_MAX_DISK_SIZE);
    }

    // 判断是否使用写入缓冲区用于追加
    public boolean useWriteBufferForAppend() {
        return options.get(WRITE_BUFFER_FOR_APPEND);
    }

    // 获取写入最大写入器数量
    public int writeMaxWritersToSpill() {
        return options.get(WRITE_MAX_WRITERS_TO_SPILL);
    }

    // 获取排序溢出缓冲区大小
    public long sortSpillBufferSize() {
        return options.get(SORT_SPILL_BUFFER_SIZE).getBytes();
    }

    // 获取排序溢出压缩选项
    public CompressOptions spillCompressOptions() {
        return new CompressOptions(
                options.get(SPILL_COMPRESSION), options.get(SPILL_COMPRESSION_ZSTD_LEVEL));
    }

    // 获取查找压缩选项
    public CompressOptions lookupCompressOptions() {
        return new CompressOptions(
                options.get(LOOKUP_CACHE_SPILL_COMPRESSION),
                options.get(SPILL_COMPRESSION_ZSTD_LEVEL));
    }

    // 获取持续发现间隔
    public Duration continuousDiscoveryInterval() {
        return options.get(CONTINUOUS_DISCOVERY_INTERVAL);
    }

    // 获取扫描每任务最大拆分数量
    public int scanSplitMaxPerTask() {
        return options.get(SCAN_MAX_SPLITS_PER_TASK);
    }

    // 获取本地排序最大文件句柄数量
    public int localSortMaxNumFileHandles() {
        return options.get(LOCAL_SORT_MAX_NUM_FILE_HANDLES);
    }

    // 获取内存页大小
    public int pageSize() {
        return (int) options.get(PAGE_SIZE).getBytes();
    }

    // 获取缓存页大小
    public int cachePageSize() {
        return (int) options.get(CACHE_PAGE_SIZE).getBytes();
    }

    // 获取查找本地文件类型
    public LookupLocalFileType lookupLocalFileType() {
        return options.get(LOOKUP_LOCAL_FILE_TYPE);
    }

    // 获取查找缓存最大内存大小
    public MemorySize lookupCacheMaxMemory() {
        return options.get(LOOKUP_CACHE_MAX_MEMORY_SIZE);
    }

    // 获取目标文件大小
    public long targetFileSize(boolean hasPrimaryKey) {
        return options.getOptional(TARGET_FILE_SIZE)
                .orElse(hasPrimaryKey ? VALUE_128_MB : VALUE_256_MB)
                .getBytes();
    }

    public long compactionFileSize(boolean hasPrimaryKey) {
        // file size to join the compaction, we don't process on middle file size to avoid
        // compact a same file twice (the compression is not calculate so accurately. the output
        // file maybe be less than target file generated by rolling file write).
        return targetFileSize(hasPrimaryKey) / 10 * 7;
    }

    public int numSortedRunCompactionTrigger() {
        return options.get(NUM_SORTED_RUNS_COMPACTION_TRIGGER);
    }

    @Nullable
    public Duration optimizedCompactionInterval() {
        return options.get(COMPACTION_OPTIMIZATION_INTERVAL);
    }

    public int numSortedRunStopTrigger() {
        Integer stopTrigger = options.get(NUM_SORTED_RUNS_STOP_TRIGGER);
        if (stopTrigger == null) {
            stopTrigger = MathUtils.addSafely(numSortedRunCompactionTrigger(), 3);
        }
        return Math.max(numSortedRunCompactionTrigger(), stopTrigger);
    }

    public int numLevels() {
        // By default, this ensures that the compaction does not fall to level 0, but at least to
        // level 1
        Integer numLevels = options.get(NUM_LEVELS);
        if (numLevels == null) {
            numLevels = MathUtils.incrementSafely(numSortedRunCompactionTrigger());
        }
        return numLevels;
    }

    public boolean commitForceCompact() {
        return options.get(COMMIT_FORCE_COMPACT);
    }

    public int maxSizeAmplificationPercent() {
        return options.get(COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT);
    }

    public int sortedRunSizeRatio() {
        return options.get(COMPACTION_SIZE_RATIO);
    }

    // 获取合并最小文件数量
    public int compactionMinFileNum() {
        return options.get(COMPACTION_MIN_FILE_NUM);
    }

    public Optional<Integer> compactionMaxFileNum() {
        return options.getOptional(COMPACTION_MAX_FILE_NUM);
    }

    public long dynamicBucketTargetRowNum() {
        return options.get(DYNAMIC_BUCKET_TARGET_ROW_NUM);
    }

    public ChangelogProducer changelogProducer() {
        return options.get(CHANGELOG_PRODUCER);
    }


    // 获取是否需要查找
    public boolean needLookup() {
        return lookupStrategy().needLookup;
    }

    // 获取查找策略
    public LookupStrategy lookupStrategy() {
        return LookupStrategy.from(
                mergeEngine().equals(MergeEngine.FIRST_ROW),
                changelogProducer().equals(ChangelogProducer.LOOKUP),
                deletionVectorsEnabled(),
                options.get(FORCE_LOOKUP));
    }

    // 判断是否去重变更日志
    public boolean changelogRowDeduplicate() {
        return options.get(CHANGELOG_PRODUCER_ROW_DEDUPLICATE);
    }

    // 判断是否按分区计划排序
    public boolean scanPlanSortPartition() {
        return options.get(SCAN_PLAN_SORT_PARTITION);
    }

    // 获取启动模式
    public StartupMode startupMode() {
        return startupMode(options);
    }

    // 静态方法：获取启动模式
    public static StartupMode startupMode(Options options) {
        StartupMode mode = options.get(SCAN_MODE);
        if (mode == StartupMode.DEFAULT) {
            if (options.getOptional(SCAN_TIMESTAMP_MILLIS).isPresent()
                    || options.getOptional(SCAN_TIMESTAMP).isPresent()) {
                return StartupMode.FROM_TIMESTAMP;
            } else if (options.getOptional(SCAN_SNAPSHOT_ID).isPresent()
                    || options.getOptional(SCAN_TAG_NAME).isPresent()
                    || options.getOptional(SCAN_WATERMARK).isPresent()
                    || options.getOptional(SCAN_VERSION).isPresent()) {
                return StartupMode.FROM_SNAPSHOT;
            } else if (options.getOptional(SCAN_FILE_CREATION_TIME_MILLIS).isPresent()) {
                return StartupMode.FROM_FILE_CREATION_TIME;
            } else if (options.getOptional(INCREMENTAL_BETWEEN).isPresent()
                    || options.getOptional(INCREMENTAL_BETWEEN_TIMESTAMP).isPresent()) {
                return StartupMode.INCREMENTAL;
            } else {
                return StartupMode.LATEST_FULL;
            }
        } else if (mode == StartupMode.FULL) {
            return StartupMode.LATEST_FULL;
        } else {
            return mode;
        }
    }

    // 获取扫描时间戳毫秒
    public Long scanTimestampMills() {
        String timestampStr = scanTimestamp();
        Long timestampMillis = options.get(SCAN_TIMESTAMP_MILLIS);
        if (timestampMillis == null && timestampStr != null) {
            return DateTimeUtils.parseTimestampData(timestampStr, 3, TimeZone.getDefault())
                    .getMillisecond();
        }
        return timestampMillis;
    }

    // 获取扫描时间戳
    public String scanTimestamp() {
        return options.get(SCAN_TIMESTAMP);
    }

    // 获取扫描水位线
    public Long scanWatermark() {
        return options.get(SCAN_WATERMARK);
    }

    // 获取扫描文件创建时间毫秒
    public Long scanFileCreationTimeMills() {
        return options.get(SCAN_FILE_CREATION_TIME_MILLIS);
    }

    // 获取扫描有界水位线
    public Long scanBoundedWatermark() {
        return options.get(SCAN_BOUNDED_WATERMARK);
    }

    // 获取扫描快照 ID
    public Long scanSnapshotId() {
        return options.get(SCAN_SNAPSHOT_ID);
    }

    // 获取扫描标签名称
    public String scanTagName() {
        return options.get(SCAN_TAG_NAME);
    }

    // 获取扫描版本
    public String scanVersion() {
        return options.get(SCAN_VERSION);
    }

    // 获取增量之间配置
    public Pair<String, String> incrementalBetween() {
        String str = options.get(INCREMENTAL_BETWEEN);
        if (str == null) {
            str = options.get(INCREMENTAL_BETWEEN_TIMESTAMP);
            if (str == null) {
                return null;
            }
        }

        String[] split = str.split(",");
        if (split.length != 2) {
            throw new IllegalArgumentException(
                    "incremental-between 或 incremental-between-timestamp 必须指定开始（不包含）和结束快照或时间戳，"
                            + "例如，'incremental-between'='5,10' 表示快照 5 和快照 10 之间的更改。但当前值为："
                            + str);
        }
        return Pair.of(split[0], split[1]);
    }

    // 获取增量之间扫描模式
    public IncrementalBetweenScanMode incrementalBetweenScanMode() {
        return options.get(INCREMENTAL_BETWEEN_SCAN_MODE);
    }

    // 获取扫描清单并行度
    public Integer scanManifestParallelism() {
        return options.get(SCAN_MANIFEST_PARALLELISM);
    }

    // 获取流式读取延迟
    public Duration streamingReadDelay() {
        return options.get(STREAMING_READ_SNAPSHOT_DELAY);
    }

    // 获取动态桶初始桶数
    public Integer dynamicBucketInitialBuckets() {
        return options.get(DYNAMIC_BUCKET_INITIAL_BUCKETS);
    }

    // 获取动态桶分配器并行度
    public Integer dynamicBucketAssignerParallelism() {
        return options.get(DYNAMIC_BUCKET_ASSIGNER_PARALLELISM);
    }

    // 获取序列号字段
    public List<String> sequenceField() {
        return options.getOptional(SEQUENCE_FIELD)
                .map(s -> Arrays.asList(s.split(",")))
                .orElse(Collections.emptyList());
    }

    // 获取行类型字段
    public Optional<String> rowkindField() {
        return options.getOptional(ROWKIND_FIELD);
    }

    // 判断是否仅写入
    public boolean writeOnly() {
        return options.get(WRITE_ONLY);
    }

    // 判断是否流式读取覆盖
    public boolean streamingReadOverwrite() {
        return options.get(STREAMING_READ_OVERWRITE);
    }

    // 判断是否动态分区覆盖
    public boolean dynamicPartitionOverwrite() {
        return options.get(DYNAMIC_PARTITION_OVERWRITE);
    }

    // 获取分区过期时间
    public Duration partitionExpireTime() {
        return options.get(PARTITION_EXPIRATION_TIME);
    }

    // 获取分区过期检查间隔
    public Duration partitionExpireCheckInterval() {
        return options.get(PARTITION_EXPIRATION_CHECK_INTERVAL);
    }

    // 获取分区过期策略
    public PartitionExpireStrategy partitionExpireStrategy() {
        return options.get(PARTITION_EXPIRATION_STRATEGY);
    }

    // 获取分区时间戳格式化器
    public String partitionTimestampFormatter() {
        return options.get(PARTITION_TIMESTAMP_FORMATTER);
    }

    // 获取分区时间戳模式
    public String partitionTimestampPattern() {
        return options.get(PARTITION_TIMESTAMP_PATTERN);
    }

    // 获取消费者 ID
    public String consumerId() {
        String consumerId = options.get(CONSUMER_ID);
        if (consumerId != null && consumerId.isEmpty()) {
            throw new RuntimeException("消费者 ID 不能为空字符串。");
        }
        return consumerId;
    }

    // 获取流式读取类型
    public static StreamingReadMode streamReadType(Options options) {
        return options.get(STREAMING_READ_MODE);
    }

    // 获取消费者过期时间
    public Duration consumerExpireTime() {
        return options.get(CONSUMER_EXPIRATION_TIME);
    }

    // 判断消费者是否忽略进度
    public boolean consumerIgnoreProgress() {
        return options.get(CONSUMER_IGNORE_PROGRESS);
    }

    // 判断是否在元存储中将表创建为分区表
    public boolean partitionedTableInMetastore() {
        return options.get(METASTORE_PARTITIONED_TABLE);
    }

    // 获取标签到分区字段
    @Nullable
    public String tagToPartitionField() {
        return options.get(METASTORE_TAG_TO_PARTITION);
    }

    // 获取标签到分区预览模式
    public TagCreationMode tagToPartitionPreview() {
        return options.get(METASTORE_TAG_TO_PARTITION_PREVIEW);
    }

    // 获取标签创建模式
    public TagCreationMode tagCreationMode() {
        return options.get(TAG_AUTOMATIC_CREATION);
    }

    // 获取标签创建周期
    public TagCreationPeriod tagCreationPeriod() {
        return options.get(TAG_CREATION_PERIOD);
    }

    // 获取标签创建延迟
    public Duration tagCreationDelay() {
        return options.get(TAG_CREATION_DELAY);
    }

    // 获取标签周期格式化器
    public TagPeriodFormatter tagPeriodFormatter() {
        return options.get(TAG_PERIOD_FORMATTER);
    }

    // 获取标签保留最大数量
    @Nullable
    public Integer tagNumRetainedMax() {
        return options.get(TAG_NUM_RETAINED_MAX);
    }

    // 获取标签默认保留时间
    public Duration tagDefaultTimeRetained() {
        return options.get(TAG_DEFAULT_TIME_RETAINED);
    }

    // 判断标签是否自动完成
    public boolean tagAutomaticCompletion() {
        return options.get(TAG_AUTOMATIC_COMPLETION);
    }

    // 获取快照水位线空闲超时
    public Duration snapshotWatermarkIdleTimeout() {
        return options.get(SNAPSHOT_WATERMARK_IDLE_TIMEOUT);
    }

    // 获取沉淀水位线时区
    public String sinkWatermarkTimeZone() {
        return options.get(SINK_WATERMARK_TIME_ZONE);
    }

    // 判断提交时是否强制创建快照
    public boolean forceCreatingSnapshot() {
        return options.get(COMMIT_FORCE_CREATE_SNAPSHOT);
    }

    // 获取字段默认值
    public Map<String, String> getFieldDefaultValues() {
        Map<String, String> defaultValues = new HashMap<>();
        String fieldPrefix = FIELDS_PREFIX + ".";
        String defaultValueSuffix = "." + DEFAULT_VALUE_SUFFIX;
        for (Map.Entry<String, String> option : options.toMap().entrySet()) {
            String key = option.getKey();
            if (key != null && key.startsWith(fieldPrefix) && key.endsWith(defaultValueSuffix)) {
                String fieldName = key.replace(fieldPrefix, "").replace(defaultValueSuffix, "");
                defaultValues.put(fieldName, option.getValue());
            }
        }
        return defaultValues;
    }

    // 获取提交回调
    public Map<String, String> commitCallbacks() {
        return callbacks(COMMIT_CALLBACKS, COMMIT_CALLBACK_PARAM);
    }

    // 获取标签回调
    public Map<String, String> tagCallbacks() {
        return callbacks(TAG_CALLBACKS, TAG_CALLBACK_PARAM);
    }

    // 回调方法
    private Map<String, String> callbacks(
            ConfigOption<String> callbacks, ConfigOption<String> callbackParam) {
        Map<String, String> result = new HashMap<>();
        for (String className : options.get(callbacks).split(",")) {
            className = className.trim();
            if (className.length() == 0) {
                continue;
            }

            String originParamKey = callbackParam.key().replace("#", className);
            String param = options.get(originParamKey);
            if (param == null) {
                param = options.get(originParamKey.toLowerCase(Locale.ROOT));
            }
            result.put(className, param);
        }
        return result;
    }

    // 判断是否启用本地合并
    public boolean localMergeEnabled() {
        return options.get(LOCAL_MERGE_BUFFER_SIZE) != null;
    }

    // 获取本地合并缓冲区大小
    public long localMergeBufferSize() {
        return options.get(LOCAL_MERGE_BUFFER_SIZE).getBytes();
    }

    // 获取跨分区更新索引 TTL
    public Duration crossPartitionUpsertIndexTtl() {
        return options.get(CROSS_PARTITION_UPSERT_INDEX_TTL);
    }

    // 获取跨分区更新引导并行度
    public int crossPartitionUpsertBootstrapParallelism() {
        return options.get(CROSS_PARTITION_UPSERT_BOOTSTRAP_PARALLELISM);
    }

    // 获取可变长度贡献
    public int varTypeSize() {
        return options.get(ZORDER_VAR_LENGTH_CONTRIBUTION);
    }

    // 判断是否启用删除向量
    public boolean deletionVectorsEnabled() {
        return options.get(DELETION_VECTORS_ENABLED);
    }

    // 获取删除向量索引文件目标大小
    public MemorySize deletionVectorIndexFileTargetSize() {
        return options.get(DELETION_VECTOR_INDEX_FILE_TARGET_SIZE);
    }

    // 获取文件索引选项
    public FileIndexOptions indexColumnsOptions() {
        return new FileIndexOptions(this);
    }

    // 获取文件索引在清单中的阈值
    public long fileIndexInManifestThreshold() {
        return options.get(FILE_INDEX_IN_MANIFEST_THRESHOLD).getBytes();
    }

    // 判断文件索引读取是否启用
    public boolean fileIndexReadEnabled() {
        return options.get(FILE_INDEX_READ_ENABLED);
    }

    // 判断删除是否强制生成变更日志
    public boolean deleteForceProduceChangelog() {
        return options.get(DELETION_FORCE_PRODUCE_CHANGELOG);
    }

    // 获取记录级别过期时间
    @Nullable
    public Duration recordLevelExpireTime() {
        return options.get(RECORD_LEVEL_EXPIRE_TIME);
    }

    // 获取记录级别时间字段
    @Nullable
    public String recordLevelTimeField() {
        return options.get(RECORD_LEVEL_TIME_FIELD);
    }

    // 获取记录级别时间字段类型
    @Nullable
    public TimeFieldType recordLevelTimeFieldType() {
        return options.get(RECORD_LEVEL_TIME_FIELD_TYPE);
    }

    // 判断准备提交时是否等待压缩
    public boolean prepareCommitWaitCompaction() {
        if (!needLookup()) {
            return false;
        }

        return options.get(LOOKUP_WAIT);
    }

    // 判断是否启用异步文件写入
    public boolean asyncFileWrite() {
        return options.get(ASYNC_FILE_WRITE);
    }

    // 判断元数据是否与 Iceberg 兼容
    public boolean metadataIcebergCompatible() {
        return options.get(METADATA_ICEBERG_COMPATIBLE);
    }

    // 枚举：合并引擎
    public enum MergeEngine implements DescribedEnum {
        DEDUPLICATE("deduplicate", "去重并保留最后一条记录。"),

        PARTIAL_UPDATE("partial-update", "部分更新非空字段。"),

        AGGREGATE("aggregation", "对具有相同主键的字段进行聚合。"),

        FIRST_ROW("first-row", "去重并保留第一条记录。");

        private final String value;
        private final String description;

        MergeEngine(String value, String description) {
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

    // 枚举：启动模式
    public enum StartupMode implements DescribedEnum {
        DEFAULT(
                "default",
                "根据其他表属性确定实际启动模式。"
                        + "如果设置了 'scan.timestamp-millis'，实际启动模式将为 'from-timestamp'，"
                        + "如果设置了 'scan.snapshot-id' 或 'scan.tag-name'，实际启动模式将为 'from-snapshot'。"
                        + "否则实际启动模式将为 'latest-full'。"),

        LATEST_FULL(
                "latest-full",
                "对于流式数据源，首次启动时生成表上的最新快照，并继续读取最新更改。"
                        + "对于批处理数据源，仅生成最新快照，不读取新更改。"),

        FULL("full", "已废弃。与 'latest-full' 相同。"),

        LATEST(
                "latest",
                "对于流式数据源，从首次启动开始连续读取最新更改，不生成初始快照。"
                        + "对于批处理数据源，行为与 'latest-full' 启动模式相同。"),

        COMPACTED_FULL(
                "compacted-full",
                "对于流式数据源，首次启动时生成表上最新压缩后的快照，并继续读取最新更改。"
                        + "对于批处理数据源，仅生成最新压缩后的快照，不读取新更改。"
                        + "选择 'scheduled full-compaction' 启用时，将选择快照。"),

        FROM_TIMESTAMP(
                "from-timestamp",
                "对于流式数据源，从 'scan.timestamp-millis' 指定的时间戳开始连续读取更改，不生成初始快照。"
                        + "对于批处理数据源，生成 'scan.timestamp-millis' 指定的时间戳的快照，不读取新更改。"),

        FROM_FILE_CREATION_TIME(
                "from-file-creation-time",
                "对于流式和批处理数据源，生成并过滤数据文件的创建时间的快照。"
                        + "对于流式数据源，首次启动时生成快照，并继续读取最新更改。"),

        FROM_SNAPSHOT(
                "from-snapshot",
                "对于流式数据源，从 'scan.snapshot-id' 指定的快照开始连续读取更改，不生成初始快照。"
                        + "对于批处理数据源，生成 'scan.snapshot-id' 或 'scan.tag-name' 指定的快照，不读取新更改。"),

        FROM_SNAPSHOT_FULL(
                "from-snapshot-full",
                "对于流式数据源，首次启动时生成 'scan.snapshot-id' 指定的快照上的快照，并连续读取更改。"
                        + "对于批处理数据源，生成 'scan.snapshot-id' 指定的快照，不读取新更改。"),

        INCREMENTAL(
                "incremental",
                "读取开始和结束快照或时间戳之间的增量更改。");

        private final String value;
        private final String description;

        StartupMode(String value, String description) {
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

    // 枚举：日志一致性模式
    public enum LogConsistency implements DescribedEnum {
        TRANSACTIONAL(
                "transactional",
                "只有在检查点之后的数据才能被读取，延迟取决于检查点间隔。"),

        EVENTUAL(
                "eventual",
                "立即可见数据，您可能会看到一些中间状态，但最终会产生正确的结果，仅适用于具有主键的表。");

        private final String value;
        private final String description;

        LogConsistency(String value, String description) {
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

    // 枚举：日志变更日志模式
    /**
     * 指定表的日志变更日志模式。
     * - AUTO：对于具有主键的表，自动模式；对于没有主键的表，全部模式。
     * - ALL：日志系统存储所有更改，包括 UPDATE_BEFORE。
     * - UPSERT：日志系统不存储 UPDATE_BEFORE 更改，依赖于状态生成所需的 update_before。
     */
    public enum LogChangelogMode implements DescribedEnum {
        AUTO("auto", "Upsert for table with primary key, all for table without primary key."),
        ALL("all", "The log system stores all changes including UPDATE_BEFORE."),
        UPSERT(
                "upsert",
                "The log system does not store the UPDATE_BEFORE changes, the log consumed job"
                        + " will automatically add the normalized node, relying on the state"
                        + " to generate the required update_before.");

        private final String value;
        private final String description;

        LogChangelogMode(String value, String description) {
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

    // 枚举：变更日志生产者
    /**
     * 指定表的变更日志生产者。
     * - NONE：无变更日志文件。
     * - INPUT：在刷新内存表时，双写到变更日志文件，变更日志来自输入。
     * - FULL_COMPACTION：每次完全合并时，生成变更日志文件。
     * - LOOKUP：在提交数据写入之前，通过 'lookup' 生成变更日志文件。
     */
    public enum ChangelogProducer implements DescribedEnum {
        NONE("none", "No changelog file."),
        INPUT(
                "input",
                "Double write to a changelog file when flushing memory table, the changelog is from input."),
        FULL_COMPACTION("full-compaction", "Generate changelog files with each full compaction."),
        LOOKUP(
                "lookup",
                "Generate changelog files through 'lookup' before committing the data writing.");

        private final String value;
        private final String description;

        ChangelogProducer(String value, String description) {
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

    // 枚举：流式读取模式
    /**
     * 指定流式读取的模式。
     * - LOG：从表的日志存储中读取数据。
     * - FILE：从表的文件存储中读取数据。
     */
    public enum StreamingReadMode implements DescribedEnum {
        LOG("log", "Read from the data of table log store."),
        FILE("file", "Read from the data of table file store.");

        private final String value;
        private final String description;

        StreamingReadMode(String value, String description) {
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

        public String getValue() {
            return value;
        }

        // 根据值获取 StreamingReadMode 枚举实例
        @VisibleForTesting
        public static StreamingReadMode fromValue(String value) {
            for (StreamingReadMode formatType : StreamingReadMode.values()) {
                if (formatType.value.equals(value)) {
                    return formatType;
                }
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid format type %s, only support [%s]",
                            value,
                            StringUtils.join(
                                    Arrays.stream(StreamingReadMode.values()).iterator(), ",")));
        }
    }

    // 枚举：流式扫描模式
    /**
     * 内部流式扫描模式，用于满足一些内部需求。
     * - NONE：没有要求。
     * - COMPACT_BUCKET_TABLE：传统桶表的合并。
     * - FILE_MONITOR：监控数据文件的变化。
     */
    public enum StreamScanMode implements DescribedEnum {
        NONE("none", "No requirement."),
        COMPACT_BUCKET_TABLE("compact-bucket-table", "Compaction for traditional bucket table."),
        FILE_MONITOR("file-monitor", "Monitor data file changes.");

        private final String value;
        private final String description;

        StreamScanMode(String value, String description) {
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

        public String getValue() {
            return value;
        }
    }

    // 枚举：增量扫描模式
    /**
     * 指定增量扫描的类型。
     * - AUTO：如果表生成变更日志文件，则扫描变更日志文件；否则，扫描新更改的文件。
     * - DELTA：扫描快照之间的新更改文件。
     * - CHANGELOG：扫描快照之间的变更日志文件。
     */
    public enum IncrementalBetweenScanMode implements DescribedEnum {
        AUTO(
                "auto",
                "Scan changelog files for the table which produces changelog files. Otherwise, scan newly changed files."),
        DELTA("delta", "Scan newly changed files between snapshots."),
        CHANGELOG("changelog", "Scan changelog files between snapshots.");

        private final String value;
        private final String description;

        IncrementalBetweenScanMode(String value, String description) {
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

        public String getValue() {
            return value;
        }

        @VisibleForTesting
        public static IncrementalBetweenScanMode fromValue(String value) {
            for (IncrementalBetweenScanMode formatType : IncrementalBetweenScanMode.values()) {
                if (formatType.value.equals(value)) {
                    return formatType;
                }
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid format type %s, only support [%s]",
                            value,
                            StringUtils.join(
                                    Arrays.stream(IncrementalBetweenScanMode.values()).iterator(),
                                    ",")));
        }
    }

    // 方法：设置默认值
    /**
     * 通过给定的 {@link Options} 设置 {@link CoreOptions} 的默认值。
     *
     * @param options 需要设置默认值的选项
     */
    public static void setDefaultValues(Options options) {
        if (options.contains(SCAN_TIMESTAMP_MILLIS) && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.FROM_TIMESTAMP);
        }

        if (options.contains(SCAN_TIMESTAMP) && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.FROM_TIMESTAMP);
        }

        if (options.contains(SCAN_FILE_CREATION_TIME_MILLIS) && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.FROM_FILE_CREATION_TIME);
        }

        if (options.contains(SCAN_SNAPSHOT_ID) && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.FROM_SNAPSHOT);
        }

        if ((options.contains(INCREMENTAL_BETWEEN_TIMESTAMP)
                || options.contains(INCREMENTAL_BETWEEN))
                && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.INCREMENTAL);
        }
    }

    // 方法：获取所有配置选项
    /**
     * 获取所有配置选项。
     *
     * @return 所有配置选项的列表
     */
    public static List<ConfigOption<?>> getOptions() {
        final Field[] fields = CoreOptions.class.getFields();
        final List<ConfigOption<?>> list = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if (ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    list.add((ConfigOption<?>) field.get(CoreOptions.class));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return list;
    }

    // 常量：不可变选项
    /**
     * CoreOptions 中不可变的配置选项。
     */
    public static final Set<String> IMMUTABLE_OPTIONS =
            Arrays.stream(CoreOptions.class.getFields())
                    .filter(
                            f ->
                                    ConfigOption.class.isAssignableFrom(f.getType())
                                            && f.getAnnotation(Immutable.class) != null)
                    .map(
                            f -> {
                                try {
                                    return ((ConfigOption<?>) f.get(CoreOptions.class)).key();
                                } catch (IllegalAccessException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .collect(Collectors.toSet());

    // 枚举：排序引擎
    /**
     * 指定具有主键的表的排序引擎。
     * - MIN_HEAP：使用最小堆进行多路排序。
     * - LOSER_TREE：使用失败者树进行多路排序，相比堆排序，失败者树比较次数更少，效率更高。
     */
    public enum SortEngine implements DescribedEnum {
        MIN_HEAP("min-heap", "Use min-heap for multiway sorting."),
        LOSER_TREE(
                "loser-tree",
                "Use loser-tree for multiway sorting. Compared with heapsort, loser-tree has fewer comparisons and is more efficient.");

        private final String value;
        private final String description;

        SortEngine(String value, String description) {
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

    // 枚举：标签创建模式
    /**
     * 指定标签的创建模式。
     * - NONE：不自动创建标签。
     * - PROCESS_TIME：基于机器时间，处理时间超过周期时间加延迟后创建标签。
     * - WATERMARK：基于输入的水印，水印超过周期时间加延迟后创建标签。
     * - BATCH：在批处理场景中，任务完成后生成与当前快照对应的标签。
     */
    public enum TagCreationMode implements DescribedEnum {
        NONE("none", "No automatically created tags."),
        PROCESS_TIME(
                "process-time",
                "Based on the time of the machine, create TAG once the processing time passes period time plus delay."),
        WATERMARK(
                "watermark",
                "Based on the watermark of the input, create TAG once the watermark passes period time plus delay."),
        BATCH(
                "batch",
                "In the batch processing scenario, the tag corresponding to the current snapshot is generated after the task is completed.");
        private final String value;
        private final String description;

        TagCreationMode(String value, String description) {
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

    // 枚举：标签周期格式化器
    /**
     * 指定标签周期的格式。
     * - WITH_DASHES：日期和小时带有连字符，例如 'yyyy-MM-dd HH'。
     * - WITHOUT_DASHES：日期和小时没有连字符，例如 'yyyyMMdd HH'。
     */
    public enum TagPeriodFormatter implements DescribedEnum {
        WITH_DASHES("with_dashes", "Dates and hours with dashes, e.g., 'yyyy-MM-dd HH'"),
        WITHOUT_DASHES("without_dashes", "Dates and hours without dashes, e.g., 'yyyyMMdd HH'");

        private final String value;
        private final String description;

        TagPeriodFormatter(String value, String description) {
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

    // 枚举：标签创建周期
    /**
     * 指定标签的创建周期。
     * - DAILY：每天生成一个标签。
     * - HOURLY：每小时生成一个标签。
     * - TWO_HOURS：每两小时生成一个标签。
     */
    public enum TagCreationPeriod implements DescribedEnum {
        DAILY("daily", "Generate a tag every day."),
        HOURLY("hourly", "Generate a tag every hour."),
        TWO_HOURS("two-hours", "Generate a tag every two hours.");

        private final String value;
        private final String description;

        TagCreationPeriod(String value, String description) {
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

    // 枚举：过期执行模式
    /**
     * 指定过期的执行模式。
     * - SYNC：同步执行过期。如果文件太多，可能会花费很长时间并阻塞流处理。
     * - ASYNC：异步执行过期。如果快照的生成大于删除，将会有文件积压。
     */
    public enum ExpireExecutionMode implements DescribedEnum {
        SYNC(
                "sync",
                "Execute expire synchronously. If there are too many files, it may take a long time and block stream processing."),
        ASYNC(
                "async",
                "Execute expire asynchronously. If the generation of snapshots is greater than the deletion, there will be a backlog of files.");

        private final String value;
        private final String description;

        ExpireExecutionMode(String value, String description) {
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

    // 枚举：范围策略
    /**
     * 指定范围策略。
     */
    public enum RangeStrategy {
        SIZE,
        QUANTITY
    }

    // 枚举：消费者模式
    /**
     * 指定表的消费者模式。
     * - EXACTLY_ONCE：读取器以快照粒度消费数据，并严格确保消费者记录的快照-id 是所有读取器已消费的快照-id +1。
     * - AT_LEAST_ONCE：每个读取器以不同的速率消费快照，所有读取器中最慢的消费进度将记录在消费者中。
     */
    public enum ConsumerMode implements DescribedEnum {
        EXACTLY_ONCE(
                "exactly-once",
                "Readers consume data at snapshot granularity, and strictly ensure that the snapshot-id recorded in the consumer is the snapshot-id + 1 that all readers have exactly consumed."),
        AT_LEAST_ONCE(
                "at-least-once",
                "Each reader consumes snapshots at a different rate, and the snapshot with the slowest consumption progress among all readers will be recorded in the consumer.");

        private final String value;
        private final String description;

        ConsumerMode(String value, String description) {
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

    // 枚举：分区过期策略
    /**
     * 指定分区的过期策略。
     * - VALUES_TIME：比较从分区值中提取的时间与当前时间。
     * - UPDATE_TIME：比较分区的最后更新时间与当前时间。
     */
    public enum PartitionExpireStrategy implements DescribedEnum {
        VALUES_TIME(
                "values-time",
                "This strategy compares the time extracted from the partition value with the current time."),
        UPDATE_TIME(
                "update-time",
                "This strategy compares the last update time of the partition with the current time.");

        private final String value;
        private final String description;

        PartitionExpireStrategy(String value, String description) {
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

    // 枚举：查找本地文件类型
    /**
     * 指定查找的本地文件类型。
     * - SORT：构建排序文件进行查找。
     * - HASH：构建哈希文件进行查找。
     */
    public enum LookupLocalFileType implements DescribedEnum {
        SORT("sort", "Construct a sorted file for lookup."),
        HASH("hash", "Construct a hash file for lookup.");

        private final String value;
        private final String description;

        LookupLocalFileType(String value, String description) {
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

    // 枚举：记录级别过期时间类型
    /**
     * 指定记录级别过期的时间字段类型。
     * - SECONDS_INT：以秒为单位的时间戳应为 INT 类型。
     * - MILLIS_LONG：以毫秒为单位的时间戳应为 BIGINT 类型。
     */
    public enum TimeFieldType implements DescribedEnum {
        SECONDS_INT("seconds-int", "Timestamps in seconds should be INT type."),
        MILLIS_LONG("millis-long", "Timestamps in milliseconds should be BIGINT type.");

        private final String value;
        private final String description;

        TimeFieldType(String value, String description) {
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
