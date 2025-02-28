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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.StartupMode;
import org.apache.paimon.CoreOptions.StreamingReadMode;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.Projection;
import org.apache.paimon.flink.log.LogSourceProvider;
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.flink.source.align.AlignedContinuousFileStoreSource;
import org.apache.paimon.flink.source.operator.MonitorFunction;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.apache.paimon.CoreOptions.StreamingReadMode.FILE;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * 构建用于 Flink Source 的 DataStream API。
 * 此构建器用于配置和创建 Flink 数据源，包括静态文件源和连续文件源。
 *
 * @since 0.8
 */
public class FlinkSourceBuilder {

    private final Table table; // 表对象
    private final Options conf; // 配置选项
    private final BucketMode bucketMode; // 数据桶模式
    private String sourceName; // 数据源名称
    private Boolean sourceBounded; // 标识数据源是有界还是无界
    private StreamExecutionEnvironment env; // Flink 执行环境
    @Nullable private int[][] projectedFields; // 投影字段
    @Nullable private Predicate predicate; // 谓词过滤条件
    @Nullable private LogSourceProvider logSourceProvider; // 日志源提供器
    @Nullable private Integer parallelism; // 数据源并行度
    @Nullable private Long limit; // 查询限制
    @Nullable private WatermarkStrategy<RowData> watermarkStrategy; // 水印策略
    @Nullable private DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo; // 动态分区过滤信息

    /**
     * 构造函数，初始化表对象、数据桶模式、配置选项等。
     *
     * @param table 数据表对象
     */
    public FlinkSourceBuilder(Table table) {
        this.table = table;
        this.bucketMode =
                table instanceof FileStoreTable
                        ? ((FileStoreTable) table).bucketMode() // 如果是文件存储表，则获取其数据桶模式
                        : BucketMode.HASH_FIXED; // 否则设置为固定哈希模式
        this.sourceName = table.name(); // 设置数据源名称为表名
        this.conf = Options.fromMap(table.options()); // 初始化配置选项
    }

    /**
     * 设置 Flink 执行环境。
     *
     * @param env Flink 执行环境
     * @return 当前构建器实例
     */
    public FlinkSourceBuilder env(StreamExecutionEnvironment env) {
        this.env = env;
        if (sourceBounded == null) {
            sourceBounded = !FlinkSink.isStreaming(env); // 如果未设置 sourceBounded，则根据环境是否为流模式推断
        }
        return this;
    }

    /**
     * 设置数据源名称。
     *
     * @param name 数据源名称
     * @return 当前构建器实例
     */
    public FlinkSourceBuilder sourceName(String name) {
        this.sourceName = name;
        return this;
    }

    /**
     * 设置数据源是有界还是无界。
     *
     * @param bounded 是否有界
     * @return 当前构建器实例
     */
    public FlinkSourceBuilder sourceBounded(boolean bounded) {
        this.sourceBounded = bounded;
        return this;
    }

    /**
     * 设置投影字段（一维数组形式）。
     *
     * @param projectedFields 投影字段
     * @return 当前构建器实例
     */
    public FlinkSourceBuilder projection(int[] projectedFields) {
        return projection(Projection.of(projectedFields).toNestedIndexes()); // 转换为二维投影字段
    }

    /**
     * 设置投影字段（二维数组形式）。
     *
     * @param projectedFields 投影字段
     * @return 当前构建器实例
     */
    public FlinkSourceBuilder projection(int[][] projectedFields) {
        this.projectedFields = projectedFields;
        return this;
    }

    /**
     * 设置谓词过滤条件。
     *
     * @param predicate 谓词过滤条件
     * @return 当前构建器实例
     */
    public FlinkSourceBuilder predicate(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }

    /**
     * 设置查询限制。
     *
     * @param limit 查询限制
     * @return 当前构建器实例
     */
    public FlinkSourceBuilder limit(@Nullable Long limit) {
        this.limit = limit;
        return this;
    }

    /**
     * 设置数据源并行度。
     *
     * @param parallelism 数据源并行度
     * @return 当前构建器实例
     */
    public FlinkSourceBuilder sourceParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    /**
     * 设置水印策略。
     *
     * @param watermarkStrategy 水印策略
     * @return 当前构建器实例
     */
    public FlinkSourceBuilder watermarkStrategy(
            @Nullable WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
        return this;
    }

    /**
     * 设置动态分区过滤字段。
     *
     * @param dynamicPartitionFilteringFields 动态分区过滤字段
     * @return 当前构建器实例
     */
    public FlinkSourceBuilder dynamicPartitionFilteringFields(
            List<String> dynamicPartitionFilteringFields) {
        if (dynamicPartitionFilteringFields != null && !dynamicPartitionFilteringFields.isEmpty()) {
            checkState(
                    table instanceof FileStoreTable,
                    "Only Paimon FileStoreTable supports dynamic filtering but get %s.",
                    table.getClass().getName());
            this.dynamicPartitionFilteringInfo =
                    new DynamicPartitionFilteringInfo(
                            ((FileStoreTable) table).schema().logicalPartitionType(),
                            dynamicPartitionFilteringFields); // 初始化动态分区过滤信息
        }
        return this;
    }

    /**
     * 设置日志源提供器（已废弃）。
     *
     * @param logSourceProvider 日志源提供器
     * @return 当前构建器实例
     */
    @Deprecated
    FlinkSourceBuilder logSourceProvider(LogSourceProvider logSourceProvider) {
        this.logSourceProvider = logSourceProvider;
        return this;
    }

    /**
     * 创建读取构建器。
     *
     * @return 读取构建器
     */
    private ReadBuilder createReadBuilder() {
        ReadBuilder readBuilder =
                table.newReadBuilder().withProjection(projectedFields).withFilter(predicate); // 初始化读取构建器
        if (limit != null) {
            readBuilder.withLimit(limit.intValue()); // 设置查询限制
        }
        return readBuilder;
    }

    /**
     * 构建静态文件源。
     *
     * @return 静态文件源
     */
    private DataStream<RowData> buildStaticFileSource() {
        Options options = Options.fromMap(table.options());
        return toDataStream( // 转换为数据流
                new StaticFileStoreSource(
                        createReadBuilder(),
                        limit,
                        options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_BATCH_SIZE),
                        options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE),
                        dynamicPartitionFilteringInfo)); // 创建静态文件源
    }

    /**
     * 构建连续文件源。
     *
     * @return 连续文件源
     */
    private DataStream<RowData> buildContinuousFileSource() {
        return toDataStream( // 转换为数据流
                new ContinuousFileStoreSource(
                        createReadBuilder(), table.options(), limit, bucketMode)); // 创建连续文件源
    }

    /**
     * 构建对齐连续文件源。
     *
     * @return 对齐连续文件源
     */
    private DataStream<RowData> buildAlignedContinuousFileSource() {
        assertStreamingConfigurationForAlignMode(env); // 检查流处理对齐模式的配置
        return toDataStream( // 转换为数据流
                new AlignedContinuousFileStoreSource(
                        createReadBuilder(), table.options(), limit, bucketMode)); // 创建对齐连续文件源
    }

    /**
     * 将源转换为数据流。
     *
     * @param source 源对象
     * @return 数据流
     */
    private DataStream<RowData> toDataStream(Source<RowData, ?, ?> source) {
        DataStreamSource<RowData> dataStream =
                env.fromSource(
                        source,
                        watermarkStrategy == null
                                ? WatermarkStrategy.noWatermarks()
                                : watermarkStrategy,
                        sourceName,
                        produceTypeInfo()); // 从源创建数据流
        if (parallelism != null) {
            dataStream.setParallelism(parallelism); // 设置数据流并行度
        }
        return dataStream;
    }

    /**
     * 生成类型信息。
     *
     * @return 类型信息
     */
    private TypeInformation<RowData> produceTypeInfo() {
        RowType rowType = toLogicalType(table.rowType()); // 获取逻辑行类型
        LogicalType produceType =
                Optional.ofNullable(projectedFields)
                        .map(Projection::of)
                        .map(p -> p.project(rowType))
                        .orElse(rowType); // 根据投影字段生成类型
        return InternalTypeInfo.of(produceType);
    }

    /**
     * 构建以 {@link Row} 类型为结果的数据流。
     *
     * @return 数据流
     */
    public DataStream<Row> buildForRow() {
        DataType rowType = fromLogicalToDataType(toLogicalType(table.rowType()));
        DataType[] fieldDataTypes = rowType.getChildren().toArray(new DataType[0]);
        DataFormatConverters.RowConverter converter =
                new DataFormatConverters.RowConverter(fieldDataTypes); // 创建行转换器
        DataStream<RowData> source = build();
        return source.map((MapFunction<RowData, Row>) converter::toExternal)
                .setParallelism(source.getParallelism())
                .returns(ExternalTypeInfo.of(rowType)); // 转换数据流为 Row 类型
    }

    /**
     * 构建以 {@link RowData} 类型为结果的数据流。
     *
     * @return 数据流
     */
    public DataStream<RowData> build() {
        if (env == null) {
            throw new IllegalArgumentException("StreamExecutionEnvironment should not be null.");
        }
        if (conf.contains(CoreOptions.CONSUMER_ID)
                && !conf.contains(CoreOptions.CONSUMER_EXPIRATION_TIME)) {
            throw new IllegalArgumentException(
                    "consumer.expiration-time should be specified when using consumer-id.");
        }

        if (sourceBounded) {
            return buildStaticFileSource(); // 如果是有界数据源，构建静态文件源
        }
        TableScanUtils.streamingReadingValidate(table); // 验证流式读取条件

        // 读取配置选项
        StartupMode startupMode = CoreOptions.startupMode(conf);
        StreamingReadMode streamingReadMode = CoreOptions.streamReadType(conf);

        if (logSourceProvider != null && streamingReadMode != StreamingReadMode.FILE) {
            logSourceProvider.preCreateSource(); // 预创建日志源
            if (startupMode != StartupMode.LATEST_FULL) {
                return toDataStream(logSourceProvider.createSource(null)); // 创建日志源
            } else {
                return toDataStream(
                        HybridSource.<RowData, StaticFileStoreSplitEnumerator>builder(
                                        LogHybridSourceFactory.buildHybridFirstSource(
                                                table, projectedFields, predicate))
                                .addSource(
                                        new LogHybridSourceFactory(logSourceProvider),
                                        Boundedness.CONTINUOUS_UNBOUNDED)
                                .build()); // 创建混合数据源
            }
        } else {
            if (conf.get(FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_ENABLED)) {
                return buildAlignedContinuousFileSource(); // 构建对齐连续文件源
            } else if (conf.contains(CoreOptions.CONSUMER_ID)
                    && conf.get(CoreOptions.CONSUMER_CONSISTENCY_MODE)
                    == CoreOptions.ConsumerMode.EXACTLY_ONCE) {
                return buildContinuousStreamOperator(); // 构建连续流操作符
            } else {
                return buildContinuousFileSource(); // 构建连续文件源
            }
        }
    }

    /**
     * 构建连续流操作符。
     *
     * @return 数据流
     */
    private DataStream<RowData> buildContinuousStreamOperator() {
        DataStream<RowData> dataStream;
        if (limit != null) {
            throw new IllegalArgumentException(
                    "Cannot limit streaming source, please use batch execution mode.");
        }
        dataStream =
                MonitorFunction.buildSource(
                        env,
                        sourceName,
                        produceTypeInfo(),
                        createReadBuilder(),
                        conf.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis(),
                        watermarkStrategy == null,
                        conf.get(
                                FlinkConnectorOptions.STREAMING_READ_SHUFFLE_BUCKET_WITH_PARTITION),
                        bucketMode); // 构建连续流操作符
        if (parallelism != null) {
            dataStream.getTransformation().setParallelism(parallelism); // 设置并行度
        }
        if (watermarkStrategy != null) {
            dataStream = dataStream.assignTimestampsAndWatermarks(watermarkStrategy); // 设置水印策略
        }
        return dataStream;
    }

    /**
     * 检查流处理对齐模式的配置是否满足要求。
     *
     * @param env 执行环境
     */
    private void assertStreamingConfigurationForAlignMode(StreamExecutionEnvironment env) {
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkArgument(
                checkpointConfig.isCheckpointingEnabled(),
                "The align mode of paimon source is only supported when checkpoint enabled. Please set "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL.key()
                        + "larger than 0");
        checkArgument(
                checkpointConfig.getMaxConcurrentCheckpoints() == 1,
                "The align mode of paimon source supports at most one ongoing checkpoint at the same time. Please set "
                        + ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS.key()
                        + " to 1");
        checkArgument(
                checkpointConfig.getCheckpointTimeout()
                        > conf.get(FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_TIMEOUT)
                        .toMillis(),
                "The align mode of paimon source requires that the timeout of checkpoint is greater than the timeout of the source's snapshot alignment. Please increase "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT.key()
                        + " or decrease "
                        + FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_TIMEOUT.key());
        checkArgument(
                !env.getCheckpointConfig().isUnalignedCheckpointsEnabled(),
                "The align mode of paimon source currently does not support unaligned checkpoints. Please set "
                        + ExecutionCheckpointingOptions.ENABLE_UNALIGNED.key()
                        + " to false.");
        checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "The align mode of paimon source currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key()
                        + " to exactly-once");
    }
}
