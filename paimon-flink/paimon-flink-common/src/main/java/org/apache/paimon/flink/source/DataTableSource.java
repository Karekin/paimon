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
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.flink.FlinkConnectorOptions.WatermarkEmitStrategy;
import org.apache.paimon.flink.PaimonDataStreamScanProvider;
import org.apache.paimon.flink.log.LogSourceProvider;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.flink.lookup.FileStoreLookupFunction;
import org.apache.paimon.flink.lookup.LookupRuntimeProviderFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Projection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.plan.stats.TableStats;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_ASYNC;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_ASYNC_THREAD_NUMBER;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_REMOVE_NORMALIZE;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_GROUP;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_EMIT_STRATEGY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_IDLE_TIMEOUT;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * 用于在批处理模式（batch mode）或关闭变更追踪（change-tracking is disabled）时，
 *      创建 {@link StaticFileStoreSource} 或 {@link ContinuousFileStoreSource}。
 * 对于流处理模式（streaming mode）且开启变更追踪（change-tracking enabled）且采用 FULL 扫描模式，
 *      将创建一个基于 {@link LogHybridSourceFactory.FlinkHybridFirstSource} 和通过{@link LogSourceProvider} 创建的 kafka 日志源的混合数据源。
 */
public class DataTableSource extends FlinkTableSource {

    // 表的唯一标识符
    private final ObjectIdentifier tableIdentifier;

    // 标识当前表扫描是流式（true）还是批处理（false）
    private final boolean streaming;

    // Flink 动态表工厂的上下文环境
    private final DynamicTableFactory.Context context;

    // 日志存储表工厂（可能为 null）
    @Nullable private final LogStoreTableFactory logStoreTableFactory;

    // 水印策略（可能为 null）
    @Nullable private WatermarkStrategy<RowData> watermarkStrategy;

    // 动态分区过滤字段列表
    @Nullable private List<String> dynamicPartitionFilteringFields;

    // 构造函数，用于初始化表源的基础信息
    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        // 调用具有更多参数的构造函数
        this(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                null,
                null,
                null,
                null,
                null);
    }

    // 构造函数，用于初始化表源的所有参数
    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy,
            @Nullable List<String> dynamicPartitionFilteringFields) {
        super(table, predicate, projectFields, limit); // 调用父类构造函数
        this.tableIdentifier = tableIdentifier;
        this.streaming = streaming;
        this.context = context;
        this.logStoreTableFactory = logStoreTableFactory;
        this.predicate = predicate;
        this.projectFields = projectFields;
        this.limit = limit;
        this.watermarkStrategy = watermarkStrategy;
        this.dynamicPartitionFilteringFields = dynamicPartitionFilteringFields;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (!streaming) {
            // 如果处于批处理模式，合并所有数据，并返回仅插入模式
            return ChangelogMode.insertOnly();
        }

        if (table.primaryKeys().isEmpty()) {
            // 如果表中没有主键，返回仅插入模式
            return ChangelogMode.insertOnly();
        } else {
            Options options = Options.fromMap(table.options()); // 获取表的配置选项

            if (new CoreOptions(options).mergeEngine() == CoreOptions.MergeEngine.FIRST_ROW) {
                // 如果合并引擎为 FIRST_ROW，返回仅插入模式
                return ChangelogMode.insertOnly();
            }

            if (options.get(SCAN_REMOVE_NORMALIZE)) {
                // 如果设置了清除归一化选项，返回包括所有变更的日志模式
                return ChangelogMode.all();
            }

            if (logStoreTableFactory == null
                    && options.get(CHANGELOG_PRODUCER) != ChangelogProducer.NONE) {
                // 如果日志存储表工厂为空且变更日志生产者未禁用，返回所有变更模式
                return ChangelogMode.all();
            }

            // 优化：如果启用事务一致性和所有变更日志模式，则避免生成归一化节点
            return options.get(LOG_CONSISTENCY) == LogConsistency.TRANSACTIONAL
                    && options.get(LOG_CHANGELOG_MODE) == LogChangelogMode.ALL
                    ? ChangelogMode.all()
                    : ChangelogMode.upsert();
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        LogSourceProvider logSourceProvider = null;

        if (logStoreTableFactory != null) {
            // 创建日志源提供器
            logSourceProvider =
                    logStoreTableFactory.createSourceProvider(context, scanContext, projectFields);
        }

        // 获取当前水印策略
        WatermarkStrategy<RowData> watermarkStrategy = this.watermarkStrategy;

        // 获取表的配置选项
        Options options = Options.fromMap(table.options());

        if (watermarkStrategy != null) {
            // 配置水印策略的事件触发类型
            WatermarkEmitStrategy emitStrategy = options.get(SCAN_WATERMARK_EMIT_STRATEGY);
            if (emitStrategy == WatermarkEmitStrategy.ON_EVENT) {
                watermarkStrategy = new OnEventWatermarkStrategy(watermarkStrategy);
            }

            // 配置水印生成的空闲超时时间
            Duration idleTimeout = options.get(SCAN_WATERMARK_IDLE_TIMEOUT);
            if (idleTimeout != null) {
                watermarkStrategy = watermarkStrategy.withIdleness(idleTimeout);
            }

            // 配置水印对齐的分组策略
            String watermarkAlignGroup = options.get(SCAN_WATERMARK_ALIGNMENT_GROUP);
            if (watermarkAlignGroup != null) {
                watermarkStrategy =
                        WatermarkAlignUtils.withWatermarkAlignment(
                                watermarkStrategy,
                                watermarkAlignGroup,
                                options.get(SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT),
                                options.get(SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL));
            }
        }

        // 初始化 Flink 数据源构建器
        FlinkSourceBuilder sourceBuilder =
                new FlinkSourceBuilder(table)
                        .sourceName(tableIdentifier.asSummaryString()) // 设置数据源名称
                        .sourceBounded(!streaming) // 设置是否为有界数据源（流式为 false，批处理为 true）
                        .logSourceProvider(logSourceProvider) // 设置日志源提供器
                        .projection(projectFields) // 设置投影字段
                        .predicate(predicate) // 设置谓词过滤条件
                        .limit(limit) // 设置查询限制
                        .watermarkStrategy(watermarkStrategy) // 设置水印策略
                        .dynamicPartitionFilteringFields(dynamicPartitionFilteringFields); // 设置动态分区过滤字段

        // 返回运行时扫描数据源提供器
        return new PaimonDataStreamScanProvider(
                !streaming,
                env ->
                        sourceBuilder
                                .sourceParallelism(inferSourceParallelism(env)) // 推断并设置数据源并行度
                                .env(env) // 设置 Flink 环境
                                .build());
    }

    @Override
    public DataTableSource copy() {
        // 创建当前表源的副本
        return new DataTableSource(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                dynamicPartitionFilteringFields);
    }

    @Override
    public void pushWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        // 设置水印策略
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // 检查是否存在限制查询，流式查询不支持限制
        if (limit != null) {
            throw new RuntimeException(
                    "Limit push down should not happen in Lookup source, but it is " + limit);
        }

        // 获取投影字段和连接键
        int[] projection =
                projectFields == null
                        ? IntStream.range(0, table.rowType().getFieldCount()).toArray() // 默认为所有字段
                        : Projection.of(projectFields).toTopLevelIndexes();
        int[] joinKey = Projection.of(context.getKeys()).toTopLevelIndexes();

        // 获取表的配置选项
        Options options = new Options(table.options());

        // 是否启用异步查找
        boolean enableAsync = options.get(LOOKUP_ASYNC);

        // 异步线程池大小
        int asyncThreadNumber = options.get(LOOKUP_ASYNC_THREAD_NUMBER);

        // 创建运行时查找数据源提供器
        return LookupRuntimeProviderFactory.create(
                new FileStoreLookupFunction(table, projection, joinKey, predicate),
                enableAsync,
                asyncThreadNumber);
    }

    @Override
    public TableStats reportStatistics() {
        // 如果处于流式模式，无法统计，返回未知
        if (streaming) {
            return TableStats.UNKNOWN;
        }

        // 扫描并统计表数据
        scanSplitsForInference();
        return new TableStats(splitStatistics.totalRowCount());
    }

    @Override
    public String asSummaryString() {
        // 返回表源的摘要字符串
        return "Paimon-DataSource";
    }

    @Override
    public List<String> listAcceptedFilterFields() {
        // 如果是流式模式，不支持动态过滤；否则返回分区键作为可能的过滤字段
        return streaming
                ? Collections.emptyList()
                : table.partitionKeys();
    }

    @Override
    public void applyDynamicFiltering(List<String> candidateFilterFields) {
        // 确保当前不是流式模式
        checkState(
                !streaming,
                "Cannot apply dynamic filtering to Paimon table '%s' when streaming reading.",
                table.name());

        // 确保表是分区表
        checkState(
                !table.partitionKeys().isEmpty(),
                "Cannot apply dynamic filtering to non-partitioned Paimon table '%s'.",
                table.name());

        // 应用动态分区过滤字段
        this.dynamicPartitionFilteringFields = candidateFilterFields;
    }

    @Override
    public boolean isStreaming() {
        // 返回当前是否是流式模式
        return streaming;
    }
}
