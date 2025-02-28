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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.PredicateConverter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PartitionPredicateVisitor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.LookupTableSource.LookupContext;
import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.options.OptionsUtils.PAIMON_PREFIX;

/**
 * Paimon 的 Flink 扫描数据源。
 */
public abstract class FlinkTableSource {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkTableSource.class);

    protected static final String FLINK_INFER_SCAN_PARALLELISM =
            String.format(
                    "%s%s", PAIMON_PREFIX, FlinkConnectorOptions.INFER_SCAN_PARALLELISM.key());

    protected final Table table; // 数据表

    @Nullable protected Predicate predicate; // 过滤条件
    @Nullable protected int[][] projectFields; // 投影字段
    @Nullable protected Long limit; // 查询限制
    protected SplitStatistics splitStatistics; // 分片统计信息

    public FlinkTableSource(Table table) {
        this(table, null, null, null); // 调用另一个构造函数
    }

    public FlinkTableSource(
            Table table,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit) {
        this.table = table; // 初始化数据表
        this.predicate = predicate; // 初始化过滤条件
        this.projectFields = projectFields; // 初始化投影字段
        this.limit = limit; // 初始化查询限制
    }

    /**
     * 将过滤条件下推到数据源，并返回未被处理的过滤条件。
     * <p>
     * 数据源必须确保已处理的过滤条件已完全评估，否则查询结果将不正确。
     *
     * @param filters 过滤条件列表
     * @return 未被处理的过滤条件列表
     */
    public List<ResolvedExpression> pushFilters(List<ResolvedExpression> filters) {
        List<String> partitionKeys = table.partitionKeys(); // 获取分区键
        RowType rowType = LogicalTypeConversion.toLogicalType(table.rowType()); // 获取行类型

        List<ResolvedExpression> unConsumedFilters = new ArrayList<>(); // 未被处理的过滤条件
        List<ResolvedExpression> consumedFilters = new ArrayList<>(); // 已被处理的过滤条件
        List<Predicate> converted = new ArrayList<>(); // 转换后的条件
        PredicateVisitor<Boolean> visitor = new PartitionPredicateVisitor(partitionKeys); // 访问者

        for (ResolvedExpression filter : filters) {
            Optional<Predicate> predicateOptional = PredicateConverter.convert(rowType, filter);

            if (!predicateOptional.isPresent()) {
                unConsumedFilters.add(filter); // 过滤条件无法转换，添加到未处理列表
            } else {
                Predicate p = predicateOptional.get();
                if (isStreaming() || !p.visit(visitor)) {
                    unConsumedFilters.add(filter); // 条件不满足，添加到未处理列表
                } else {
                    consumedFilters.add(filter); // 条件满足，添加到已处理列表
                }
                converted.add(p); // 添加转换后的条件
            }
        }
        predicate = converted.isEmpty() ? null : PredicateBuilder.and(converted); // 合并条件
        LOG.info("过滤条件: {} / {}", consumedFilters.size(), filters.size());

        return unConsumedFilters;
    }

    /**
     * 设置投影字段。
     *
     * @param projectedFields 投影字段
     */
    public void pushProjection(int[][] projectedFields) {
        this.projectFields = projectedFields; // 设置投影字段
    }

    /**
     * 设置查询限制。
     *
     * @param limit 查询限制
     */
    public void pushLimit(long limit) {
        this.limit = limit; // 设置查询限制
    }

    /**
     * 返回变更日志模式。
     *
     * @return 变更日志模式
     */
    public abstract ChangelogMode getChangelogMode();

    /**
     * 获取扫描运行时提供程序。
     *
     * @param scanContext 扫描上下文
     * @return 扫描运行时提供程序
     */
    public abstract ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext);

    /**
     * 应用水印策略。
     *
     * @param watermarkStrategy 水印策略
     */
    public abstract void pushWatermark(WatermarkStrategy<RowData> watermarkStrategy);

    /**
     * 获取 Lookup 运行时提供程序。
     *
     * @param context Lookup 上下文
     * @return Lookup 运行时提供程序
     */
    public abstract LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context);

    /**
     * 返回表的统计信息。
     *
     * @return 表的统计信息
     */
    public abstract TableStats reportStatistics();

    /**
     * 返回当前数据源的副本。
     *
     * @return 当前数据源的副本
     */
    public abstract FlinkTableSource copy();

    /**
     * 返回当前数据源的摘要字符串。
     *
     * @return 摘要字符串
     */
    public abstract String asSummaryString();

    /**
     * 返回可以动态过滤的字段列表。
     *
     * @return 动态过滤字段列表
     */
    public abstract List<String> listAcceptedFilterFields();

    /**
     * 应用动态过滤。
     *
     * @param candidateFilterFields 候选过滤字段
     */
    public abstract void applyDynamicFiltering(List<String> candidateFilterFields);

    /**
     * 是否为流模式。
     *
     * @return 是否为流模式
     */
    public abstract boolean isStreaming();

    /**
     * 推断数据源的并行度。
     *
     * @param env 执行环境
     * @return 推断的并行度
     */
    @Nullable
    protected Integer inferSourceParallelism(StreamExecutionEnvironment env) {
        Options options = Options.fromMap(this.table.options()); // 获取配置选项
        Configuration envConfig = (Configuration) env.getConfiguration();
        if (envConfig.containsKey(FLINK_INFER_SCAN_PARALLELISM)) {
            options.set(
                    FlinkConnectorOptions.INFER_SCAN_PARALLELISM,
                    Boolean.parseBoolean(envConfig.toMap().get(FLINK_INFER_SCAN_PARALLELISM)));
        }
        Integer parallelism = options.get(FlinkConnectorOptions.SCAN_PARALLELISM);
        if (parallelism == null && options.get(FlinkConnectorOptions.INFER_SCAN_PARALLELISM)) {
            if (isStreaming()) {
                parallelism = Math.max(1, options.get(CoreOptions.BUCKET)); // 流模式下使用桶数量作为并行度
            } else {
                scanSplitsForInference();
                parallelism = splitStatistics.splitNumber();
                if (null != limit && limit > 0) {
                    int limitCount =
                            limit >= Integer.MAX_VALUE ? Integer.MAX_VALUE : limit.intValue();
                    parallelism = Math.min(parallelism, limitCount); // 根据限制调整并行度
                }

                parallelism = Math.max(1, parallelism); // 保证并行度至少为1
                parallelism =
                        Math.min(
                                parallelism,
                                options.get(FlinkConnectorOptions.INFER_SCAN_MAX_PARALLELISM)); // 根据最大并行度限制调整并行度
            }
        }
        return parallelism;
    }

    /**
     * 扫描分片以推断统计信息。
     */
    protected void scanSplitsForInference() {
        if (splitStatistics == null) {
            List<Split> splits =
                    table.newReadBuilder().withFilter(predicate).newScan().plan().splits();
            splitStatistics = new SplitStatistics(splits); // 创建分片统计信息
        }
    }

    /**
     * 分片统计信息，用于推断行数和并行度。
     */
    protected static class SplitStatistics {

        private final int splitNumber; // 分片数量
        private final long totalRowCount; // 总行数

        protected SplitStatistics(List<Split> splits) {
            this.splitNumber = splits.size(); // 分片数量
            this.totalRowCount = splits.stream().mapToLong(Split::rowCount).sum(); // 总行数
        }

        public int splitNumber() {
            return splitNumber;
        }

        public long totalRowCount() {
            return totalRowCount;
        }
    }

    public Table getTable() {
        return table;
    }
}
