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

package org.apache.paimon.flink.sink;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.sink.index.GlobalDynamicBucketSink;
import org.apache.paimon.flink.sorter.TableSortInfo;
import org.apache.paimon.flink.sorter.TableSorter;
import org.apache.paimon.flink.sorter.TableSorter.OrderType;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTERING_SAMPLE_FACTOR;
import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTERING_STRATEGY;
import static org.apache.paimon.flink.FlinkConnectorOptions.MIN_CLUSTERING_SAMPLE_FACTOR;
import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.paimon.flink.sorter.TableSorter.OrderType.HILBERT;
import static org.apache.paimon.flink.sorter.TableSorter.OrderType.ORDER;
import static org.apache.paimon.flink.sorter.TableSorter.OrderType.ZORDER;
import static org.apache.paimon.table.BucketMode.BUCKET_UNAWARE;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * 用于构建 Flink Sink 的 DataStream API。
 *
 * @since 0.8
 */
@Public
public class FlinkSinkBuilder {

    // 日志记录器，用于记录构建过程中的日志
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSinkBuilder.class);

    // 存储表的实例，表示将要写入的表
    protected final FileStoreTable table;

    // 输入流，包含待处理的数据
    private DataStream<RowData> input;

    // 覆盖分区（可选），用于 INSERT OVERWRITE 操作时指定分区
    @Nullable protected Map<String, String> overwritePartition;

    // Sink 的并行度（可选）
    @Nullable protected Integer parallelism;

    // 表的排序信息（可选）
    @Nullable private TableSortInfo tableSortInfo;

    // ============== 以下字段用于扩展 ==============

    // 是否启用合并 Sink（默认值为 false）
    protected boolean compactSink = false;

    // 日志 Sink 函数（可选）
    @Nullable protected LogSinkFunction logSinkFunction;

    /**
     * 构造方法，初始化 FlinkSinkBuilder。
     * @param table 要写入的表
     */
    public FlinkSinkBuilder(Table table) {
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException("不支持的表类型: " + table);
        }
        this.table = (FileStoreTable) table;
    }

    /**
     * 将 {@link DataStream} 中的 {@link Row} 转换为 {@link RowData} 类型的 DataStream。
     * 需要提供 {@link DataType} 以帮助构建器将 {@link Row} 转换为 {@link RowData}。
     */
    public FlinkSinkBuilder forRow(DataStream<Row> input, DataType rowDataType) {
        RowType rowType = (RowType) rowDataType.getLogicalType();
        DataType[] fieldDataTypes = rowDataType.getChildren().toArray(new DataType[0]);

        // 创建 Row 转换器，用于将 Row 转换为 RowData
        DataFormatConverters.RowConverter converter =
                new DataFormatConverters.RowConverter(fieldDataTypes);

        // 将 Row 转换为 RowData，并设置并行度和返回类型
        this.input =
                input.map((MapFunction<Row, RowData>) converter::toInternal)
                        .setParallelism(input.getParallelism())
                        .returns(InternalTypeInfo.of(rowType));
        return this;
    }

    /**
     * 使用 {@link RowData} 类型的 {@link DataStream} 来构建 Sink。
     */
    public FlinkSinkBuilder forRowData(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    /**
     * 启用 INSERT OVERWRITE 操作。
     */
    public FlinkSinkBuilder overwrite() {
        return overwrite(new HashMap<>());
    }

    /**
     * 启用 INSERT OVERWRITE 分区操作。
     * @param overwritePartition 覆盖的分区信息
     */
    public FlinkSinkBuilder overwrite(Map<String, String> overwritePartition) {
        this.overwritePartition = overwritePartition;
        return this;
    }

    /**
     * 设置 Sink 的并行度。
     * @param parallelism 并行度
     */
    public FlinkSinkBuilder parallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    /**
     * 如果可能的话，执行输入数据的聚类。
     * @param clusteringColumns 聚类列
     * @param clusteringStrategy 聚类策略
     * @param sortInCluster 是否在聚类中排序
     * @param sampleFactor 采样因子
     */
    public FlinkSinkBuilder clusteringIfPossible(
            String clusteringColumns,
            String clusteringStrategy,
            boolean sortInCluster,
            int sampleFactor) {
        // 如果聚类列为空，或者执行模式为 STREAMING，或者表类型不符合要求，则跳过聚类
        if (clusteringColumns == null || clusteringColumns.isEmpty()) {
            return this;
        }
        checkState(input != null, "必须先指定输入流。");
        if (FlinkSink.isStreaming(input) || !table.bucketMode().equals(BUCKET_UNAWARE)) {
            LOG.warn(
                    "启用了聚类，但由于只支持无主键的桶表和批处理模式，因此已跳过聚类操作。");
            return this;
        }

        // 如果没有跳过聚类，检查聚类列名和采样因子值
        List<String> columns = Arrays.asList(clusteringColumns.split(","));
        List<String> fieldNames = table.schema().fieldNames();
        checkState(
                new HashSet<>(fieldNames).containsAll(new HashSet<>(columns)),
                String.format(
                        "字段名 %s 必须包含所有聚类列名 %s。",
                        fieldNames, columns));
        checkState(
                sampleFactor >= MIN_CLUSTERING_SAMPLE_FACTOR,
                "允许的最小聚类采样因子为 " + MIN_CLUSTERING_SAMPLE_FACTOR + "。");

        // 设置排序信息
        TableSortInfo.Builder sortInfoBuilder = new TableSortInfo.Builder();
        if (clusteringStrategy.equals(CLUSTERING_STRATEGY.defaultValue())) {
            if (columns.size() == 1) {
                sortInfoBuilder.setSortStrategy(ORDER);
            } else if (columns.size() < 5) {
                sortInfoBuilder.setSortStrategy(ZORDER);
            } else {
                sortInfoBuilder.setSortStrategy(HILBERT);
            }
        } else {
            sortInfoBuilder.setSortStrategy(OrderType.of(clusteringStrategy));
        }

        // 计算并行度和样本大小
        int upstreamParallelism = input.getParallelism();
        String sinkParallelismValue =
                table.options().get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        int sinkParallelism =
                sinkParallelismValue == null
                        ? upstreamParallelism
                        : Integer.parseInt(sinkParallelismValue);

        sortInfoBuilder
                .setSortColumns(columns)
                .setSortInCluster(sortInCluster)
                .setSinkParallelism(sinkParallelism);
        int globalSampleSize = sinkParallelism * sampleFactor;
        // If the adaptive scheduler is not enabled, the local sample size is determined by the
        // division of global sample size by the upstream parallelism, which limits total
        // received data of global sample node. If the adaptive scheduler is enabled, the
        // local sample size will equal to sinkParallelism * minimum sample factor.
        int localSampleSize =
                upstreamParallelism > 0
                        ? Math.max(sampleFactor, globalSampleSize / upstreamParallelism)
                        : sinkParallelism * MIN_CLUSTERING_SAMPLE_FACTOR;

        // 设置排序信息
        this.tableSortInfo =
                sortInfoBuilder
                        .setRangeNumber(sinkParallelism)
                        .setGlobalSampleSize(globalSampleSize)
                        .setLocalSampleSize(localSampleSize)
                        .build();
        return this;
    }

    /**
     * 构建 {@link DataStreamSink} 对象。
     */
    public DataStreamSink<?> build() {
        // 尝试对输入数据进行排序
        input = trySortInput(input);

        // 将输入流转换为 InternalRow 类型
        DataStream<InternalRow> input = mapToInternalRow(this.input, table.rowType());

        // 如果启用了本地合并并且表包含主键，则执行本地合并操作
        if (table.coreOptions().localMergeEnabled() && table.schema().primaryKeys().size() > 0) {
            input =
                    input.forward()
                            .transform(
                                    "local merge",
                                    input.getType(),
                                    new LocalMergeOperator(table.schema()))
                            .setParallelism(input.getParallelism());
        }

        // 根据桶模式构建不同的 Sink
        BucketMode bucketMode = table.bucketMode();
        switch (bucketMode) {
            case HASH_FIXED:
                return buildForFixedBucket(input);
            case HASH_DYNAMIC:
                return buildDynamicBucketSink(input, false);
            case CROSS_PARTITION:
                return buildDynamicBucketSink(input, true);
            case BUCKET_UNAWARE:
                return buildUnawareBucketSink(input);
            default:
                throw new UnsupportedOperationException("不支持的桶模式: " + bucketMode);
        }
    }

    /**
     * 将 {@link RowData} 转换为 {@link InternalRow} 类型的流。
     */
    protected DataStream<InternalRow> mapToInternalRow(
            DataStream<RowData> input, org.apache.paimon.types.RowType rowType) {
        return input.map((MapFunction<RowData, InternalRow>) FlinkRowWrapper::new)
                .setParallelism(input.getParallelism())
                .returns(org.apache.paimon.flink.utils.InternalTypeInfo.fromRowType(rowType));
    }

    /**
     * 构建动态桶 Sink。
     */
    protected DataStreamSink<?> buildDynamicBucketSink(
            DataStream<InternalRow> input, boolean globalIndex) {
        checkArgument(logSinkFunction == null, "动态桶模式无法与日志系统一起使用。");
        return compactSink && !globalIndex
                // todo support global index sort compact
                ? new DynamicBucketCompactSink(table, overwritePartition).build(input, parallelism)
                : globalIndex
                ? new GlobalDynamicBucketSink(table, overwritePartition)
                .build(input, parallelism)
                : new RowDynamicBucketSink(table, overwritePartition)
                .build(input, parallelism);
    }

    /**
     * 构建固定桶 Sink。
     */
    protected DataStreamSink<?> buildForFixedBucket(DataStream<InternalRow> input) {
        DataStream<InternalRow> partitioned =
                partition(
                        input,
                        new RowDataChannelComputer(table.schema(), logSinkFunction != null),
                        parallelism);
        FixedBucketSink sink = new FixedBucketSink(table, overwritePartition, logSinkFunction);
        return sink.sinkFrom(partitioned);
    }

    /**
     * 构建非桶模式 Sink。
     */
    private DataStreamSink<?> buildUnawareBucketSink(DataStream<InternalRow> input) {
        checkArgument(
                table.primaryKeys().isEmpty(),
                "非桶模式仅支持追加表。");
        return new RowUnawareBucketSink(table, overwritePartition, logSinkFunction, parallelism)
                .sinkFrom(input);
    }

    /**
     * 尝试对输入数据进行排序，如果排序信息可用，则执行排序。
     */
    private DataStream<RowData> trySortInput(DataStream<RowData> input) {
        if (tableSortInfo != null) {
            TableSorter sorter =
                    TableSorter.getSorter(
                            input.getExecutionEnvironment(), input, table, tableSortInfo);
            return sorter.sort();
        }
        return input;
    }
}

