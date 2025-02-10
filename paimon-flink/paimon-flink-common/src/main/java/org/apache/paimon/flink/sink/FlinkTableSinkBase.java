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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.flink.PaimonDataStreamSinkProvider;
import org.apache.paimon.flink.log.LogSinkProvider;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTERING_COLUMNS;
import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTERING_SAMPLE_FACTOR;
import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTERING_SORT_IN_CLUSTER;
import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTERING_STRATEGY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_PARALLELISM;

/**
 * Flink Table Sink 基类，用于创建 Sink，支持动态表写入、覆盖写入（overwrite）和分区（partitioning）。
 * 该类是 Paimon 表在 Flink 生态中的核心写入组件。
 */
public abstract class FlinkTableSinkBase
        implements DynamicTableSink, SupportsOverwrite, SupportsPartitioning {

    /** 表的唯一标识符，包含数据库名和表名 */
    protected final ObjectIdentifier tableIdentifier;

    /** Flink 动态表工厂上下文，包含表的元数据信息 */
    protected final DynamicTableFactory.Context context;

    /** （可选）日志存储表工厂，用于支持日志存储的 Sink */
    @Nullable protected final LogStoreTableFactory logStoreTableFactory;

    /** 目标表对象 */
    protected final Table table;

    /** 静态分区信息（键值对存储） */
    protected Map<String, String> staticPartitions = new HashMap<>();

    /** 是否启用覆盖写入（INSERT OVERWRITE）模式 */
    protected boolean overwrite = false;

    /**
     * 构造方法，初始化 FlinkTableSinkBase。
     *
     * @param tableIdentifier 表的唯一标识符
     * @param table 目标表对象
     * @param context Flink 动态表工厂上下文
     * @param logStoreTableFactory （可选）日志存储表工厂
     */
    public FlinkTableSinkBase(
            ObjectIdentifier tableIdentifier,
            Table table,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.context = context;
        this.logStoreTableFactory = logStoreTableFactory;
    }

    /**
     * 获取 Changelog 模式，决定 Sink 如何处理数据变更。
     *
     * @param requestedMode 请求的 Changelog 模式
     * @return 最终确定的 Changelog 模式
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (table.primaryKeys().isEmpty()) {
            // 如果表没有主键，不进行特殊处理，直接返回请求的模式
            return requestedMode;
        } else {
            Options options = Options.fromMap(table.options());

            // 如果 Changelog 生产者为 INPUT，则直接返回请求的模式
            if (options.get(CHANGELOG_PRODUCER) == ChangelogProducer.INPUT) {
                return requestedMode;
            }

            // 如果合并引擎为 AGGREGATE，直接返回请求的模式
            if (options.get(MERGE_ENGINE) == MergeEngine.AGGREGATE) {
                return requestedMode;
            }

            // 如果合并引擎为 PARTIAL_UPDATE 且定义了聚合函数，则返回请求的模式
            if (options.get(MERGE_ENGINE) == MergeEngine.PARTIAL_UPDATE
                    && new CoreOptions(options).definedAggFunc()) {
                return requestedMode;
            }

            // 如果日志变更模式为 ALL，则返回请求的模式
            if (options.get(LOG_CHANGELOG_MODE) == LogChangelogMode.ALL) {
                return requestedMode;
            }

            // 默认情况下，带主键的 Sink 采用 UPSERT 方式
            ChangelogMode.Builder builder = ChangelogMode.newBuilder();
            for (RowKind kind : requestedMode.getContainedKinds()) {
                // 排除 UPDATE_BEFORE 事件
                if (kind != RowKind.UPDATE_BEFORE) {
                    builder.addContainedKind(kind);
                }
            }
            return builder.build();
        }
    }

    /**
     * 获取 Sink 运行时提供者，定义具体的 Sink 运行逻辑。
     *
     * @param context 运行时上下文
     * @return SinkRuntimeProvider 实例
     */
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // 在流模式下不支持 INSERT OVERWRITE
        if (overwrite && !context.isBounded()) {
            throw new UnsupportedOperationException(
                    "Paimon 不支持流式 INSERT OVERWRITE。");
        }

        // 创建日志存储 Sink 提供者（如果存在）
        LogSinkProvider logSinkProvider = null;
        if (logStoreTableFactory != null) {
            logSinkProvider = logStoreTableFactory.createSinkProvider(this.context, context);
        }

        // 获取表的配置信息
        Options conf = Options.fromMap(table.options());

        // 在覆盖写入模式下不向日志存储 Sink 进行写入
        final LogSinkFunction logSinkFunction =
                overwrite ? null : (logSinkProvider == null ? null : logSinkProvider.createSink());

        // 构造 Sink 提供者
        return new PaimonDataStreamSinkProvider(
                (dataStream) -> {
                    // 创建 Sink 构建器
                    LogFlinkSinkBuilder builder = createSinkBuilder();

                    // 配置日志存储 Sink、数据流、分区聚合等
                    builder.logSinkFunction(logSinkFunction)
                            .forRowData(
                                    new DataStream<>(
                                            dataStream.getExecutionEnvironment(),
                                            dataStream.getTransformation()))
                            .clusteringIfPossible(
                                    conf.get(CLUSTERING_COLUMNS),
                                    conf.get(CLUSTERING_STRATEGY),
                                    conf.get(CLUSTERING_SORT_IN_CLUSTER),
                                    conf.get(CLUSTERING_SAMPLE_FACTOR));

                    // 如果启用了覆盖写入模式，则应用静态分区
                    if (overwrite) {
                        builder.overwrite(staticPartitions);
                    }

                    // 设置 Sink 并行度（如果存在配置）
                    conf.getOptional(SINK_PARALLELISM).ifPresent(builder::parallelism);

                    // 构建最终的 Sink
                    return builder.build();
                });
    }

    /**
     * 创建 Sink 构建器。
     *
     * @return LogFlinkSinkBuilder 实例
     */
    protected LogFlinkSinkBuilder createSinkBuilder() {
        return new LogFlinkSinkBuilder(table);
    }

    /**
     * 复制当前 Sink 实例。
     *
     * @return 复制后的 FlinkTableSink 实例
     */
    @Override
    public DynamicTableSink copy() {
        FlinkTableSink copied =
                new FlinkTableSink(tableIdentifier, table, context, logStoreTableFactory);
        copied.staticPartitions = new HashMap<>(staticPartitions);
        copied.overwrite = overwrite;
        return copied;
    }

    /**
     * 获取 Sink 的摘要信息。
     *
     * @return Sink 的描述字符串
     */
    @Override
    public String asSummaryString() {
        return "PaimonSink";
    }

    /**
     * 应用静态分区，向 staticPartitions 变量添加静态分区信息。
     *
     * @param partition 需要应用的静态分区信息（键值对）
     */
    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        table.partitionKeys()
                .forEach(
                        partitionKey -> {
                            if (partition.containsKey(partitionKey)) {
                                this.staticPartitions.put(
                                        partitionKey, partition.get(partitionKey));
                            }
                        });
    }

    /**
     * 设置是否启用覆盖写入模式（INSERT OVERWRITE）。
     *
     * @param overwrite 是否启用覆盖写入模式
     */
    @Override
    public void applyOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }
}

