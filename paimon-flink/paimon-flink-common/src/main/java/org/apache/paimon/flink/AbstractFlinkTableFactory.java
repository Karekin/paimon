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

import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.CoreOptions.StreamingReadMode;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.flink.source.SystemTableSource;
import org.apache.paimon.flink.source.table.PushedRichTableSource;
import org.apache.paimon.flink.source.table.PushedTableSource;
import org.apache.paimon.flink.source.table.RichTableSource;
import org.apache.paimon.lineage.LineageMeta;
import org.apache.paimon.lineage.LineageMetaFactory;
import org.apache.paimon.lineage.TableLineageEntity;
import org.apache.paimon.lineage.TableLineageEntityImpl;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.STREAMING_READ_MODE;
import static org.apache.paimon.CoreOptions.StartupMode.FROM_SNAPSHOT;
import static org.apache.paimon.CoreOptions.StartupMode.FROM_SNAPSHOT_FULL;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOG_SYSTEM;
import static org.apache.paimon.flink.FlinkConnectorOptions.NONE;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_PUSH_DOWN;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.flink.log.LogStoreTableFactory.discoverLogStoreFactory;

/** 抽象表工厂类，用于创建 Flink 的动态表源（DynamicTableSource）和动态表接收器（DynamicTableSink）。 */
public abstract class AbstractFlinkTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFlinkTableFactory.class);

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // 获取原始表定义，包含表的元数据和选项
        CatalogTable origin = context.getCatalogTable().getOrigin();

        // 判断当前运行模式是否为流式模式（Streaming）
        boolean isStreamingMode =
                context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        // 根据不同的表类型进行处理
        if (origin instanceof SystemCatalogTable) {
            // 如果是系统表，创建一个 PushedTableSource
            // PushedTableSource 表示将表定义推送到存储中，并提供相应的表源
            return new PushedTableSource(
                    new SystemTableSource(
                            ((SystemCatalogTable) origin).table(), // 系统表实例
                            isStreamingMode, // 是否是流式模式
                            context.getObjectIdentifier())); // 表的对象标识符
        } else {
            // 如果是普通表，构建一个 Paimon 表实例
            Table table = buildPaimonTable(context);

            // 特殊处理：如果是 FileStoreTable 系统表，则保存表的血缘信息
            if (table instanceof FileStoreTable) {
                // 调用 storeTableLineage 方法保存表的血缘信息
                storeTableLineage(
                        ((FileStoreTable) table).catalogEnvironment().lineageMetaFactory(),
                        context,
                        (entity, lineageFactory) -> {
                            try (LineageMeta lineage =
                                         lineageFactory.create(() -> Options.fromMap(table.options()))) {
                                // 保存表作为数据源的血缘信息
                                lineage.saveSourceTableLineage(entity);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }

            // 创建 DataTableSource，用于从存储中读取数据
            DataTableSource source =
                    new DataTableSource(
                            context.getObjectIdentifier(), // 表的对象标识符
                            table, // Paimon 表实例
                            isStreamingMode, // 是否是流式模式
                            context, // 表的上下文
                            createOptionalLogStoreFactory(context).orElse(null)); // 日志存储工厂

            // 根据配置选择是否使用功能更强的表源（PushedRichTableSource）
            return new Options(table.options()).get(SCAN_PUSH_DOWN)
                    ? new PushedRichTableSource(source) // 强功能表源
                    : new RichTableSource(source); // 普通功能表源
        }
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        Table table = buildPaimonTable(context);

        // 如果是 FileStoreTable 系统表，则保存表的血缘信息
        if (table instanceof FileStoreTable) {
            storeTableLineage(
                    ((FileStoreTable) table).catalogEnvironment().lineageMetaFactory(),
                    context,
                    (entity, lineageFactory) -> {
                        try (LineageMeta lineage =
                                     lineageFactory.create(() -> Options.fromMap(table.options()))) {
                            // 保存表作为数据接收器的血缘信息
                            lineage.saveSinkTableLineage(entity);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }

        // 创建 FlinkTableSink，用于将数据写入存储
        return new FlinkTableSink(
                context.getObjectIdentifier(), // 表的对象标识符
                table, // Paimon 表实例
                context, // 表的上下文
                createOptionalLogStoreFactory(context).orElse(null)); // 日志存储工厂
    }

    // 保存表的血缘信息
    private void storeTableLineage(
            @Nullable LineageMetaFactory lineageMetaFactory,
            Context context,
            BiConsumer<TableLineageEntity, LineageMetaFactory> tableLineage) {
        if (lineageMetaFactory != null) {
            // 从 Context 中获取数据管道的名称
            String pipelineName = context.getConfiguration().get(PipelineOptions.NAME);
            if (pipelineName == null) {
                // 如果未配置管道名称，则抛出异常
                throw new ValidationException("Cannot get pipeline name for lineage meta.");
            }

            // 创建表血缘实体对象
            tableLineage.accept(
                    new TableLineageEntityImpl(
                            context.getObjectIdentifier().getDatabaseName(), // 表所属的数据库名称
                            context.getObjectIdentifier().getObjectName(), // 表的名称
                            pipelineName, // 数据管道名称
                            Timestamp.fromEpochMillis(System.currentTimeMillis())), // 表的创建时间
                    lineageMetaFactory); // 表血缘元数据分析工厂
        }
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        // 返回此工厂类需要的配置选项（默认为空集合）
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        // 返回此工厂类支持的可选配置选项（默认为空集合）
        return new HashSet<>();
    }

    // ~ Tools ------------------------------------------------------------------

    // 创建可选的日志存储工厂
    public static Optional<LogStoreTableFactory> createOptionalLogStoreFactory(
            DynamicTableFactory.Context context) {
        return createOptionalLogStoreFactory(
                context.getClassLoader(), context.getCatalogTable().getOptions());
    }

    static Optional<LogStoreTableFactory> createOptionalLogStoreFactory(
            ClassLoader classLoader, Map<String, String> options) {
        Options configOptions = new Options();
        options.forEach(configOptions::setString);

        // 如果配置的日志系统是 NONE，则直接返回空
        if (configOptions.get(LOG_SYSTEM).equalsIgnoreCase(NONE)) {
            validateFileStoreContinuous(configOptions); // 验证文件存储的连续读取模式是否合法
            return Optional.empty();
        } else if (configOptions.get(SCAN_MODE) == FROM_SNAPSHOT
                || configOptions.get(SCAN_MODE) == FROM_SNAPSHOT_FULL) {
            // 如果扫描模式是 FROM_SNAPSHOT 或 FROM_SNAPSHOT_FULL，则抛出异常
            throw new ValidationException(
                    String.format(
                            "Log system does not support %s and %s scan mode",
                            FROM_SNAPSHOT, FROM_SNAPSHOT_FULL));
        }

        // 创建日志存储工厂
        return Optional.of(discoverLogStoreFactory(classLoader, configOptions.get(LOG_SYSTEM)));
    }

    // 验证文件存储的连续读取模式是否合法
    private static void validateFileStoreContinuous(Options options) {
        LogChangelogMode changelogMode = options.get(LOG_CHANGELOG_MODE);
        StreamingReadMode streamingReadMode = options.get(STREAMING_READ_MODE);

        // 如果变更日志模式是 UPSERT，抛出异常
        if (changelogMode == LogChangelogMode.UPSERT) {
            throw new ValidationException(
                    "File store continuous reading does not support upsert changelog mode.");
        }

        // 如果一致性模式是 EVENTUAL，抛出异常
        LogConsistency consistency = options.get(LOG_CONSISTENCY);
        if (consistency == LogConsistency.EVENTUAL) {
            throw new ValidationException(
                    "File store continuous reading does not support eventual consistency mode.");
        }

        // 如果流式读取模式是 LOG，抛出异常
        if (streamingReadMode == StreamingReadMode.LOG) {
            throw new ValidationException(
                    "File store continuous reading does not support the log streaming read mode.");
        }
    }

    // 创建目录上下文
    static CatalogContext createCatalogContext(DynamicTableFactory.Context context) {
        return CatalogContext.create(
                Options.fromMap(context.getCatalogTable().getOptions()), // 表的选项
                new FlinkFileIOLoader()); // Flink 文件 I/O 加载器
    }

    static Table buildPaimonTable(DynamicTableFactory.Context context) {
        // 获取原始表定义
        CatalogTable origin = context.getCatalogTable().getOrigin();
        Table table;

        // 获取动态表配置选项
        Map<String, String> dynamicOptions = getDynamicTableConfigOptions(context);
        dynamicOptions.forEach(
                (key, newValue) -> {
                    // 检查动态选项是否与原始表选项冲突
                    String oldValue = origin.getOptions().get(key);
                    if (!Objects.equals(oldValue, newValue)) {
                        SchemaManager.checkAlterTableOption(key, oldValue, newValue, true);
                    }
                });
        // 合并原始表选项和动态选项
        Map<String, String> newOptions = new HashMap<>();
        newOptions.putAll(origin.getOptions());
        newOptions.putAll(dynamicOptions);

        // 根据表类型构建 Paimon 表实例
        if (origin instanceof DataCatalogTable) {
            FileStoreTable fileStoreTable = (FileStoreTable) ((DataCatalogTable) origin).table();
            table = fileStoreTable.copyWithoutTimeTravel(newOptions);
        } else {
            table =
                    FileStoreTableFactory.create(createCatalogContext(context))
                            .copyWithoutTimeTravel(newOptions);
        }

        // 获取表的元数据
        Schema schema = FlinkCatalog.fromCatalogTable(context.getCatalogTable());
        RowType rowType = toLogicalType(schema.rowType());
        List<String> partitionKeys = schema.partitionKeys();
        List<String> primaryKeys = schema.primaryKeys();

        // 检查表模式是否与 Paimon 表模式一致
        Preconditions.checkArgument(
                schemaEquals(toLogicalType(table.rowType()), rowType),
                "Flink schema and store schema are not the same, " + "store schema is %s, Flink schema is %s",
                table.rowType(),
                rowType);

        // 检查分区键是否一致
        Preconditions.checkArgument(
                table.partitionKeys().equals(partitionKeys),
                "Flink partitionKeys and store partitionKeys are not the same, " + "store partitionKeys is %s, Flink partitionKeys is %s",
                table.partitionKeys(),
                partitionKeys);

        // 检查主键是否一致
        Preconditions.checkArgument(
                table.primaryKeys().equals(primaryKeys),
                "Flink primaryKeys and store primaryKeys are not the same, " + "store primaryKeys is %s, Flink primaryKeys is %s",
                table.primaryKeys(),
                primaryKeys);

        return table;
    }

    // 比较两个 RowType 是否一致
    @VisibleForTesting
    static boolean schemaEquals(RowType rowType1, RowType rowType2) {
        List<RowType.RowField> fieldList1 = rowType1.getFields();
        List<RowType.RowField> fieldList2 = rowType2.getFields();
        if (fieldList1.size() != fieldList2.size()) {
            return false;
        }

        for (int i = 0; i < fieldList1.size(); i++) {
            RowType.RowField f1 = fieldList1.get(i);
            RowType.RowField f2 = fieldList2.get(i);
            if (!f1.getName().equals(f2.getName()) || !f1.getType().equals(f2.getType())) {
                return false;
            }
        }

        return true;
    }

    // 获取动态表配置选项
    static Map<String, String> getDynamicTableConfigOptions(DynamicTableFactory.Context context) {
        Map<String, String> optionsFromTableConfig = new HashMap<>();

        // 获取表的配置
        ReadableConfig config = context.getConfiguration();

        // 将配置转换为 Map
        Map<String, String> conf;
        if (config instanceof Configuration) {
            conf = ((Configuration) config).toMap();
        } else if (config instanceof TableConfig) {
            conf = ((TableConfig) config).getConfiguration().toMap();
        } else {
            throw new IllegalArgumentException("Unexpected config: " + config.getClass());
        }

        // 定义正则表达式模板，用于匹配动态表配置选项
        String template =
                String.format(
                        "(%s)\\.(%s|\\*)\\.(%s|\\*)\\.(%s|\\*)\\.(.+)",
                        FlinkConnectorOptions.TABLE_DYNAMIC_OPTION_PREFIX,
                        context.getObjectIdentifier().getCatalogName(),
                        context.getObjectIdentifier().getDatabaseName(),
                        context.getObjectIdentifier().getObjectName());

        // 编译正则表达式
        Pattern pattern = Pattern.compile(template);

        // 遍历配置项，提取匹配的动态选项
        conf.keySet()
                .forEach(
                        (key) -> {
                            if (key.startsWith(FlinkConnectorOptions.TABLE_DYNAMIC_OPTION_PREFIX)) {
                                Matcher matcher = pattern.matcher(key);
                                if (matcher.find()) {
                                    optionsFromTableConfig.put(matcher.group(5), conf.get(key));
                                }
                            }
                        });

        // 如果提取到动态选项，则记录日志
        if (!optionsFromTableConfig.isEmpty()) {
            LOG.info(
                    "Loading dynamic table options for {} in table config: {}",
                    context.getObjectIdentifier().getObjectName(),
                    optionsFromTableConfig);
        }

        return optionsFromTableConfig;
    }
}
