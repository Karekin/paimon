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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.factories.FlinkFactoryUtil.FlinkTableFactoryHelper;
import org.apache.paimon.flink.log.LogStoreRegister;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.CoreOptions.LogConsistency;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.flink.factories.FlinkFactoryUtil.createFlinkTableFactoryHelper;
import static org.apache.paimon.flink.kafka.KafkaLogOptions.TOPIC;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;

/** Kafka 的 {@link LogStoreTableFactory} 实现类 */
public class KafkaLogStoreFactory implements LogStoreTableFactory {

    // Kafka 的标识符
    public static final String IDENTIFIER = "kafka";

    // Kafka 相关配置的前缀
    public static final String KAFKA_PREFIX = IDENTIFIER + ".";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * 获取 Kafka 主题名称
     *
     * @param context 上下文对象，包含表的元数据等信息
     * @return Kafka 主题名称
     */
    private String topic(Context context) {
        return context.getCatalogTable().getOptions().get(TOPIC.key());
    }

    /**
     * 创建 KafkaLogSourceProvider 作为数据源提供者
     *
     * @param context        表的上下文，包含表的元数据
     * @param sourceContext  动态表源的上下文
     * @param projectFields  需要投影的字段索引
     * @return KafkaLogSourceProvider 实例
     */
    @Override
    public KafkaLogSourceProvider createSourceProvider(
            Context context,
            DynamicTableSource.Context sourceContext,
            @Nullable int[][] projectFields) {
        // 使用 FlinkTableFactoryHelper 帮助类处理 Flink 表工厂的参数
        FlinkTableFactoryHelper helper = createFlinkTableFactoryHelper(this, context);

        // 获取表的解析模式
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();

        // 获取物理数据类型
        DataType physicalType = schema.toPhysicalRowDataType();

        // 定义主键反序列化器（如果存在主键）
        DeserializationSchema<RowData> primaryKeyDeserializer = null;

        // 获取主键字段索引
        int[] primaryKey = getPrimaryKeyIndexes(schema);

        // 如果表有主键，则创建主键解码器
        if (primaryKey.length > 0) {
            DataType keyType = DataTypeUtils.projectRow(physicalType, primaryKey);
            primaryKeyDeserializer =
                    LogStoreTableFactory.getKeyDecodingFormat(helper)
                            .createRuntimeDecoder(sourceContext, keyType);
        }

        // 创建值解码器
        DeserializationSchema<RowData> valueDeserializer =
                LogStoreTableFactory.getValueDecodingFormat(helper)
                        .createRuntimeDecoder(sourceContext, physicalType);

        // 转换为 Options 配置
        Options options = toOptions(helper.getOptions());

        // 解析扫描时间戳
        Long timestampMills = options.get(SCAN_TIMESTAMP_MILLIS);
        String timestampString = options.get(SCAN_TIMESTAMP);

        // 如果时间戳为 null 且时间戳字符串不为空，则解析时间戳
        if (timestampMills == null && timestampString != null) {
            timestampMills =
                    DateTimeUtils.parseTimestampData(timestampString, 3, TimeZone.getDefault())
                            .getMillisecond();
        }

        // 创建 KafkaLogSourceProvider 作为 Kafka 数据源
        return new KafkaLogSourceProvider(
                topic(context),
                toKafkaProperties(options),
                physicalType,
                primaryKey,
                primaryKeyDeserializer,
                valueDeserializer,
                projectFields,
                options.get(LOG_CONSISTENCY),
                // TODO: 需要访问 CoreOptions 中的所有选项
                CoreOptions.startupMode(options),
                timestampMills);
    }

    /**
     * 创建 KafkaLogSinkProvider 作为数据写入提供者
     *
     * @param context     表的上下文，包含表的元数据
     * @param sinkContext 动态表 Sink 的上下文
     * @return KafkaLogSinkProvider 实例
     */
    @Override
    public KafkaLogSinkProvider createSinkProvider(
            Context context, DynamicTableSink.Context sinkContext) {
        // 使用 FlinkTableFactoryHelper 解析表工厂参数
        FlinkTableFactoryHelper helper = createFlinkTableFactoryHelper(this, context);

        // 获取表的解析模式
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();

        // 获取物理数据类型
        DataType physicalType = schema.toPhysicalRowDataType();

        // 定义主键序列化器（如果存在主键）
        SerializationSchema<RowData> primaryKeySerializer = null;

        // 获取主键字段索引
        int[] primaryKey = getPrimaryKeyIndexes(schema);

        // 如果表有主键，则创建主键编码器
        if (primaryKey.length > 0) {
            DataType keyType = DataTypeUtils.projectRow(physicalType, primaryKey);
            primaryKeySerializer =
                    LogStoreTableFactory.getKeyEncodingFormat(helper)
                            .createRuntimeEncoder(sinkContext, keyType);
        }

        // 创建值编码器
        SerializationSchema<RowData> valueSerializer =
                LogStoreTableFactory.getValueEncodingFormat(helper)
                        .createRuntimeEncoder(sinkContext, physicalType);

        // 转换为 Options 配置
        Options options = toOptions(helper.getOptions());

        // 创建 KafkaLogSinkProvider 作为 Kafka 数据 Sink
        return new KafkaLogSinkProvider(
                topic(context),
                toKafkaProperties(options),
                primaryKeySerializer,
                valueSerializer,
                options.get(LOG_CONSISTENCY),
                options.get(LOG_CHANGELOG_MODE));
    }

    /**
     * 创建 Kafka 日志存储注册器
     *
     * @param context 注册上下文
     * @return KafkaLogStoreRegister 实例
     */
    @Override
    public LogStoreRegister createRegister(RegisterContext context) {
        return new KafkaLogStoreRegister(context);
    }

    /**
     * 获取主键字段索引
     *
     * @param schema 解析后的表模式
     * @return 主键字段索引数组
     */
    private int[] getPrimaryKeyIndexes(ResolvedSchema schema) {
        final List<String> columns = schema.getColumnNames();
        return schema.getPrimaryKey()
                .map(UniqueConstraint::getColumns)
                .map(pkColumns -> pkColumns.stream().mapToInt(columns::indexOf).toArray())
                .orElseGet(() -> new int[] {});
    }

    /**
     * 将 Options 配置转换为 Kafka 配置属性
     *
     * @param options 选项参数
     * @return Kafka 配置属性
     */
    public static Properties toKafkaProperties(Options options) {
        Properties properties = new Properties();
        properties.putAll(convertToPropertiesPrefixKey(options.toMap(), KAFKA_PREFIX));

        // 如果一致性模式为事务型，则设置 Kafka 的隔离级别为 "read_committed"
        if (options.get(LOG_CONSISTENCY) == LogConsistency.TRANSACTIONAL) {
            properties.setProperty(ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        return properties;
    }

    /**
     * 将 ReadableConfig 转换为 Options
     *
     * @param config 可读配置
     * @return 转换后的 Options
     */
    private Options toOptions(ReadableConfig config) {
        Options options = new Options();
        ((Configuration) config).toMap().forEach(options::setString);
        return options;
    }
}

