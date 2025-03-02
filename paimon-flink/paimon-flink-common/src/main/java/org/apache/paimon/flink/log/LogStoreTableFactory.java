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

package org.apache.paimon.flink.log;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.flink.factories.FlinkFactoryUtil.FlinkTableFactoryHelper;
import org.apache.paimon.options.Options;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import static org.apache.paimon.CoreOptions.LOG_FORMAT;
import static org.apache.paimon.CoreOptions.LOG_KEY_FORMAT;

/**
 * 配置默认日志表连接器的基本接口。日志表由托管表工厂使用。
 *
 * <p>日志表专门用于处理无界数据，支持流式读取和流式写入。
 */
public interface LogStoreTableFactory extends Factory {

    /**
     * 从 {@link CatalogTable} 和额外的上下文信息创建 {@link LogSourceProvider} 实例。
     *
     * @param context 表的上下文信息，包括表元数据等
     * @param sourceContext 动态表源的上下文
     * @param projectFields 需要投影的字段索引（可为空）
     * @return 返回一个 LogSourceProvider 实例
     */
    LogSourceProvider createSourceProvider(
            Context context,
            DynamicTableSource.Context sourceContext,
            @Nullable int[][] projectFields);

    /**
     * 从 {@link CatalogTable} 和额外的上下文信息创建 {@link LogSinkProvider} 实例。
     *
     * @param context 表的上下文信息，包括表元数据等
     * @param sinkContext 动态表 Sink 的上下文
     * @return 返回一个 LogSinkProvider 实例
     */
    LogSinkProvider createSinkProvider(Context context, DynamicTableSink.Context sinkContext);

    /**
     * 创建 {@link LogStoreRegister} 实例用于表的 DDL 操作。
     * 当表被创建或删除时，它将日志表注册到日志存储系统。
     *
     * @param context 注册上下文信息
     * @return 返回一个 LogStoreRegister 实例
     */
    LogStoreRegister createRegister(RegisterContext context);

    /** 用于创建日志存储注册器的上下文接口。 */
    interface RegisterContext {
        /** 获取表的配置信息。 */
        Options getOptions();

        /** 获取表的标识符。 */
        Identifier getIdentifier();
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 获取日志主键格式的配置选项。
     *
     * @return 返回主键格式的配置选项
     */
    static ConfigOption<String> logKeyFormat() {
        return ConfigOptions.key(LOG_KEY_FORMAT.key())
                .stringType()
                .defaultValue(LOG_KEY_FORMAT.defaultValue());
    }

    /**
     * 获取日志值格式的配置选项。
     *
     * @return 返回值格式的配置选项
     */
    static ConfigOption<String> logFormat() {
        return ConfigOptions.key(LOG_FORMAT.key())
                .stringType()
                .defaultValue(LOG_FORMAT.defaultValue());
    }

    /**
     * 发现并获取指定标识符的日志存储工厂实例。
     *
     * @param cl 类加载器
     * @param identifier 工厂的标识符
     * @return 返回 LogStoreTableFactory 实例
     */
    static LogStoreTableFactory discoverLogStoreFactory(ClassLoader cl, String identifier) {
        return FactoryUtil.discoverFactory(cl, LogStoreTableFactory.class, identifier);
    }

    /**
     * 获取用于主键解码的解码格式。
     *
     * @param helper FlinkTableFactoryHelper 工具类
     * @return 返回主键的解码格式
     */
    static DecodingFormat<DeserializationSchema<RowData>> getKeyDecodingFormat(
            FlinkTableFactoryHelper helper) {
        DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, logKeyFormat());
        validateKeyFormat(format, helper.getOptions().get(logKeyFormat()));
        return format;
    }

    /**
     * 获取用于主键编码的编码格式。
     *
     * @param helper FlinkTableFactoryHelper 工具类
     * @return 返回主键的编码格式
     */
    static EncodingFormat<SerializationSchema<RowData>> getKeyEncodingFormat(
            FlinkTableFactoryHelper helper) {
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, logKeyFormat());
        validateKeyFormat(format, helper.getOptions().get(logKeyFormat()));
        return format;
    }

    /**
     * 获取用于值解码的解码格式。
     *
     * @param helper FlinkTableFactoryHelper 工具类
     * @return 返回值的解码格式
     */
    static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            FlinkTableFactoryHelper helper) {
        DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, logFormat());
        validateValueFormat(format, helper.getOptions().get(logFormat()));
        return format;
    }

    /**
     * 获取用于值编码的编码格式。
     *
     * @param helper FlinkTableFactoryHelper 工具类
     * @return 返回值的编码格式
     */
    static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            FlinkTableFactoryHelper helper) {
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, logFormat());
        validateValueFormat(format, helper.getOptions().get(logFormat()));
        return format;
    }

    /**
     * 验证主键格式是否符合要求。
     *
     * @param format 需要验证的格式
     * @param name 格式名称
     * @throws ValidationException 如果主键格式包含非 INSERT-only 记录，则抛出异常
     */
    static void validateKeyFormat(Format format, String name) {
        if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            throw new ValidationException(
                    String.format(
                            "主键格式只能处理 INSERT-only 记录。"
                                    + "但 %s 的变更日志模式为 %s。",
                            name, format.getChangelogMode()));
        }
    }

    /**
     * 验证值格式是否符合要求。
     *
     * @param format 需要验证的格式
     * @param name 格式名称
     * @throws ValidationException 如果值格式不支持所有记录，则抛出异常
     */
    static void validateValueFormat(Format format, String name) {
        if (!format.getChangelogMode().equals(ChangelogMode.all())) {
            throw new ValidationException(
                    String.format(
                            "值格式应当支持所有记录。"
                                    + "但 %s 的变更日志模式为 %s。",
                            name, format.getChangelogMode()));
        }
    }
}

