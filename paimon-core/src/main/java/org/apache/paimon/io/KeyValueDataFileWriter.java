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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

/**
 * 一个 {@link StatsCollectingSingleFileWriter} 的子类，用于写入包含 {@link KeyValue} 的数据文件。
 * 同时在写完文件后生成 {@link DataFileMeta} 元数据对象。
 *
 * <p>注意：写入的记录必须是排序好的，因为该类不会比较最小和最大键来生成 {@link DataFileMeta}。
 */
public class KeyValueDataFileWriter
        extends StatsCollectingSingleFileWriter<KeyValue, DataFileMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueDataFileWriter.class);

    private final RowType keyType; // 键的数据类型
    private final RowType valueType; // 值的数据类型
    private final long schemaId; // 模式（Schema）ID
    private final int level; // 文件级别

    private final SimpleStatsConverter keyStatsConverter; // 键的统计信息转换器
    private final SimpleStatsConverter valueStatsConverter; // 值的统计信息转换器
    private final InternalRowSerializer keySerializer; // 键的序列化器
    private final FileSource fileSource; // 文件来源

    private BinaryRow minKey = null; // 当前文件中的最小键
    private InternalRow maxKey = null; // 当前文件中的最大键
    private long minSeqNumber = Long.MAX_VALUE; // 最小序列号
    private long maxSeqNumber = Long.MIN_VALUE; // 最大序列号
    private long deleteRecordCount = 0; // 删除记录的计数

    /**
     * 构造方法，初始化 {@link KeyValueDataFileWriter} 对象。
     *
     * @param fileIO 文件 I/O 实例
     * @param factory 文件写入工厂实例
     * @param path 文件路径
     * @param converter 将 {@link KeyValue} 转换为 {@link InternalRow} 的函数
     * @param keyType 键的数据类型
     * @param valueType 值的数据类型
     * @param simpleStatsExtractor 提取简单统计信息的工具（可选）
     * @param schemaId 模式（Schema）ID
     * @param level 文件级别
     * @param compression 压缩方式
     * @param options 核心选项配置
     * @param fileSource 文件来源
     */
    public KeyValueDataFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            Function<KeyValue, InternalRow> converter,
            RowType keyType,
            RowType valueType,
            @Nullable SimpleStatsExtractor simpleStatsExtractor,
            long schemaId,
            int level,
            String compression,
            CoreOptions options,
            FileSource fileSource) {
        super(
                fileIO,
                factory,
                path,
                converter,
                KeyValue.schema(keyType, valueType), // 生成的 schema
                simpleStatsExtractor,
                compression,
                StatsCollectorFactories.createStatsFactories( // 创建统计信息收集器工厂
                        options,
                        KeyValue.schema(keyType, valueType).getFieldNames()),
                options.asyncFileWrite()); // 是否异步写入文件

        this.keyType = keyType;
        this.valueType = valueType;
        this.schemaId = schemaId;
        this.level = level;

        this.keyStatsConverter = new SimpleStatsConverter(keyType); // 初始化键的统计信息转换器
        this.valueStatsConverter = new SimpleStatsConverter(valueType); // 初始化值的统计信息转换器
        this.keySerializer = new InternalRowSerializer(keyType); // 初始化键的序列化器
        this.fileSource = fileSource;
    }

    @Override
    public void write(KeyValue kv) throws IOException {
        super.write(kv); // 调用父类的写入方法

        updateMinKey(kv); // 更新最小键
        updateMaxKey(kv); // 更新最大键

        updateMinSeqNumber(kv); // 更新最小序列号
        updateMaxSeqNumber(kv); // 更新最大序列号

        if (kv.valueKind().isRetract()) {
            deleteRecordCount++; // 统计删除记录
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("写入路径 " + path + " 的键值对 " + kv.toString(keyType, valueType)); // 调试日志
        }
    }

    private void updateMinKey(KeyValue kv) {
        if (minKey == null) {
            minKey = keySerializer.toBinaryRow(kv.key()).copy(); // 将键序列化为二进制并复制
        }
    }

    private void updateMaxKey(KeyValue kv) {
        maxKey = kv.key(); // 设置当前键为最大键
    }

    private void updateMinSeqNumber(KeyValue kv) {
        minSeqNumber = Math.min(minSeqNumber, kv.sequenceNumber()); // 更新最小序列号
    }

    private void updateMaxSeqNumber(KeyValue kv) {
        maxSeqNumber = Math.max(maxSeqNumber, kv.sequenceNumber()); // 更新最大序列号
    }

    @Override
    @Nullable
    public DataFileMeta result() throws IOException {
        if (recordCount() == 0) {
            return null; // 如果没有记录，返回 null
        }

        SimpleColStats[] rowStats = fieldStats(); // 获取字段统计信息
        int numKeyFields = keyType.getFieldCount(); // 获取键字段的数量

        SimpleColStats[] keyFieldStats = Arrays.copyOfRange(rowStats, 0, numKeyFields); // 获取键字段的统计信息
        SimpleStats keyStats = keyStatsConverter.toBinary(keyFieldStats); // 将键字段统计信息转换为二进制

        SimpleColStats[] valFieldStats = Arrays.copyOfRange(rowStats, numKeyFields + 2, rowStats.length); // 获取值字段的统计信息
        SimpleStats valueStats = valueStatsConverter.toBinary(valFieldStats); // 将值字段统计信息转换为二进制

        return new DataFileMeta( // 创建 DataFileMeta 对象
                path.getName(), // 文件名
                fileIO.getFileSize(path), // 文件大小
                recordCount(), // 记录数量
                minKey, // 最小键
                keySerializer.toBinaryRow(maxKey).copy(), // 最大键
                keyStats, // 键的统计信息
                valueStats, // 值的统计信息
                minSeqNumber, // 最小序列号
                maxSeqNumber, // 最大序列号
                schemaId, // 模式 ID
                level, // 文件级别
                deleteRecordCount, // 删除记录计数
                null, // 当前暂未启用主键表的文件过滤功能（例如删除表）
                fileSource); // 文件来源
    }
}
