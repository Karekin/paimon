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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Function;

/**
 * 一个 {@link SingleFileWriter} 的扩展类，除了写入文件之外，还为每个写入的字段生成统计信息。
 *
 * @param <T> 要写入的记录类型。
 * @param <R> 在写完文件后生成的结果类型。
 */
public abstract class StatsCollectingSingleFileWriter<T, R> extends SingleFileWriter<T, R> {

    @Nullable private final SimpleStatsExtractor simpleStatsExtractor; // 简单统计信息提取工具
    @Nullable private SimpleStatsCollector simpleStatsCollector = null; // 简单统计信息收集器

    public StatsCollectingSingleFileWriter(
            FileIO fileIO, // 文件 I/O 实例
            FormatWriterFactory factory, // 文件写入工厂实例
            Path path, // 文件路径
            Function<T, InternalRow> converter, // 数据转换器
            RowType writeSchema, // 写入的 schema
            @Nullable SimpleStatsExtractor simpleStatsExtractor, // 简单统计信息提取工具（可选）
            String compression, // 压缩方式
            SimpleColStatsCollector.Factory[] statsCollectors, // 统计信息收集器工厂数组
            boolean asyncWrite) { // 是否异步写入
        super(fileIO, factory, path, converter, compression, asyncWrite);
        this.simpleStatsExtractor = simpleStatsExtractor;

        // 如果没有传入 simpleStatsExtractor，则初始化一个 SimpleStatsCollector
        if (this.simpleStatsExtractor == null) {
            this.simpleStatsCollector = new SimpleStatsCollector(writeSchema, statsCollectors);
        }

        // 检查统计信息收集器的数量是否与写入 schema 的字段数量一致
        Preconditions.checkArgument(
                statsCollectors.length == writeSchema.getFieldCount(),
                "统计信息收集器的字段数量必须与写入 schema 的字段数量一致。");
    }

    @Override
    public void write(T record) throws IOException {
        // 调用父类的 writeImpl 方法，实际写入数据并返回 InternalRow 格式的行数据
        InternalRow rowData = writeImpl(record);

        // 如果启用了 simpleStatsCollector 且未被禁用，则收集当前行的统计信息
        if (simpleStatsCollector != null && !simpleStatsCollector.isDisabled()) {
            simpleStatsCollector.collect(rowData);
        }
    }

    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        // 检查是否启用了 simpleStatsExtractor，否则抛出异常
        // 因为如果没有 simpleStatsExtractor，则无法处理 bundle 数据
        Preconditions.checkState(
                simpleStatsExtractor != null,
                "如果未启用 simpleStatsExtractor，则无法写入 bundle 数据，可能会丢失所有统计信息。");

        // 调用父类的 writeBundle 方法，处理 bundle 数据
        super.writeBundle(bundle);
    }

    public SimpleColStats[] fieldStats() throws IOException {
        // 确保 writer 已关闭后才能访问统计信息
        Preconditions.checkState(closed, "只有在 writer 关闭后才能访问统计信息。");

        // 如果启用了 simpleStatsExtractor，则从文件中提取统计信息
        if (simpleStatsExtractor != null) {
            return simpleStatsExtractor.extract(fileIO, path);
        } else {
            // 否则从 simpleStatsCollector 中提取统计信息
            return simpleStatsCollector.extract();
        }
    }
}
