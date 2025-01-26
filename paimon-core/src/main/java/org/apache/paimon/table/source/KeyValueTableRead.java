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

package org.apache.paimon.table.source;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.splitread.IncrementalChangelogReadProvider;
import org.apache.paimon.table.source.splitread.IncrementalDiffReadProvider;
import org.apache.paimon.table.source.splitread.MergeFileSplitReadProvider;
import org.apache.paimon.table.source.splitread.RawFileSplitReadProvider;
import org.apache.paimon.table.source.splitread.SplitReadProvider;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * 一个抽象层，用于封装 {@link MergeFileSplitRead}，以便读取 {@link InternalRow}。
 *
 * <p>该类继承自 {@link AbstractDataTableRead}，并支持多种分片读取提供者的初始化与操作，
 * 包括合并文件读取、增量变更读取等。
 */
public final class KeyValueTableRead extends AbstractDataTableRead<KeyValue> {

    // 读取提供者的列表，包括支持的多种读取方式
    private final List<SplitReadProvider> readProviders;

    // 投影操作的列映射
    private int[][] projection = null;

    // 是否强制保留删除标志
    private boolean forceKeepDelete = false;

    // 筛选条件（谓词）
    private Predicate predicate = null;

    // 用于 I/O 操作的管理器
    private IOManager ioManager = null;

    /**
     * 构造函数。
     *
     * @param mergeReadSupplier 提供合并文件读取的供应器。
     * @param batchRawReadSupplier 提供原始批量文件读取的供应器。
     * @param schema 表模式（表结构）。
     */
    public KeyValueTableRead(
            Supplier<MergeFileSplitRead> mergeReadSupplier,
            Supplier<RawFileSplitRead> batchRawReadSupplier,
            TableSchema schema) {
        super(schema);

        // 初始化读取提供者列表
        this.readProviders =
                Arrays.asList(
                        new RawFileSplitReadProvider(batchRawReadSupplier, this::assignValues),
                        new MergeFileSplitReadProvider(mergeReadSupplier, this::assignValues),
                        new IncrementalChangelogReadProvider(mergeReadSupplier, this::assignValues),
                        new IncrementalDiffReadProvider(mergeReadSupplier, this::assignValues));
    }

    /**
     * 初始化所有读取提供者并返回对应的读取器列表。
     *
     * @return 已初始化的分片读取器列表。
     */
    private List<SplitRead<InternalRow>> initialized() {
        List<SplitRead<InternalRow>> readers = new ArrayList<>();
        for (SplitReadProvider readProvider : readProviders) {
            if (readProvider.initialized()) {
                readers.add(readProvider.getOrCreate());
            }
        }
        return readers;
    }

    /**
     * 为分片读取器分配相关配置值。
     *
     * @param read 分片读取器。
     */
    private void assignValues(SplitRead<InternalRow> read) {
        if (forceKeepDelete) {
            read = read.forceKeepDelete(); // 强制保留删除标志
        }
        read.withProjection(projection) // 设置投影
                .withFilter(predicate) // 设置过滤条件
                .withIOManager(ioManager); // 设置 I/O 管理器
    }

    @Override
    public void projection(int[][] projection) {
        initialized().forEach(r -> r.withProjection(projection)); // 为所有读取器设置投影
        this.projection = projection;
    }

    @Override
    public InnerTableRead forceKeepDelete() {
        initialized().forEach(SplitRead::forceKeepDelete); // 为所有读取器强制保留删除标志
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    protected InnerTableRead innerWithFilter(Predicate predicate) {
        initialized().forEach(r -> r.withFilter(predicate)); // 为所有读取器设置过滤条件
        this.predicate = predicate;
        return this;
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        initialized().forEach(r -> r.withIOManager(ioManager)); // 为所有读取器设置 I/O 管理器
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public RecordReader<InternalRow> reader(Split split) throws IOException {
        DataSplit dataSplit = (DataSplit) split;
        for (SplitReadProvider readProvider : readProviders) {
            if (readProvider.match(dataSplit, forceKeepDelete)) {
                // 根据匹配的读取提供者创建对应的读取器
                return readProvider.getOrCreate().createReader(dataSplit);
            }
        }

        throw new RuntimeException("不应该发生的情况。");
    }

    /**
     * 解包 {@link RecordReader<KeyValue>}，将其转换为 {@link RecordReader<InternalRow>}。
     *
     * @param reader 原始的 {@link RecordReader<KeyValue>}。
     * @return 转换后的 {@link RecordReader<InternalRow>}。
     */
    public static RecordReader<InternalRow> unwrap(RecordReader<KeyValue> reader) {
        return new RecordReader<InternalRow>() {

            @Nullable
            @Override
            public RecordIterator<InternalRow> readBatch() throws IOException {
                RecordIterator<KeyValue> batch = reader.readBatch();
                return batch == null ? null : new ValueContentRowDataRecordIterator(batch);
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }
}
