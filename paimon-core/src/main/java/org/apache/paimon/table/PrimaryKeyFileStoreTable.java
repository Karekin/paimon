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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.PrimaryKeyIcebergCommitCallback;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.KeyValueTableRead;
import org.apache.paimon.table.source.MergeTreeSplitGenerator;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/**
 * {@link FileStoreTable} 的实现类，专门用于带主键的表。
 * 该类管理基于 Key-Value 存储的文件存储表，提供对数据的读取、写入、查询、过滤等操作。
 */
class PrimaryKeyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L; // 序列化版本号

    /** 延迟初始化的 KeyValueFileStore 对象 */
    private transient KeyValueFileStore lazyStore;

    /**
     * 构造方法，创建带主键的文件存储表。
     *
     * @param fileIO 文件 I/O 处理对象
     * @param path 表的存储路径
     * @param tableSchema 表的 Schema 信息
     */
    PrimaryKeyFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        this(fileIO, path, tableSchema, CatalogEnvironment.empty());
    }

    /**
     * 带有 CatalogEnvironment 的构造方法
     *
     * @param fileIO 文件 I/O 处理对象
     * @param path 表的存储路径
     * @param tableSchema 表的 Schema 信息
     * @param catalogEnvironment 目录环境，用于管理元数据
     */
    PrimaryKeyFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, path, tableSchema, catalogEnvironment);
    }

    /**
     * 获取存储引擎（KeyValueFileStore），用于存储和管理数据。
     * 采用懒加载模式，首次调用时初始化存储引擎。
     *
     * @return KeyValueFileStore 实例
     */
    @Override
    public KeyValueFileStore store() {
        if (lazyStore == null) {
            // 获取表的逻辑行类型
            RowType rowType = tableSchema.logicalRowType();
            // 从表 schema 选项中获取配置参数
            Options conf = Options.fromMap(tableSchema.options());
            CoreOptions options = new CoreOptions(conf);
            // 获取主键字段提取器
            KeyValueFieldsExtractor extractor =
                    PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR;

            // 创建合并函数工厂
            MergeFunctionFactory<KeyValue> mfFactory =
                    PrimaryKeyTableUtils.createMergeFunctionFactory(tableSchema, extractor);

            // 如果需要查找功能，则包装合并函数工厂
            if (options.needLookup()) {
                mfFactory =
                        LookupMergeFunction.wrap(
                                mfFactory, new RowType(extractor.keyFields(tableSchema)), rowType);
            }

            // 初始化 KeyValueFileStore
            lazyStore =
                    new KeyValueFileStore(
                            fileIO(),
                            schemaManager(),
                            tableSchema,
                            tableSchema.crossPartitionUpdate(),
                            options,
                            tableSchema.logicalPartitionType(),
                            PrimaryKeyTableUtils.addKeyNamePrefix(
                                    tableSchema.logicalBucketKeyType()),
                            new RowType(extractor.keyFields(tableSchema)),
                            rowType,
                            extractor,
                            mfFactory,
                            name(),
                            catalogEnvironment);
        }
        return lazyStore;
    }

    /**
     * 生成数据分片（Split），用于优化读取性能。
     *
     * @return SplitGenerator 实例
     */
    @Override
    protected SplitGenerator splitGenerator() {
        CoreOptions options = store().options();
        return new MergeTreeSplitGenerator(
                store().newKeyComparator(),
                options.splitTargetSize(),
                options.splitOpenFileCost(),
                options.deletionVectorsEnabled(),
                options.mergeEngine());
    }

    /**
     * 判断是否支持流式读取覆盖写入（streaming read overwrite）。
     *
     * @return true 支持；false 不支持
     */
    @Override
    public boolean supportStreamingReadOverwrite() {
        return new CoreOptions(tableSchema.options()).streamingReadOverwrite();
    }

    /**
     * 过滤器推送下推策略，仅支持主键字段的过滤。
     *
     * @return 过滤器消费者
     */
    @Override
    protected BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer() {
        return (scan, predicate) -> {
            // 仅对主键字段执行过滤推送
            List<Predicate> keyFilters =
                    pickTransformFieldMapping(
                            splitAnd(predicate),
                            tableSchema.fieldNames(),
                            tableSchema.trimmedPrimaryKeys());
            if (keyFilters.size() > 0) {
                ((KeyValueFileStoreScan) scan).withKeyFilter(and(keyFilters));
            }

            // 在分桶级别支持值过滤
            ((KeyValueFileStoreScan) scan).withValueFilter(predicate);
        };
    }

    /**
     * 创建新的表读取对象（TableRead）。
     *
     * @return InnerTableRead 实例
     */
    @Override
    public InnerTableRead newRead() {
        return new KeyValueTableRead(
                () -> store().newRead(), () -> store().newBatchRawFileRead(), schema());
    }

    /**
     * 创建新的表写入对象（TableWrite）。
     *
     * @param commitUser 提交用户
     * @return TableWriteImpl 实例
     */
    @Override
    public TableWriteImpl<KeyValue> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    /**
     * 创建新的表写入对象，并支持 ManifestCache 过滤。
     *
     * @param commitUser 提交用户
     * @param manifestFilter ManifestCache 过滤器（可为空）
     * @return TableWriteImpl 实例
     */
    @Override
    public TableWriteImpl<KeyValue> newWrite(
            String commitUser, ManifestCacheFilter manifestFilter) {
        KeyValue kv = new KeyValue();
        return new TableWriteImpl<>(
                rowType(),
                store().newWrite(commitUser, manifestFilter),
                createRowKeyExtractor(),
                (record, rowKind) ->
                        kv.replace(
                                record.primaryKey(),
                                KeyValue.UNKNOWN_SEQUENCE,
                                rowKind,
                                record.row()),
                rowKindGenerator(),
                CoreOptions.fromMap(tableSchema.options()).ignoreDelete());
    }

    /**
     * 创建本地表查询对象（LocalTableQuery）。
     *
     * @return LocalTableQuery 实例
     */
    @Override
    public LocalTableQuery newLocalTableQuery() {
        return new LocalTableQuery(this);
    }

    /**
     * 创建提交回调列表，在提交事务时执行。
     *
     * @param commitUser 提交用户
     * @return 提交回调列表
     */
    @Override
    protected List<CommitCallback> createCommitCallbacks(String commitUser) {
        List<CommitCallback> callbacks = super.createCommitCallbacks(commitUser);
        CoreOptions options = coreOptions();

        // 如果启用了 Iceberg 兼容模式，则添加 Iceberg 提交回调
        if (options.metadataIcebergCompatible()) {
            callbacks.add(new PrimaryKeyIcebergCommitCallback(this, commitUser));
        }

        return callbacks;
    }
}

