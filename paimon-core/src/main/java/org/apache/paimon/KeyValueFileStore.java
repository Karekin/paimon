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

package org.apache.paimon;

import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.HashIndexMaintainer;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.operation.KeyValueFileStoreWrite;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.operation.ScanBucketFilter;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.UserDefinedSeqComparator;
import org.apache.paimon.utils.ValueEqualiserSupplier;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link FileStore} for querying and updating {@link KeyValue}s. */
/**
* @授课老师: 码界探索
* @微信: 252810631
* @版权所有: 请尊重劳动成果
* KeyValueFileStore 用来查询、更新FileStore文件存储
*/
public class KeyValueFileStore extends AbstractFileStore<KeyValue> {

    private final boolean crossPartitionUpdate;
    private final RowType bucketKeyType;
    private final RowType keyType;
    private final RowType valueType;
    private final KeyValueFieldsExtractor keyValueFieldsExtractor;
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    private final Supplier<RecordEqualiser> valueEqualiserSupplier;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final String tableName;

    public KeyValueFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            boolean crossPartitionUpdate,
            CoreOptions options,
            RowType partitionType,
            RowType bucketKeyType,
            RowType keyType,
            RowType valueType,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            MergeFunctionFactory<KeyValue> mfFactory,
            String tableName,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, schemaManager, schema, options, partitionType, catalogEnvironment);
        this.crossPartitionUpdate = crossPartitionUpdate;
        this.bucketKeyType = bucketKeyType;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keyValueFieldsExtractor = keyValueFieldsExtractor;
        this.mfFactory = mfFactory;
        this.keyComparatorSupplier = new KeyComparatorSupplier(keyType);
        this.valueEqualiserSupplier = new ValueEqualiserSupplier(valueType);
        this.tableName = tableName;
    }

    @Override
    public BucketMode bucketMode() {
        if (options.bucket() == -1) {
            return crossPartitionUpdate ? BucketMode.CROSS_PARTITION : BucketMode.HASH_DYNAMIC;
        } else {
            checkArgument(!crossPartitionUpdate);
            return BucketMode.HASH_FIXED;
        }
    }

    @Override
    public KeyValueFileStoreScan newScan() {
        return newScan(false);
    }

    @Override
    public MergeFileSplitRead newRead() {
        return new MergeFileSplitRead(
                options,
                schema,
                keyType,
                valueType,
                newKeyComparator(),
                mfFactory,
                newReaderFactoryBuilder());
    }

    public RawFileSplitRead newBatchRawFileRead() {
        return new RawFileSplitRead(
                fileIO,
                schemaManager,
                schema,
                valueType,
                FileFormatDiscover.of(options),
                pathFactory(),
                options.fileIndexReadEnabled());
    }

    public KeyValueFileReaderFactory.Builder newReaderFactoryBuilder() {
        return KeyValueFileReaderFactory.builder(
                fileIO,
                schemaManager,
                schema,
                keyType,
                valueType,
                FileFormatDiscover.of(options),
                pathFactory(),
                keyValueFieldsExtractor,
                options);
    }

    @Override
    public KeyValueFileStoreWrite newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 创建一个新的键值文件存储写入器（KeyValueFileStoreWrite）。
    *  根据当前的配置和分桶模式，构建一个用于写入键值对到文件存储的写入器实例。
    */
    @Override
    public KeyValueFileStoreWrite newWrite(String commitUser, ManifestCacheFilter manifestFilter) {
        // 索引维护器的工厂类，初始化为null，根据分桶模式可能会被赋值
        IndexMaintainer.Factory<KeyValue> indexFactory = null;
        // 如果当前分桶模式是动态哈希分桶
        if (bucketMode() == BucketMode.HASH_DYNAMIC) {
            // 则创建一个哈希索引维护器的工厂类，并传入新的索引文件处理器
            indexFactory = new HashIndexMaintainer.Factory(newIndexFileHandler());
        }
        // 删除向量维护器的工厂类，初始化为null，
        DeletionVectorsMaintainer.Factory deletionVectorsMaintainerFactory = null;
        // 如果启用了删除向量
        if (options.deletionVectorsEnabled()) {
            // 则创建一个删除向量维护器的工厂类，并传入新的索引文件处理器
            deletionVectorsMaintainerFactory =
                    new DeletionVectorsMaintainer.Factory(newIndexFileHandler());
        }
        // 返回一个新的键值文件存储写入器实例
        return new KeyValueFileStoreWrite(
                fileIO,// 文件IO接口，用于读写文件
                schemaManager,// 模式管理器，用于管理数据库或表的模式
                schema,// 当前的模式信息
                commitUser,// 提交操作的用户
                partitionType,// 分区类型
                keyType,// 键的类型
                valueType,// 值的类型
                keyComparatorSupplier, // 键的比较器供应器，用于比较键
                () -> UserDefinedSeqComparator.create(valueType, options),// 值序列的比较器创建函数，用于比较值序列
                valueEqualiserSupplier,// 值的等价器供应器，用于判断值是否等价
                mfFactory,// Manifest文件的工厂类，用于创建Manifest文件
                pathFactory(),// 路径工厂类，用于生成文件路径
                format2PathFactory(),// 格式到路径的工厂类，用于根据格式生成路径
                snapshotManager(),// 快照管理器，用于管理快照
                newScan(true).withManifestCacheFilter(manifestFilter),// 创建一个新的扫描器，并应用manifest过滤器
                indexFactory,// 索引维护器的工厂类（可能为null）
                deletionVectorsMaintainerFactory,// 删除向量维护器的工厂类（可能为null）
                options,// 配置选项
                keyValueFieldsExtractor,// 键值字段提取器，用于从数据中提取键值对
                tableName); // 表名
    }

    private Map<String, FileStorePathFactory> format2PathFactory() {
        Map<String, FileStorePathFactory> pathFactoryMap = new HashMap<>();
        Set<String> formats = new HashSet<>(options.fileFormatPerLevel().values());
        formats.add(options.fileFormat().getFormatIdentifier());
        formats.forEach(
                format ->
                        pathFactoryMap.put(
                                format,
                                new FileStorePathFactory(
                                        options.path(),
                                        partitionType,
                                        options.partitionDefaultName(),
                                        format)));
        return pathFactoryMap;
    }

    private KeyValueFileStoreScan newScan(boolean forWrite) {
        ScanBucketFilter bucketFilter =
                new ScanBucketFilter(bucketKeyType) {
                    @Override
                    public void pushdown(Predicate keyFilter) {
                        if (bucketMode() != BucketMode.HASH_FIXED) {
                            return;
                        }

                        List<Predicate> bucketFilters =
                                pickTransformFieldMapping(
                                        splitAnd(keyFilter),
                                        keyType.getFieldNames(),
                                        bucketKeyType.getFieldNames());
                        if (bucketFilters.size() > 0) {
                            setBucketKeyFilter(and(bucketFilters));
                        }
                    }
                };
        return new KeyValueFileStoreScan(
                partitionType,
                bucketFilter,
                snapshotManager(),
                schemaManager,
                schema,
                keyValueFieldsExtractor,
                manifestFileFactory(forWrite),
                manifestListFactory(forWrite),
                options.bucket(),
                forWrite,
                options.scanManifestParallelism(),
                options.deletionVectorsEnabled(),
                options.mergeEngine(),
                options.changelogProducer());
    }

    @Override
    public Comparator<InternalRow> newKeyComparator() {
        return keyComparatorSupplier.get();
    }
}
