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

/**
 * {@link FileStore} for querying and updating {@link KeyValue}s.
 * 利用 FileStore 实现存储、查询、更新键值对
 */
public class KeyValueFileStore extends AbstractFileStore<KeyValue> {
    //是否跨分区更新
    private final boolean crossPartitionUpdate;
    //桶键的类型
    private final RowType bucketKeyType;
    //键的类型
    private final RowType keyType;
    //值的类型
    private final RowType valueType;
    //键值对字段提取器
    private final KeyValueFieldsExtractor keyValueFieldsExtractor;
    //键比较器供应器
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    //值等价器供应器
    private final Supplier<RecordEqualiser> valueEqualiserSupplier;
    //合并函数工厂
    private final MergeFunctionFactory<KeyValue> mfFactory;
    //当前表名
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
        //如果选项中的桶数为 -1，则分桶模式由跨分区更新决定，否则为固定哈希分桶
        if (options.bucket() == -1) {
            return crossPartitionUpdate ? BucketMode.CROSS_PARTITION : BucketMode.HASH_DYNAMIC;
        } else {
            checkArgument(!crossPartitionUpdate);
            return BucketMode.HASH_FIXED;
        }
    }

    @Override
    public KeyValueFileStoreScan newScan() {
        //创建一个新的扫描器
        return newScan(false);
    }

    @Override
    public MergeFileSplitRead newRead() {
        //创建一个新的读取器
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
        //创建一个新的批量原始文件读取器
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
        //创建一个新的文件读取器工厂构建器
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
        //创建一个新的写入器
        return newWrite(commitUser, null);
    }

    /**
     * 创建一个新的键值文件存储写入器（KeyValueFileStoreWrite）。
     * 根据当前的配置和分桶模式，构建一个用于写入键值对到文件存储的写入器实例。
     */
    @Override
    public KeyValueFileStoreWrite newWrite(String commitUser, ManifestCacheFilter manifestFilter) {
        // 初始化索引维护器工厂
        IndexMaintainer.Factory<KeyValue> indexFactory = null;
        if (bucketMode() == BucketMode.HASH_DYNAMIC) {
            // 如果分桶模式是动态哈希分桶，则创建哈希索引维护器工厂
            indexFactory = new HashIndexMaintainer.Factory(newIndexFileHandler());
        }
        // 初始化删除向量维护器工厂
        DeletionVectorsMaintainer.Factory deletionVectorsMaintainerFactory = null;
        if (options.deletionVectorsEnabled()) {
            // 如果启用了删除向量，则创建删除向量维护器工厂
            deletionVectorsMaintainerFactory = new DeletionVectorsMaintainer.Factory(newIndexFileHandler());
        }
        // 返回一个新的写入器实例
        return new KeyValueFileStoreWrite(
                fileIO, // 文件 IO 接口
                schemaManager, // 模式管理器
                schema, // 当前的表模式
                commitUser, // 提交用户
                partitionType, // 分区类型
                keyType, // 键的类型
                valueType, // 值的类型
                keyComparatorSupplier, // 键比较器供应器
                () -> UserDefinedSeqComparator.create(valueType, options), // 值比较器
                valueEqualiserSupplier, // 值等价器供应器
                mfFactory, // 合并函数工厂
                pathFactory(), // 路径工厂
                format2PathFactory(), // 格式到路径工厂的映射
                snapshotManager(), // 快照管理器
                newScan(true).withManifestCacheFilter(manifestFilter), // 创建带有过滤器的扫描器
                indexFactory, // 索引维护器工厂
                deletionVectorsMaintainerFactory, // 删除向量维护器工厂
                options, // 配置选项
                keyValueFieldsExtractor, // 键值字段提取器
                tableName); // 表名
    }

    // 创建格式到路径工厂的映射
    private Map<String, FileStorePathFactory> format2PathFactory() {
        Map<String, FileStorePathFactory> pathFactoryMap = new HashMap<>();
        // 获取所有文件格式
        Set<String> formats = new HashSet<>(options.fileFormatPerLevel().values());
        formats.add(options.fileFormat().getFormatIdentifier());
        // 遍历所有格式并创建对应的路径工厂
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

    // 创建一个新的扫描器
    private KeyValueFileStoreScan newScan(boolean forWrite) {
        // 创建一个桶筛选过滤器
        ScanBucketFilter bucketFilter = new ScanBucketFilter(bucketKeyType) {
            @Override
            public void pushdown(Predicate keyFilter) {
                if (bucketMode() != BucketMode.HASH_FIXED) {
                    return;
                }
                // 提取键过滤器
                List<Predicate> bucketFilters = pickTransformFieldMapping(
                        splitAnd(keyFilter),
                        keyType.getFieldNames(),
                        bucketKeyType.getFieldNames());
                if (!bucketFilters.isEmpty()) {
                    setBucketKeyFilter(and(bucketFilters));
                }
            }
        };
        // 返回新的扫描器实例
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
        // 返回键比较器
        return keyComparatorSupplier.get();
    }
}
