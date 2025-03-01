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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.operation.ScanBucketFilter;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.types.RowType;

import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;


/**
 * {@link FileStore} 的实现类，用于读取和写入 {@link InternalRow} 表数据。
 * 该类继承自 {@link AbstractFileStore}，并实现了追加写入的文件存储逻辑。
 * */
public class AppendOnlyFileStore extends AbstractFileStore<InternalRow> {

    /** 桶键的类型定义。 */
    private final RowType bucketKeyType;

    /** 行数据的类型定义。 */
    private final RowType rowType;

    /** 表的名称。 */
    private final String tableName;

    /**
     * 构造函数，用于初始化追加写入的文件存储。
     * @param fileIO 文件 I/O 组件，用于管理文件读写。
     * @param schemaManager Schema 管理器，用于管理表的 Schema。
     * @param schema 表的 Schema，包含表的结构信息。
     * @param options 核心选项，包含存储的配置参数。
     * @param partitionType 分区键的类型定义。
     * @param bucketKeyType 桶键的类型定义。
     * @param rowType 行数据的类型定义。
     * @param tableName 表的名称。
     * @param catalogEnvironment 目录环境，用于与目录服务交互。
     * */
    public AppendOnlyFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            CoreOptions options,
            RowType partitionType,
            RowType bucketKeyType,
            RowType rowType,
            String tableName,
            CatalogEnvironment catalogEnvironment) {
        // 调用父类构造函数，初始化文件存储
        super(fileIO, schemaManager, schema, options, partitionType, catalogEnvironment);
        this.bucketKeyType = bucketKeyType; // 设置桶键类型
        this.rowType = rowType; // 设置行数据类型
        this.tableName = tableName; // 设置表名称
    }

    /**
     * 获取当前文件存储的桶模式。
     * 如果配置的桶数量为 -1，则表示不使用桶模式；否则使用固定哈希桶。
     * @return 当前文件存储的桶模式。
     * */
    @Override
    public BucketMode bucketMode() {
        return options.bucket() == -1 ? BucketMode.BUCKET_UNAWARE : BucketMode.HASH_FIXED;
    }

    /**
     * 创建一个新的数据扫描器，用于查询表数据。
     * @return 数据扫描器对象。
     * */
    @Override
    public AppendOnlyFileStoreScan newScan() {
        return newScan(false); // 调用带参数的 newScan 方法
    }

    /**
     * 创建一个新的原始文件分片读取器，用于读取文件分片。
     * @return 文件分片读取器对象。
     * */
    @Override
    public RawFileSplitRead newRead() {
        // 创建文件分片读取器，并传入相关参数
        return new RawFileSplitRead(
                fileIO,
                schemaManager,
                schema,
                rowType,
                FileFormatDiscover.of(options),
                pathFactory(),
                options.fileIndexReadEnabled());
    }

    /**
     * 创建一个新的数据写入器，用于向表中写入数据。
     * @param commitUser 提交数据的用户信息。
     * @return 数据写入器对象。
     * */
    @Override
    public AppendOnlyFileStoreWrite newWrite(String commitUser) {
        return newWrite(commitUser, null); // 调用带参数的 newWrite 方法
    }

    /**
     * 创建一个新的数据写入器，可以选择是否使用 ManifestCacheFilter。
     * @param commitUser 提交数据的用户信息。
     * @param manifestFilter ManifestCacheFilter，可选。
     * @return 数据写入器对象。
     * */
    @Override
    public AppendOnlyFileStoreWrite newWrite(
            String commitUser, ManifestCacheFilter manifestFilter) {
        return new AppendOnlyFileStoreWrite(
                fileIO,
                newRead(), // 使用当前对象创建的文件分片读取器
                schema.id(),
                commitUser,
                rowType,
                pathFactory(),
                snapshotManager(),
                newScan(true).withManifestCacheFilter(manifestFilter), // 创建扫描器并添加 ManifestCacheFilter
                options,
                bucketMode(),
                options.deletionVectorsEnabled() // 是否启用删除向量维护
                        ? DeletionVectorsMaintainer.factory(newIndexFileHandler()) // 如果启用，则创建删除向量维护者
                        : null,
                tableName);
    }

    /**
     * 创建一个新的数据扫描器，可以选择是否用于写入操作。
     * @param forWrite 是否用于写入操作。
     * @return 数据扫描器对象。
     * */
    private AppendOnlyFileStoreScan newScan(boolean forWrite) {
        // 创建一个 ScanBucketFilter 匿名类，用于过滤桶键
        ScanBucketFilter bucketFilter = new ScanBucketFilter(bucketKeyType) {
            @Override
            public void pushdown(Predicate predicate) {
                if (bucketMode() != BucketMode.HASH_FIXED) {
                    return; // 如果不是固定哈希桶模式，不进行过滤
                }

                if (bucketKeyType.getFieldCount() == 0) {
                    return; // 如果桶键类型没有字段，不进行过滤
                }

                // 分解过滤条件，并选择符合条件的字段
                List<Predicate> bucketFilters = pickTransformFieldMapping(
                        splitAnd(predicate), // 拆分 and 过滤条件
                        rowType.getFieldNames(), // 获取行数据的字段名
                        bucketKeyType.getFieldNames()); // 获取桶键的字段名

                if (bucketFilters.size() > 0) {
                    setBucketKeyFilter(and(bucketFilters)); // 设置桶键过滤条件
                }
            }
        };

        return new AppendOnlyFileStoreScan(
                partitionType,
                bucketFilter, // 传入桶键过滤器
                snapshotManager(), // 传入快照管理器
                schemaManager,
                schema,
                manifestFileFactory(forWrite),
                manifestListFactory(forWrite),
                options.bucket(),
                forWrite, // 是否用于写入操作
                options.scanManifestParallelism(), // 扫描清单的并行度
                options.fileIndexReadEnabled()); // 是否启用文件索引读取
    }

    /**
     * 创建一个新的键比较器，当前实现返回 null。
     * @return 键比较器，当前实现返回 null。
     * */
    @Override
    public Comparator<InternalRow> newKeyComparator() {
        return null;
    }
}
