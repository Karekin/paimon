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

import org.apache.paimon.FileStore;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SegmentsCache;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * FileStore 上的抽象接口层，用于提供对 {@link InternalRow}（数据）的读写能力。
 * 该接口表示 Paimon 表（文件存储表），封装了底层文件存储的访问逻辑，并提供表级别的操作。
 */
public interface FileStoreTable extends DataTable {

    /**
     * 设置 Manifest 文件的缓存，用于加速元数据访问。
     *
     * @param manifestCache Manifest 文件的缓存对象
     */
    void setManifestCache(SegmentsCache<Path> manifestCache);

    /**
     * 获取表的行类型（RowType）。
     * 默认实现返回表的逻辑行类型。
     *
     * @return 表的 RowType
     */
    @Override
    default RowType rowType() {
        return schema().logicalRowType();
    }

    /**
     * 获取表的分区字段列表。
     * 默认实现返回表 schema 中定义的分区键。
     *
     * @return 分区字段列表
     */
    @Override
    default List<String> partitionKeys() {
        return schema().partitionKeys();
    }

    /**
     * 获取表的主键字段列表。
     * 默认实现返回表 schema 中定义的主键。
     *
     * @return 主键字段列表
     */
    @Override
    default List<String> primaryKeys() {
        return schema().primaryKeys();
    }

    /**
     * 获取桶分配策略（BucketSpec）。
     * 默认实现返回基于表 schema 配置的桶策略，包括桶模式、桶键、桶数量等信息。
     *
     * @return 桶分配策略
     */
    default BucketSpec bucketSpec() {
        return new BucketSpec(bucketMode(), schema().bucketKeys(), schema().numBuckets());
    }

    /**
     * 获取桶模式（BucketMode）。
     * 桶模式决定了数据如何划分到不同的 bucket 进行存储。
     *
     * @return 桶模式
     */
    default BucketMode bucketMode() {
        return store().bucketMode();
    }

    /**
     * 获取表的配置信息（Options）。
     * 默认实现返回表 schema 中定义的所有配置项。
     *
     * @return 表的配置信息（键值对）
     */
    @Override
    default Map<String, String> options() {
        return schema().options();
    }

    /**
     * 获取表的注释信息。
     *
     * @return 可选的表注释，如果 schema 中没有定义，则返回空
     */
    @Override
    default Optional<String> comment() {
        return Optional.ofNullable(schema().comment());
    }

    /**
     * 获取表的 schema 信息。
     *
     * @return 表的 TableSchema
     */
    TableSchema schema();

    /**
     * 获取表对应的文件存储引擎（FileStore）。
     * 该方法用于访问底层文件存储，实现数据的读写。
     *
     * @return 文件存储对象
     */
    FileStore<?> store();

    /**
     * 获取表的 Catalog 环境（CatalogEnvironment）。
     * 该方法返回 Catalog 相关的环境信息，例如存储路径、版本管理等。
     *
     * @return 目录环境对象
     */
    CatalogEnvironment catalogEnvironment();

    /**
     * 创建表的副本，并应用动态选项。
     *
     * @param dynamicOptions 需要应用的新选项
     * @return 复制后的 FileStoreTable 实例
     */
    @Override
    FileStoreTable copy(Map<String, String> dynamicOptions);

    /**
     * 创建表的副本，并应用新的 TableSchema。
     *
     * @param newTableSchema 需要应用的新 schema
     * @return 复制后的 FileStoreTable 实例
     */
    FileStoreTable copy(TableSchema newTableSchema);

    /**
     * 复制表实例，但不包含时间旅行（Time Travel）相关的选项。
     * 即便存在时间旅行选项，也不会影响表的 schema。
     *
     * @param dynamicOptions 需要应用的选项
     * @return 复制后的 FileStoreTable 实例
     */
    FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions);

    /**
     * 复制表实例，并更新到最新 schema。
     * 该方法会覆盖旧的选项，可能导致某些选项丢失（TODO: 需要优化）。
     *
     * @return 复制后的 FileStoreTable 实例
     */
    FileStoreTable copyWithLatestSchema();

    /**
     * 创建一个新的表写入实例（TableWriteImpl）。
     *
     * @param commitUser 提交用户（写入者）
     * @return 表写入对象
     */
    @Override
    TableWriteImpl<?> newWrite(String commitUser);

    /**
     * 创建一个新的表写入实例，并支持 ManifestCache 过滤。
     *
     * @param commitUser 提交用户
     * @param manifestFilter ManifestCache 过滤器（可选）
     * @return 表写入对象
     */
    TableWriteImpl<?> newWrite(String commitUser, ManifestCacheFilter manifestFilter);

    /**
     * 创建一个新的表提交实例（TableCommitImpl）。
     *
     * @param commitUser 提交用户
     * @return 表提交对象
     */
    @Override
    TableCommitImpl newCommit(String commitUser);

    /**
     * 创建一个本地表查询实例（LocalTableQuery）。
     *
     * @return 本地表查询对象
     */
    LocalTableQuery newLocalTableQuery();

    /**
     * 获取数据文件的 Schema 统计信息（SimpleStats）。
     *
     * @param dataFileMeta 数据文件元数据
     * @return 数据文件的统计信息
     */
    default SimpleStats getSchemaFieldStats(DataFileMeta dataFileMeta) {
        return dataFileMeta.valueStats();
    }

    /**
     * 判断表是否支持流式读取覆盖写入（Streaming Read Overwrite）。
     *
     * @return true 支持，false 不支持
     */
    boolean supportStreamingReadOverwrite();

    /**
     * 创建行键提取器（RowKeyExtractor）。
     * 该提取器用于从行数据中提取主键。
     *
     * @return 行键提取器实例
     */
    RowKeyExtractor createRowKeyExtractor();

    /**
     * 根据分支名称获取 {@link DataTable} 对象。
     * 该方法不会保留当前表中的动态选项，切换到指定分支后，新的表实例将具有分支的最新 schema。
     *
     * @param branchName 需要切换到的分支名称
     * @return 切换后的 FileStoreTable 实例
     */
    @Override
    FileStoreTable switchToBranch(String branchName);
}

