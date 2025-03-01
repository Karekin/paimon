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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.stats.SimpleStatsConverters;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.MergeEngine.AGGREGATE;
import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;
import static org.apache.paimon.CoreOptions.MergeEngine.PARTIAL_UPDATE;

/**
 * 为 {@link KeyValueFileStore} 提供的文件存储扫描器.
 * 文件存储扫描器用于扫描和过滤文件存储中的文件，以支持查询和更新操作。
 * 通过继承 {@link AbstractFileScan}，并实现了相关的过滤和统计功能。
 */
public class KeyValueFileStoreScan extends AbstractFileStoreScan {

    // 简单统计转换器，用于键字段的统计信息
    private final SimpleStatsConverters fieldKeyStatsConverters;

    // 简单统计转换器，用于值字段的统计信息
    private final SimpleStatsConverters fieldValueStatsConverters;

    // 键过滤条件
    private Predicate keyFilter;

    // 值过滤条件
    private Predicate valueFilter;

    // 是否启用删除向量
    private final boolean deletionVectorsEnabled;

    // 合并引擎
    private final MergeEngine mergeEngine;

    // 表示生产变更日志的方式
    private final ChangelogProducer changelogProducer;

    public KeyValueFileStoreScan(
            RowType partitionType,                          // 分区类型
            ScanBucketFilter bucketFilter,                 // 桶筛选过滤器
            SnapshotManager snapshotManager,               // 快照管理器
            SchemaManager schemaManager,                   // 模式管理器
            TableSchema schema,                            // 表模式
            KeyValueFieldsExtractor keyValueFieldsExtractor, // 键值字段提取器
            ManifestFile.Factory manifestFileFactory,      // 表示文件工厂
            ManifestList.Factory manifestListFactory,      // 表示列表工厂
            int numOfBuckets,                              // 桶的数量
            boolean checkNumOfBuckets,                     // 是否检查桶的数量
            Integer scanManifestParallelism,               // 扫描清单并行度
            boolean deletionVectorsEnabled,                // 是否启用删除向量
            MergeEngine mergeEngine,                       // 合并引擎
            ChangelogProducer changelogProducer) {         // 变更日志生产者
        super(
                partitionType,                          // 分区类型
                bucketFilter,                           // 桶筛选过滤器
                snapshotManager,                        // 快照管理器
                schemaManager,                          // 模式管理器
                schema,                                 // 表模式
                manifestFileFactory,                    // 表示文件工厂
                manifestListFactory,                    // 表示列表工厂
                numOfBuckets,                           // 桶的数量
                checkNumOfBuckets,                      // 是否检查桶的数量
                scanManifestParallelism);               // 扫描清单并行度
        // 初始化用于键字段的统计转换器
        this.fieldKeyStatsConverters =
                new SimpleStatsConverters(
                        sid -> keyValueFieldsExtractor.keyFields(scanTableSchema(sid)), // 根据给定的模式ID提取键字段
                        schema.id()); // 表模式的ID
        // 初始化用于值字段的统计转换器
        this.fieldValueStatsConverters =
                new SimpleStatsConverters(
                        sid -> keyValueFieldsExtractor.valueFields(scanTableSchema(sid)), // 根据给定的模式ID提取值字段
                        schema.id()); // 表模式的ID
        this.deletionVectorsEnabled = deletionVectorsEnabled; // 是否启用删除向量
        this.mergeEngine = mergeEngine; // 合并引擎
        this.changelogProducer = changelogProducer; // 变更日志生产者
    }

    /**
     * 设置键过滤条件
     *
     * @param predicate 键过滤条件
     * @return 当前扫描器实例
     */
    public KeyValueFileStoreScan withKeyFilter(Predicate predicate) {
        this.keyFilter = predicate; // 设置键过滤条件
        this.bucketKeyFilter.pushdown(predicate); // 将过滤条件传递到桶筛选过滤器
        return this; // 返回当前扫描器实例
    }

    /**
     * 设置值过滤条件
     *
     * @param predicate 值过滤条件
     * @return 当前扫描器实例
     */
    public KeyValueFileStoreScan withValueFilter(Predicate predicate) {
        this.valueFilter = predicate; // 设置值过滤条件
        return this; // 返回当前扫描器实例
    }

    /**
     * 根据统计信息过滤文件清单条目
     *
     * @param entry 文件清单条目
     * @return 如果文件清单条目应被保留，则返回 true
     */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        Predicate filter = null; // 过滤条件
        SimpleStatsConverter serializer = null; // 简单统计转换器
        SimpleStats stats = null; // 简单统计

        // 如果启用了值过滤，并且当前文件的有效级别大于0
        if (isValueFilterEnabled(entry)) {
            filter = this.valueFilter; // 设置过滤条件为值过滤条件
            serializer = this.fieldValueStatsConverters.getOrCreate(entry.file().schemaId()); // 获取值字段的统计转换器
            stats = entry.file().valueStats(); // 获取文件的值统计信息
        }

        // 如果过滤条件未设置，并且键过滤条件不为空
        if (filter == null && this.keyFilter != null) {
            filter = this.keyFilter; // 设置过滤条件为键过滤条件
            serializer = this.fieldKeyStatsConverters.getOrCreate(entry.file().schemaId()); // 获取键字段的统计转换器
            stats = entry.file().keyStats(); // 获取文件的键统计信息
        }

        // 如果没有过滤条件，则认为文件应被保留
        if (filter == null) {
            return true;
        }

        // 测试过滤条件是否满足
        return filter.test(
                entry.file().rowCount(), // 文件的行数
                serializer.evolution(stats.minValues()), // 最小值演进
                serializer.evolution(stats.maxValues()), // 最大值演进
                serializer.evolution(stats.nullCounts(), entry.file().rowCount()) // 空值计数演进
        );
    }

    /**
     * 检查值过滤条件是否启用
     *
     * @param entry 文件清单条目
     * @return 如果值过滤条件启用，则返回 true
     */
    private boolean isValueFilterEnabled(ManifestEntry entry) {
        // 如果值过滤条件为空，则返回 false
        if (this.valueFilter == null) {
            return false;
        }

        // 根据扫描模式判断是否启用值过滤
        switch (this.scanMode) {
            case ALL: // 全部扫描模式
                // 如果启用了删除向量，或者合并引擎为 FIRST_ROW，并且文件的级别大于0，则启用值过滤
                return (this.deletionVectorsEnabled || this.mergeEngine == MergeEngine.FIRST_ROW) && entry.level() > 0;
            case DELTA: // 增量扫描模式
                return false;
            case CHANGELOG: // 变更日志扫描模式
                // 如果变更日志生产者为 LOOKUP 或 FULL_COMPACTION，则启用值过滤
                return this.changelogProducer == ChangelogProducer.LOOKUP || this.changelogProducer == ChangelogProducer.FULL_COMPACTION;
            default: // 其他模式
                throw new UnsupportedOperationException("不支持的扫描模式: " + this.scanMode);
        }
    }

    /**
     * 根据统计信息过滤整个桶中的文件清单条目
     *
     * @param entries 文件清单条目列表
     * @return 过滤后的文件清单条目列表
     */
    @Override
    protected List<ManifestEntry> filterWholeBucketByStats(List<ManifestEntry> entries) {
        // 如果没有值过滤条件，或者扫描模式不是全部模式，则返回原始条目列表
        if (this.valueFilter == null || this.scanMode != ScanMode.ALL) {
            return entries;
        }

        // 判断文件是否没有重叠
        if (noOverlapping(entries)) {
            // 按每个文件过滤整个桶
            return this.filterWholeBucketPerFile(entries);
        } else {
            // 过滤整个桶中的所有文件
            return this.filterWholeBucketAllFiles(entries);
        }
    }

    /**
     * 按每个文件过滤整个桶
     *
     * @param entries 文件清单条目列表
     * @return 过滤后的文件清单条目列表
     */
    private List<ManifestEntry> filterWholeBucketPerFile(List<ManifestEntry> entries) {
        List<ManifestEntry> filtered = new ArrayList<>(); // 过滤后的文件清单条目列表
        for (ManifestEntry entry : entries) {
            if (this.filterByValueFilter(entry)) { // 如果文件满足值过滤条件
                filtered.add(entry); // 添加到过滤后的列表中
            }
        }
        return filtered; // 返回过滤后的列表
    }

    /**
     * 过滤整个桶中的所有文件
     *
     * @param entries 文件清单条目列表
     * @return 过滤后的文件清单条目列表
     */
    private List<ManifestEntry> filterWholeBucketAllFiles(List<ManifestEntry> entries) {
        // 如果没有启用删除向量，或者合并引擎是 PARTIAL_UPDATE 或 AGGREGATE，则返回原始条目列表
        if (!this.deletionVectorsEnabled
                && (this.mergeEngine == MergeEngine.PARTIAL_UPDATE || this.mergeEngine == MergeEngine.AGGREGATE)) {
            return entries;
        }

        // 遍历所有条目，如果存在至少一个条目满足值过滤条件，则返回所有条目，否则返回空列表
        for (ManifestEntry entry : entries) {
            if (this.filterByValueFilter(entry)) {
                return entries; // 所有条目均保留
            }
        }
        return Collections.emptyList(); // 返回空列表
    }

    /**
     * 根据值过滤条件过滤文件清单条目
     *
     * @param entry 文件清单条目
     * @return 如果文件满足值过滤条件，则返回 true
     */
    private boolean filterByValueFilter(ManifestEntry entry) {
        SimpleStatsConverter serializer =
                this.fieldValueStatsConverters.getOrCreate(entry.file().schemaId()); // 获取值字段的统计转换器
        SimpleStats stats = entry.file().valueStats(); // 获取文件的值统计信息

        // 测试过滤条件是否满足
        return this.valueFilter.test(
                entry.file().rowCount(), // 文件的行数
                serializer.evolution(stats.minValues()), // 最小值演进
                serializer.evolution(stats.maxValues()), // 最大值演进
                serializer.evolution(stats.nullCounts(), entry.file().rowCount()) // 空值计数演进
        );
    }

    /**
     * 检查文件清单条目是否没有重叠
     *
     * @param entries 文件清单条目列表
     * @return 如果没有重叠，则返回 true
     */
    private static boolean noOverlapping(List<ManifestEntry> entries) {
        if (entries.size() <= 1) { // 如果文件数量小于等于1
            return true;
        }

        Integer previousLevel = null; // 上一个文件的有效级别

        for (ManifestEntry entry : entries) {
            int level = entry.file().level(); // 获取文件的有效级别
            if (level == 0) { // 如果有效级别为0
                return false; // 存在重叠
            }

            if (previousLevel == null) { // 如果上一个级别未初始化
                previousLevel = level;
            } else {
                if (previousLevel != level) { // 如果当前级别与上一个级别不同
                    return false; // 存在重叠
                }
            }
        }

        return true; // 没有重叠
    }
}
