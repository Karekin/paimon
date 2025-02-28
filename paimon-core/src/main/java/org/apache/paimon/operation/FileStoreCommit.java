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

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.List;
import java.util.Map;

/**
 * 文件存储提交操作接口，提供提交和覆盖功能。
 * 该接口定义了文件存储系统中与提交、清理和回滚相关的操作。
 */
public interface FileStoreCommit extends AutoCloseable {

    // 使用全局锁进行提交操作
    // 确保在提交过程中对全局资源的互斥访问
    FileStoreCommit withLock(Lock lock);

    // 是否忽略空提交
    // 如果设置为 true，则在提交时会忽略空的提交内容
    FileStoreCommit ignoreEmptyCommit(boolean ignoreEmptyCommit);

    // 设置分区过期规则
    // 用于在提交时清理过期的分区数据
    FileStoreCommit withPartitionExpire(PartitionExpire partitionExpire);

    // 从失败中恢复时，过滤出需要重试的提交
    // 根据给定的 ManifestCommittable 列表，返回需要重新提交的 Committable
    List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committables);

    // 提交更改
    // 将指定的 ManifestCommittable 提交到文件存储系统
    void commit(ManifestCommittable committable, Map<String, String> properties);

    // 提交更改并检查追加文件
    // 在提交时检查追加的文件是否存在或是否完整
    void commit(
            ManifestCommittable committable,
            Map<String, String> properties,
            boolean checkAppendFiles);

    /**
     * 覆盖指定分区的数据。
     * committable：提交的内容，包含新的数据。
     * partition：要覆盖的分区。
     * properties：提交时的附加属性。
     * 注意：如果 partition 没有包含所有分区键，可能会导致覆盖操作不完整。
     */
    void overwrite(
            Map<String, String> partition,
            ManifestCommittable committable,
            Map<String, String> properties);

    /**
     * 删除多个分区。
     * partitions：要删除的分区列表，每个分区由一个 Map 表示。
     * commitIdentifier：提交标识符，用于唯一标识此次删除操作。
     * 注意：partitions 列表不能为空，否则会导致操作失败。
     */
    void dropPartitions(List<Map<String, String>> partitions, long commitIdentifier);

    // 清空表
    // 删除表中所有数据，并生成一个新的快照
    void truncateTable(long commitIdentifier);

    // 中止不成功的提交
    // 删除与提交相关的数据文件
    void abort(List<CommitMessage> commitMessages);

    // 使用度量指标来测量提交操作
    // 用于记录提交过程中的性能指标和统计数据
    FileStoreCommit withMetrics(CommitMetrics metrics);

    // 提交新的统计信息
    // 将统计信息保存到文件存储系统中，并生成一个新的快照
    void commitStatistics(Statistics stats, long commitIdentifier);

    // 获取路径工厂
    // 用于生成文件存储系统中的路径
    FileStorePathFactory pathFactory();

    // 获取文件输入输出接口
    // 用于读取和写入文件存储系统中的文件
    FileIO fileIO();

    // 关闭资源
    // 释放与文件存储系统相关的资源
    @Override
    void close();
}
