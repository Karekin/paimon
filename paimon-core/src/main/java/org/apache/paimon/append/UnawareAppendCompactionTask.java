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

package org.apache.paimon.append;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.append.AppendDeletionFileMaintainer;
import org.apache.paimon.deletionvectors.append.UnawareAppendDeletionFileMaintainer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;

/**
 * UnawareAppendCompactionTask 是由 UnawareAppendTableCompactionCoordinator 生成的合并任务。
 * 主要用于管理文件存储表中数据的合并。
 * 任务中包含了分区、合并前后的文件元数据信息，以及执行合并操作的逻辑。
 */
public class UnawareAppendCompactionTask {

    // 数据所在分区的二进制行表示
    private final BinaryRow partition;

    // 合并前的数据文件元数据列表
    private final List<DataFileMeta> compactBefore;

    // 合并后的数据文件元数据列表
    private final List<DataFileMeta> compactAfter;

    /**
     * 构造函数，初始化 UnawareAppendCompactionTask 对象。
     * 输入文件列表不能为空，否则会抛出异常。
     *
     * @param partition   数据分区
     * @param files       文件元数据列表
     */
    public UnawareAppendCompactionTask(BinaryRow partition, List<DataFileMeta> files) {
        // 检查文件列表是否为空
        Preconditions.checkArgument(files != null, "File list cannot be null");

        this.partition = partition;
        // 初始化合并前的文件列表
        compactBefore = new ArrayList<>(files);
        // 初始化合并后的文件列表
        compactAfter = new ArrayList<>();
    }

    /**
     * 获取数据分区。
     *
     * @return 数据分区
     */
    public BinaryRow partition() {
        return partition;
    }

    /**
     * 获取合并前的文件元数据列表。
     *
     * @return 合并前的文件元数据列表
     */
    public List<DataFileMeta> compactBefore() {
        return compactBefore;
    }

    /**
     * 获取合并后的文件元数据列表。
     *
     * @return 合并后的文件元数据列表
     */
    public List<DataFileMeta> compactAfter() {
        return compactAfter;
    }

    /**
     * 执行合并操作，返回合并结果。
     * 包括删除向量的操作和索引文件的维护。
     * 如果删除向量未启用，则直接进行文件合并。
     *
     * @param table 文件存储表
     * @param write 文件存储写入器
     * @return 合并后的提交消息
     * @throws Exception 如果合并过程中发生异常
     */
    public CommitMessage doCompact(FileStoreTable table, AppendOnlyFileStoreWrite write)
            throws Exception {
        // 获取删除向量是否启用
        boolean dvEnabled = table.coreOptions().deletionVectorsEnabled();

        // 检查是否满足删除向量或合并条件
        Preconditions.checkArgument(
                dvEnabled || compactBefore.size() > 1,
                "AppendOnlyCompactionTask need more than one file input.");

        IndexIncrement indexIncrement; // 索引增量

        if (dvEnabled) {
            // 获取删除向量索引文件维护器
            UnawareAppendDeletionFileMaintainer dvIndexFileMaintainer =
                    AppendDeletionFileMaintainer.forUnawareAppend(
                            table.store().newIndexFileHandler(),
                            table.snapshotManager().latestSnapshotId(),
                            partition);

            // 合并文件并维护删除向量
            compactAfter.addAll(
                    write.compactRewrite(
                            partition,
                            UNAWARE_BUCKET,
                            dvIndexFileMaintainer::getDeletionVector,
                            compactBefore));

            // 通知移除文件对应的删除向量
            compactBefore.forEach(
                    f -> dvIndexFileMaintainer.notifyRemovedDeletionVector(f.fileName()));

            // 持久化索引条目
            List<IndexManifestEntry> indexEntries = dvIndexFileMaintainer.persist();
            // 确保没有新增索引文件
            Preconditions.checkArgument(
                    indexEntries.stream().noneMatch(i -> i.kind() == FileKind.ADD));

            // 收集已移除的索引文件
            List<IndexFileMeta> removed =
                    indexEntries.stream()
                            .map(IndexManifestEntry::indexFile)
                            .collect(Collectors.toList());

            // 创建索引增量对象
            indexIncrement = new IndexIncrement(Collections.emptyList(), removed);
        } else {
            // 直接合并文件
            compactAfter.addAll(
                    write.compactRewrite(partition, UNAWARE_BUCKET, null, compactBefore));

            // 创建空的索引增量对象
            indexIncrement = new IndexIncrement(Collections.emptyList());
        }

        // 创建合并增量对象
        CompactIncrement compactIncrement =
                new CompactIncrement(compactBefore, compactAfter, Collections.emptyList());

        // 创建提交消息
        return new CommitMessageImpl(
                partition,
                0, // bucket 0 is bucket for unaware-bucket table
                // for compatibility with the old design
                DataIncrement.emptyIncrement(),
                compactIncrement,
                indexIncrement);
    }

    /**
     * 计算对象的哈希值，用于哈希表等数据结构。
     *
     * @return 对象的哈希值
     */
    public int hashCode() {
        return Objects.hash(partition, compactBefore, compactAfter);
    }

    /**
     * 重写 equals 方法，比较两个对象是否相等。
     *
     * @param o 要比较的对象
     * @return 如果对象相等，返回 true；否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnawareAppendCompactionTask that = (UnawareAppendCompactionTask) o;
        return Objects.equals(partition, that.partition)
                && Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter);
    }

    /**
     * 重写 toString 方法，返回对象的字符串表示。
     *
     * @return 对象的字符串表示
     */
    @Override
    public String toString() {
        return String.format(
                "CompactionTask {"
                        + "partition = %s, "
                        + "compactBefore = %s, "
                        + "compactAfter = %s}",
                partition, compactBefore, compactAfter);
    }
}
