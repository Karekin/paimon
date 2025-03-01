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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/** 维护删除向量的索引文件。 */
public class DeletionVectorsMaintainer {

    private final IndexFileHandler indexFileHandler; // 索引文件处理器
    private final Map<String, DeletionVector> deletionVectors; // 存储删除向量的映射表
    private boolean modified; // 是否有修改

    private DeletionVectorsMaintainer(
            IndexFileHandler fileHandler, Map<String, DeletionVector> deletionVectors) {
        this.indexFileHandler = fileHandler;
        this.deletionVectors = deletionVectors;
        this.modified = false;
    }

    /**
     * 通知新的删除操作，将指定文件的指定行位置标记为已删除。
     * @param fileName 文件名
     * @param position 行位置
     */
    public void notifyNewDeletion(String fileName, long position) {
        DeletionVector deletionVector =
                deletionVectors.computeIfAbsent(fileName, k -> new BitmapDeletionVector());
        if (deletionVector.checkedDelete(position)) {
            modified = true;
        }
    }

    /**
     * 通知新的删除操作，将指定文件的删除向量设置为给定的删除向量。
     * @param fileName 文件名
     * @param deletionVector 删除向量
     */
    public void notifyNewDeletion(String fileName, DeletionVector deletionVector) {
        deletionVectors.put(fileName, deletionVector);
        modified = true;
    }

    /**
     * 合并新的删除向量，如果存在旧的删除向量则进行合并。
     * @param fileName 文件名
     * @param deletionVector 新的删除向量
     */
    public void mergeNewDeletion(String fileName, DeletionVector deletionVector) {
        DeletionVector old = deletionVectors.get(fileName);
        if (old != null) {
            deletionVector.merge(old);
        }
        deletionVectors.put(fileName, deletionVector);
        modified = true;
    }

    /**
     * 删除指定文件的删除向量，通常用于在压缩过程中移除旧文件的删除向量。
     * @param fileName 文件名
     */
    public void removeDeletionVectorOf(String fileName) {
        if (deletionVectors.containsKey(fileName)) {
            deletionVectors.remove(fileName);
            modified = true;
        }
    }

    /**
     * 如果有修改，则写入新的删除向量索引文件。
     * @return 包含删除向量索引文件元数据的列表，如果无需提交更改则返回空列表
     */
    public List<IndexFileMeta> writeDeletionVectorsIndex() {
        if (modified) {
            modified = false;
            return indexFileHandler.writeDeletionVectorsIndex(deletionVectors);
        }
        return Collections.emptyList();
    }

    /**
     * 获取指定文件名对应的删除向量。
     * @param fileName 文件名
     * @return 包含删除向量的 Optional 或空 Optional
     */
    public Optional<DeletionVector> deletionVectorOf(String fileName) {
        return Optional.ofNullable(deletionVectors.get(fileName));
    }

    public IndexFileHandler indexFileHandler() {
        return indexFileHandler;
    }

    @VisibleForTesting
    public Map<String, DeletionVector> deletionVectors() {
        return deletionVectors;
    }

    public static Factory factory(IndexFileHandler handler) {
        return new Factory(handler);
    }

    /** 工厂类，用于恢复或创建 DeletionVectorsMaintainer。 */
    public static class Factory {

        private final IndexFileHandler handler;

        public Factory(IndexFileHandler handler) {
            this.handler = handler;
        }

        /**
         * 根据指定的快照 ID、分区和桶号创建或恢复 DeletionVectorsMaintainer。
         * @param snapshotId 快照 ID
         * @param partition 分区
         * @param bucket 桶号
         * @return DeletionVectorsMaintainer 实例
         */
        public DeletionVectorsMaintainer createOrRestore(
                @Nullable Long snapshotId, BinaryRow partition, int bucket) {
            List<IndexFileMeta> indexFiles =
                    snapshotId == null
                            ? Collections.emptyList()
                            : handler.scan(snapshotId, DELETION_VECTORS_INDEX, partition, bucket);
            Map<String, DeletionVector> deletionVectors =
                    new HashMap<>(handler.readAllDeletionVectors(indexFiles));
            return createOrRestore(deletionVectors);
        }

        @VisibleForTesting
        public DeletionVectorsMaintainer createOrRestore(
                @Nullable Long snapshotId, BinaryRow partition) {
            List<IndexFileMeta> indexFiles =
                    snapshotId == null
                            ? Collections.emptyList()
                            : handler.scanEntries(snapshotId, DELETION_VECTORS_INDEX, partition)
                            .stream()
                            .map(IndexManifestEntry::indexFile)
                            .collect(Collectors.toList());
            Map<String, DeletionVector> deletionVectors =
                    new HashMap<>(handler.readAllDeletionVectors(indexFiles));
            return createOrRestore(deletionVectors);
        }

        /**
         * 创建一个新的 DeletionVectorsMaintainer 实例。
         * @return DeletionVectorsMaintainer 实例
         */
        public DeletionVectorsMaintainer create() {
            return createOrRestore(new HashMap<>());
        }

        /**
         * 创建或恢复 DeletionVectorsMaintainer 实例。
         * @param deletionVectors 删除向量映射表
         * @return DeletionVectorsMaintainer 实例
         */
        public DeletionVectorsMaintainer createOrRestore(
                Map<String, DeletionVector> deletionVectors) {
            return new DeletionVectorsMaintainer(handler, deletionVectors);
        }
    }
}
