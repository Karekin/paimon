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

package org.apache.paimon.index;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.IntIterator;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** 处理索引文件。 */
public class IndexFileHandler {

    private final SnapshotManager snapshotManager; // 快照管理器
    private final PathFactory pathFactory; // 路径工厂
    private final IndexManifestFile indexManifestFile; // 索引清单文件
    private final HashIndexFile hashIndex; // 哈希索引文件
    private final DeletionVectorsIndexFile deletionVectorsIndex; // 删除向量索引文件

    public IndexFileHandler(
            SnapshotManager snapshotManager,
            PathFactory pathFactory,
            IndexManifestFile indexManifestFile,
            HashIndexFile hashIndex,
            DeletionVectorsIndexFile deletionVectorsIndex) {
        this.snapshotManager = snapshotManager;
        this.pathFactory = pathFactory;
        this.indexManifestFile = indexManifestFile;
        this.hashIndex = hashIndex;
        this.deletionVectorsIndex = deletionVectorsIndex;
    }

    public DeletionVectorsIndexFile deletionVectorsIndex() {
        return this.deletionVectorsIndex;
    }

    /**
     * 扫描哈希索引文件。
     * @param snapshotId 快照 ID
     * @param partition 分区
     * @param bucket 桶号
     * @return 包含哈希索引文件元数据的 Optional 或空 Optional
     */
    public Optional<IndexFileMeta> scanHashIndex(
            long snapshotId, BinaryRow partition, int bucket) {
        List<IndexFileMeta> result = scan(snapshotId, HASH_INDEX, partition, bucket);
        if (result.size() > 1) {
            throw new IllegalArgumentException(
                    "发现多个桶的哈希索引文件: " + result);
        }
        return result.isEmpty() ? Optional.empty() : Optional.of(result.get(0));
    }

    /**
     * 扫描删除向量索引文件。
     * @param snapshotId 快照 ID
     * @param partition 分区
     * @param bucket 桶号
     * @return 数据文件与删除文件的映射表
     */
    public Map<String, DeletionFile> scanDVIndex(
            @Nullable Long snapshotId, BinaryRow partition, int bucket) {
        if (snapshotId == null) {
            return Collections.emptyMap();
        }
        Snapshot snapshot = snapshotManager.snapshot(snapshotId);
        String indexManifest = snapshot.indexManifest();
        if (indexManifest == null) {
            return Collections.emptyMap();
        }
        Map<String, DeletionFile> result = new HashMap<>();
        for (IndexManifestEntry file : indexManifestFile.read(indexManifest)) {
            IndexFileMeta meta = file.indexFile();
            if (meta.indexType().equals(DELETION_VECTORS_INDEX)
                    && file.partition().equals(partition)
                    && file.bucket() == bucket) {
                LinkedHashMap<String, Pair<Integer, Integer>> dvRanges =
                        meta.deletionVectorsRanges();
                checkNotNull(dvRanges);
                for (String dataFile : dvRanges.keySet()) {
                    Pair<Integer, Integer> pair = dvRanges.get(dataFile);
                    DeletionFile deletionFile =
                            new DeletionFile(
                                    filePath(meta).toString(), pair.getLeft(), pair.getRight());
                    result.put(dataFile, deletionFile);
                }
            }
        }
        return result;
    }

    /**
     * 扫描索引文件清单。
     * @param indexType 索引类型
     * @return 符合条件的清单条目列表
     */
    public List<IndexManifestEntry> scan(String indexType) {
        Snapshot snapshot = snapshotManager.latestSnapshot();
        if (snapshot == null) {
            return Collections.emptyList();
        }
        String indexManifest = snapshot.indexManifest();
        if (indexManifest == null) {
            return Collections.emptyList();
        }

        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry file : indexManifestFile.read(indexManifest)) {
            if (file.indexFile().indexType().equals(indexType)) {
                result.add(file);
            }
        }
        return result;
    }

    /**
     * 扫描指定快照、索引类型、分区和桶号的索引文件元数据。
     * @param snapshotId 快照 ID
     * @param indexType 索引类型
     * @param partition 分区
     * @param bucket 桶号
     * @return 符合条件的索引文件元数据列表
     */
    public List<IndexFileMeta> scan(
            long snapshotId, String indexType, BinaryRow partition, int bucket) {
        List<IndexFileMeta> result = new ArrayList<>();
        for (IndexManifestEntry file : scanEntries(snapshotId, indexType, partition)) {
            if (file.bucket() == bucket) {
                result.add(file.indexFile());
            }
        }
        return result;
    }

    /**
     * 扫描指定快照、索引类型和分区的索引文件元数据，并按分区和桶号分组。
     * @param snapshot 快照 ID
     * @param indexType 索引类型
     * @param partitions 分区集合
     * @return 按分区和桶号分组的索引文件元数据映射表
     */
    public Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> scan(
            long snapshot, String indexType, Set<BinaryRow> partitions) {
        return scan(snapshotManager.snapshot(snapshot), indexType, partitions);
    }

    /**
     * 扫描指定快照、索引类型和分区的索引文件元数据，并按分区和桶号分组。
     * @param snapshot 快照
     * @param indexType 索引类型
     * @param partitions 分区集合
     * @return 按分区和桶号分组的索引文件元数据映射表
     */
    public Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> scan(
            Snapshot snapshot, String indexType, Set<BinaryRow> partitions) {
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> result = new HashMap<>();
        for (IndexManifestEntry file : scanEntries(snapshot, indexType, partitions)) {
            result.computeIfAbsent(Pair.of(file.partition(), file.bucket()), k -> new ArrayList<>())
                    .add(file.indexFile());
        }
        return result;
    }

    /**
     * 扫描所有索引文件清单条目。
     * @return 清单条目列表
     */
    public List<IndexManifestEntry> scanEntries() {
        Snapshot snapshot = snapshotManager.latestSnapshot();
        if (snapshot == null || snapshot.indexManifest() == null) {
            return Collections.emptyList();
        }

        return indexManifestFile.read(snapshot.indexManifest());
    }

    /**
     * 扫描指定索引类型和分区的索引文件清单条目。
     * @param indexType 索引类型
     * @param partition 分区
     * @return 符合条件的清单条目列表
     */
    public List<IndexManifestEntry> scanEntries(
            String indexType, BinaryRow partition) {
        Long snapshot = snapshotManager.latestSnapshotId();
        if (snapshot == null) {
            return Collections.emptyList();
        }

        return scanEntries(snapshot, indexType, partition);
    }

    /**
     * 扫描指定快照 ID、索引类型和分区的索引文件清单条目。
     * @param snapshotId 快照 ID
     * @param indexType 索引类型
     * @param partition 分区
     * @return 符合条件的清单条目列表
     */
    public List<IndexManifestEntry> scanEntries(
            long snapshotId, String indexType, BinaryRow partition) {
        return scanEntries(snapshotId, indexType, Collections.singleton(partition));
    }

    /**
     * 扫描指定快照 ID、索引类型和分区集合的索引文件清单条目。
     * @param snapshot 快照 ID
     * @param indexType 索引类型
     * @param partitions 分区集合
     * @return 符合条件的清单条目列表
     */
    public List<IndexManifestEntry> scanEntries(
            long snapshot, String indexType, Set<BinaryRow> partitions) {
        return scanEntries(snapshotManager.snapshot(snapshot), indexType, partitions);
    }

    /**
     * 扫描指定快照、索引类型和分区集合的索引文件清单条目。
     * @param snapshot 快照
     * @param indexType 索引类型
     * @param partitions 分区集合
     * @return 符合条件的清单条目列表
     */
    public List<IndexManifestEntry> scanEntries(
            Snapshot snapshot, String indexType, Set<BinaryRow> partitions) {
        String indexManifest = snapshot.indexManifest();
        if (indexManifest == null) {
            return Collections.emptyList();
        }

        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry file : indexManifestFile.read(indexManifest)) {
            if (file.indexFile().indexType().equals(indexType)
                    && partitions.contains(file.partition())) {
                result.add(file);
            }
        }
        return result;
    }

    /**
     * 根据文件元数据构造路径对象。
     * @param file 文件元数据
     * @return 路径对象
     */
    public Path filePath(IndexFileMeta file) {
        return pathFactory.toPath(file.fileName());
    }

    /**
     * 读取哈希索引文件的内容并转换为整型列表。
     * @param file 文件元数据
     * @return 哈希索引整型列表
     */
    public List<Integer> readHashIndexList(IndexFileMeta file) {
        return IntIterator.toIntList(readHashIndex(file));
    }

    /**
     * 读取哈希索引文件的整型迭代器。
     * @param file 文件元数据
     * @return 哈希索引整型迭代器
     */
    public IntIterator readHashIndex(IndexFileMeta file) {
        if (!file.indexType().equals(HASH_INDEX)) {
            throw new IllegalArgumentException("输入文件不是哈希索引: " + file.indexType());
        }

        try {
            return hashIndex.read(file.fileName());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 写入哈希索引文件（使用 int 数组）。
     * @param ints 整型数组
     * @return 文件元数据
     */
    public IndexFileMeta writeHashIndex(int[] ints) {
        return writeHashIndex(ints.length, IntIterator.create(ints));
    }

    /**
     * 写入哈希索引文件。
     * @param size 大小
     * @param iterator 整型迭代器
     * @return 文件元数据
     */
    public IndexFileMeta writeHashIndex(int size, IntIterator iterator) {
        String file;
        try {
            file = hashIndex.write(iterator);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new IndexFileMeta(HASH_INDEX, file, hashIndex.fileSize(file), size);
    }

    /**
     * 检查索引清单文件是否存在。
     * @param indexManifest 清单文件名
     * @return 是否存在
     */
    public boolean existsManifest(String indexManifest) {
        return indexManifestFile.exists(indexManifest);
    }

    /**
     * 读取索引清单文件的内容。
     * @param indexManifest 清单文件名
     * @return 清单条目列表
     */
    public List<IndexManifestEntry> readManifest(String indexManifest) {
        return indexManifestFile.read(indexManifest);
    }

    /**
     * 读取索引清单文件的内容（可能抛出 IOException）。
     * @param indexManifest 清单文件名
     * @return 清单条目列表
     * @throws IOException 如果发生 I/O 错误
     */
    public List<IndexManifestEntry> readManifestWithIOException(String indexManifest)
            throws IOException {
        return indexManifestFile.readWithIOException(indexManifest);
    }

    /**
     * 根据文件元数据返回对应的索引文件对象。
     * @param file 文件元数据
     * @return 索引文件对象
     */
    private IndexFile indexFile(IndexFileMeta file) {
        switch (file.indexType()) {
            case HASH_INDEX:
                return hashIndex;
            case DELETION_VECTORS_INDEX:
                return deletionVectorsIndex;
            default:
                throw new IllegalArgumentException("未知的索引类型: " + file.indexType());
        }
    }

    /**
     * 检查索引文件是否存在。
     * @param file 清单条目
     * @return 是否存在
     */
    public boolean existsIndexFile(IndexManifestEntry file) {
        return indexFile(file.indexFile()).exists(file.indexFile().fileName());
    }

    /**
     * 删除索引文件（通过清单条目）。
     * @param file 清单条目
     */
    public void deleteIndexFile(IndexManifestEntry file) {
        deleteIndexFile(file.indexFile());
    }

    /**
     * 删除索引文件。
     * @param file 文件元数据
     */
    public void deleteIndexFile(IndexFileMeta file) {
        indexFile(file).delete(file.fileName());
    }

    /**
     * 删除索引清单文件。
     * @param indexManifest 清单文件名
     */
    public void deleteManifest(String indexManifest) {
        indexManifestFile.delete(indexManifest);
    }

    /**
     * 读取所有删除向量索引文件。
     * @param fileMetas 文件元数据列表
     * @return 数据文件与删除向量的映射表
     */
    public Map<String, DeletionVector> readAllDeletionVectors(List<IndexFileMeta> fileMetas) {
        for (IndexFileMeta indexFile : fileMetas) {
            checkArgument(
                    indexFile.indexType().equals(DELETION_VECTORS_INDEX),
                    "输入文件不是删除向量索引 " + indexFile.indexType());
        }
        return deletionVectorsIndex.readAllDeletionVectors(fileMetas);
    }

    /**
     * 写入删除向量索引文件。
     * @param deletionVectors 删除向量映射表
     * @return 文件元数据列表
     */
    public List<IndexFileMeta> writeDeletionVectorsIndex(
            Map<String, DeletionVector> deletionVectors) {
        return deletionVectorsIndex.write(deletionVectors);
    }
}
