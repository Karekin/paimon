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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Util for merging manifest files. */
public class ManifestFileMerger {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileMerger.class);

    /**
     * Merge several {@link ManifestFileMeta}s. {@link ManifestEntry}s representing first adding and
     * then deleting the same data file will cancel each other.
     *
     * <p>NOTE: This method is atomic.
     */
    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> input,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            long manifestFullCompactionSize,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism) {
        // 这些是新创建的清单文件元数据，在异常情况下需要清理
        List<ManifestFileMeta> newMetas = new ArrayList<>();

        try {
            // 尝试进行全量合并
            Optional<List<ManifestFileMeta>> fullCompacted =
                    tryFullCompaction(
                            input,
                            newMetas,
                            manifestFile,
                            suggestedMetaSize,
                            manifestFullCompactionSize,
                            partitionType,
                            manifestReadParallelism);
            // 如果全量合并成功，则返回结果，否则尝试小文件合并
            return fullCompacted.orElseGet(
                    () ->
                            tryMinorCompaction(
                                    input,
                                    newMetas,
                                    manifestFile,
                                    suggestedMetaSize,
                                    suggestedMinMetaCount,
                                    manifestReadParallelism));
        } catch (Throwable e) {
            // 出现异常时，清理新创建的清单文件并重新抛出异常
            for (ManifestFileMeta manifest : newMetas) {
                manifestFile.delete(manifest.fileName());
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * 尝试进行小文件合并
     *
     * 将输入的清单文件元数据进行合并，生成新的清单文件元数据列表
     */
    private static List<ManifestFileMeta> tryMinorCompaction(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newMetas,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            @Nullable Integer manifestReadParallelism) {
        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> candidates = new ArrayList<>();
        long totalSize = 0;
        // 合并现有的小清单文件
        for (ManifestFileMeta manifest : input) {
            totalSize += manifest.fileSize();
            candidates.add(manifest);
            if (totalSize >= suggestedMetaSize) {
                // 达到建议的文件大小，执行合并并生成新文件
                mergeCandidates(
                        candidates, manifestFile, result, newMetas, manifestReadParallelism);
                candidates.clear();
                totalSize = 0;
            }
        }

        // 如果剩余的清单数量过多，合并最后一部分
        if (candidates.size() >= suggestedMinMetaCount) {
            mergeCandidates(candidates, manifestFile, result, newMetas, manifestReadParallelism);
        } else {
            result.addAll(candidates);
        }
        return result;
    }

    /**
     * 合并候选清单文件元数据
     */
    private static void mergeCandidates(
            List<ManifestFileMeta> candidates,
            ManifestFile manifestFile,
            List<ManifestFileMeta> result,
            List<ManifestFileMeta> newMetas,
            @Nullable Integer manifestReadParallelism) {
        if (candidates.size() == 1) {
            // 如果候选清单只有一个，直接加入结果
            result.add(candidates.get(0));
            return;
        }

        // 合并清单条目
        Map<FileEntry.Identifier, ManifestEntry> map = new LinkedHashMap<>();
        FileEntry.mergeEntries(manifestFile, candidates, map, manifestReadParallelism);
        if (!map.isEmpty()) {
            // 写入合并后的清单条目
            List<ManifestFileMeta> merged = manifestFile.write(new ArrayList<>(map.values()));
            result.addAll(merged);
            newMetas.addAll(merged);
        }
    }

    /**
     * 尝试进行全量合并
     *
     * 将输入的清单文件元数据进行全量合并，生成新的清单文件元数据列表
     */
    public static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> inputs,
            List<ManifestFileMeta> newMetas,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            long sizeTrigger,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        // 1. 判断是否触发全量合并

        List<ManifestFileMeta> base = new ArrayList<>();
        long totalManifestSize = 0;

        // 遍历输入的清单文件元数据
        // base：包含未被删除且文件大小不小于建议大小的清单文件
        // delta：包含被删除或文件大小较小的清单文件
        int i = 0;
        for (; i < inputs.size(); i++) {
            ManifestFileMeta file = inputs.get(i);
            if (file.numDeletedFiles() == 0 && file.fileSize() >= suggestedMetaSize) {
                // 添加到 base
                base.add(file);
                totalManifestSize += file.fileSize();
            } else {
                break;
            }
        }

        List<ManifestFileMeta> delta = new ArrayList<>();
        long deltaDeleteFileNum = 0;
        long totalDeltaFileSize = 0;
        for (; i < inputs.size(); i++) {
            ManifestFileMeta file = inputs.get(i);
            delta.add(file);
            totalManifestSize += file.fileSize();
            deltaDeleteFileNum += file.numDeletedFiles();
            totalDeltaFileSize += file.fileSize();
        }

        // 如果 delta 部分的总大小小于触发阈值，不进行全量合并
        if (totalDeltaFileSize < sizeTrigger) {
            return Optional.empty();
        }

        // 2. 执行全量合并

        LOG.info(
                "Start Manifest File Full Compaction, pick the number of delete file: {}, total manifest file size: {}",
                deltaDeleteFileNum,
                totalManifestSize);

        // 2.1. 尝试通过分区过滤跳过 base 文件

        Map<FileEntry.Identifier, ManifestEntry> deltaMerged = new LinkedHashMap<>();
        FileEntry.mergeEntries(manifestFile, delta, deltaMerged, manifestReadParallelism);

        List<ManifestFileMeta> result = new ArrayList<>();
        int j = 0;
        if (partitionType.getFieldCount() > 0) {
            // 计算 delta 合并后的删除分区集合
            Set<BinaryRow> deletePartitions = computeDeletePartitions(deltaMerged);
            // 构建分区谓词
            PartitionPredicate predicate =
                    PartitionPredicate.fromMultiple(partitionType, deletePartitions);
            if (predicate != null) {
                // 遍历 base 清单文件元数据，过滤掉与删除分区相关的文件
                for (; j < base.size(); j++) {
                    ManifestFileMeta file = base.get(j);
                    if (predicate.test(
                            file.numAddedFiles() + file.numDeletedFiles(),
                            file.partitionStats().minValues(),
                            file.partitionStats().maxValues(),
                            file.partitionStats().nullCounts())) {
                        break;
                    } else {
                        result.add(file);
                    }
                }
            } else {
                // delta 中没有删除条目，base 不需要合并
                j = base.size();
                result.addAll(base);
            }
        }

        // 2.2. 通过读取条目尝试跳过 base 文件

        // 收集 delta 合并后的删除条目
        Set<FileEntry.Identifier> deleteEntries = new HashSet<>();
        deltaMerged.forEach(
                (k, v) -> {
                    if (v.kind() == FileKind.DELETE) {
                        deleteEntries.add(k);
                    }
                });

        List<ManifestEntry> mergedEntries = new ArrayList<>();
        for (; j < base.size(); j++) {
            ManifestFileMeta file = base.get(j);
            boolean contains = false;
            for (ManifestEntry entry : manifestFile.read(file.fileName(), file.fileSize())) {
                checkArgument(entry.kind() == FileKind.ADD);
                if (deleteEntries.contains(entry.identifier())) {
                    contains = true;
                } else {
                    mergedEntries.add(entry);
                }
            }
            if (contains) {
                // 该文件包含需要删除的条目，无法跳过
                j++;
                break;
            } else {
                // 该文件不包含需要删除的条目，可以直接添加到结果中
                mergedEntries.clear();
                result.add(file);
            }
        }

        // 2.3. 执行合并

        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                manifestFile.createRollingWriter();
        Exception exception = null;
        try {

            // 2.3.1 合并 mergedEntries
            for (ManifestEntry entry : mergedEntries) {
                writer.write(entry);
            }
            mergedEntries.clear();

            // 2.3.2 合并 base 中剩余的文件
            for (ManifestEntry entry :
                    FileEntry.readManifestEntries(
                            manifestFile, base.subList(j, base.size()), manifestReadParallelism)) {
                checkArgument(entry.kind() == FileKind.ADD);
                if (!deleteEntries.contains(entry.identifier())) {
                    writer.write(entry);
                }
            }

            // 2.3.3 合并 deltaMerged
            for (ManifestEntry entry : deltaMerged.values()) {
                if (entry.kind() == FileKind.ADD) {
                    writer.write(entry);
                }
            }
        } catch (Exception e) {
            exception = e;
        } finally {
            if (exception != null) {
                IOUtils.closeQuietly(writer);
                throw exception;
            }
            writer.close();
        }

        // 获取合并后的清单文件元数据
        List<ManifestFileMeta> merged = writer.result();
        result.addAll(merged);
        newMetas.addAll(merged);
        return Optional.of(result);
    }

    /**
     * 计算删除分区集合
     *
     * 根据 delta 合并后的清单条目，提取删除分区的信息
     */
    private static Set<BinaryRow> computeDeletePartitions(
            Map<FileEntry.Identifier, ManifestEntry> deltaMerged) {
        Set<BinaryRow> partitions = new HashSet<>();
        // 遍历 delta 合并后的清单条目
        for (ManifestEntry manifestEntry : deltaMerged.values()) {
            if (manifestEntry.kind() == FileKind.DELETE) {
                BinaryRow partition = manifestEntry.partition();
                partitions.add(partition);
            }
        }
        return partitions;
    }
}
