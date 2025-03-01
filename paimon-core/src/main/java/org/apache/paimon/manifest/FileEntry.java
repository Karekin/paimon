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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;


/**
 * 表示文件的条目。
 */
public interface FileEntry {

    FileKind kind(); // 获取文件类型

    BinaryRow partition(); // 获取分区信息

    int bucket(); // 获取桶号

    int level(); // 获取文件层级

    String fileName(); // 获取文件名

    Identifier identifier(); // 获取文件的唯一标识符

    BinaryRow minKey(); // 获取文件的最小键值

    BinaryRow maxKey(); // 获取文件的最大键值

    /**
     * 表示文件的唯一标识符。
     */
    class Identifier {
        public final BinaryRow partition; // 分区信息
        public final int bucket; // 桶号
        public final int level; // 文件层级
        public final String fileName; // 文件名

        /* 为字符串缓存哈希码 */
        private Integer hash;

        public Identifier(BinaryRow partition, int bucket, int level, String fileName) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
            this.fileName = fileName;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Identifier)) {
                return false;
            }
            Identifier that = (Identifier) o;
            return Objects.equals(partition, that.partition)
                    && bucket == that.bucket
                    && level == that.level
                    && Objects.equals(fileName, that.fileName);
        }

        @Override
        public int hashCode() {
            if (hash == null) {
                hash = Objects.hash(partition, bucket, level, fileName);
            }
            return hash;
        }

        @Override
        public String toString() {
            return String.format("{%s, %d, %d, %s}", partition, bucket, level, fileName);
        }

        public String toString(FileStorePathFactory pathFactory) {
            return pathFactory.getPartitionString(partition)
                    + ", bucket "
                    + bucket
                    + ", level "
                    + level
                    + ", file "
                    + fileName;
        }
    }

    static <T extends FileEntry> Collection<T> mergeEntries(Iterable<T> entries) {
        LinkedHashMap<Identifier, T> map = new LinkedHashMap<>();
        mergeEntries(entries, map);
        return map.values();
    }

    static void mergeEntries(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            Map<Identifier, ManifestEntry> map,
            @Nullable Integer manifestReadParallelism) {
        mergeEntries(
                readManifestEntries(manifestFile, manifestFiles, manifestReadParallelism), map);
    }

    static <T extends FileEntry> void mergeEntries(Iterable<T> entries, Map<Identifier, T> map) {
        for (T entry : entries) {
            Identifier identifier = entry.identifier();
            switch (entry.kind()) {
                case ADD:
                    Preconditions.checkState(
                            !map.containsKey(identifier),
                            "尝试添加的文件 %s 已经存在。",
                            identifier);
                    map.put(identifier, entry);
                    break;
                case DELETE:
                    // 每个数据文件只能添加一次和删除一次，
                    // 如果我们确定它在之前已经被添加，则可以同时删除两个条目，因为不会再对这个文件进行进一步的操作，
                    // 否则，我们必须保留删除条目，因为添加条目必须存在于之前的清单文件中
                    if (map.containsKey(identifier)) {
                        map.remove(identifier);
                    } else {
                        map.put(identifier, entry);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "不支持的文件类型" + entry.kind().name());
            }
        }
    }

    static Iterable<ManifestEntry> readManifestEntries(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            @Nullable Integer manifestReadParallelism) {
        return sequentialBatchedExecute(
                file -> manifestFile.read(file.fileName(), file.fileSize()),
                manifestFiles,
                manifestReadParallelism);
    }
}