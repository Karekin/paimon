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

package org.apache.paimon.compact;

import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * 表示在整理（compaction）过程中生成的删除文件。
 */
public interface CompactDeletionFile {

    /**
     * 获取或计算删除文件的元数据。
     * @return 包含删除文件元数据的可选值
     */
    Optional<IndexFileMeta> getOrCompute();

    /**
     * 合并旧的删除文件。
     * @param old 旧的删除文件
     * @return 合并后的删除文件
     */
    CompactDeletionFile mergeOldFile(CompactDeletionFile old);

    /**
     * 清理删除文件。
     */
    void clean();

    /**
     * 用于异步整理，当整理任务完成时，会立即生成删除文件，
     * 因此在更新整理结果时，需要合并旧的删除文件（只需删除它们）。
     * @param maintainer 删除向量维护器
     * @return 生成的删除文件
     */
    static CompactDeletionFile generateFiles(DeletionVectorsMaintainer maintainer) {
        // 写入删除向量索引，返回文件列表
        List<IndexFileMeta> files = maintainer.writeDeletionVectorsIndex();
        if (files.size() > 1) {
            // 应该只生成一个删除文件，否则抛出异常
            throw new IllegalStateException(
                    "Should only generate one compact deletion file, this is a bug.");
        }

        // 返回生成的删除文件对象
        return new GeneratedDeletionFile(
                files.isEmpty() ? null : files.get(0), maintainer.indexFileHandler());
    }

    /**
     * 用于同步整理，仅在准备提交时创建删除文件。
     * @param maintainer 删除向量维护器
     * @return 延迟生成的删除文件
     */
    static CompactDeletionFile lazyGeneration(DeletionVectorsMaintainer maintainer) {
        return new LazyCompactDeletionFile(maintainer);
    }

    /**
     * {@link CompactDeletionFile} 的生成文件实现。
     */
    class GeneratedDeletionFile implements CompactDeletionFile {

        @Nullable private final IndexFileMeta deletionFile; // 删除文件元数据
        private final IndexFileHandler fileHandler; // 文件处理器

        private boolean getInvoked = false; // 标记 getOrCompute 是否被调用

        public GeneratedDeletionFile(
                @Nullable IndexFileMeta deletionFile, IndexFileHandler fileHandler) {
            this.deletionFile = deletionFile;
            this.fileHandler = fileHandler;
        }

        @Override
        public Optional<IndexFileMeta> getOrCompute() {
            this.getInvoked = true; // 标记 getOrCompute 被调用
            return Optional.ofNullable(deletionFile); // 返回删除文件元数据
        }

        @Override
        public CompactDeletionFile mergeOldFile(CompactDeletionFile old) {
            if (!(old instanceof GeneratedDeletionFile)) {
                // 旧文件必须是 GeneratedDeletionFile 类型
                throw new IllegalStateException(
                        "old should be a GeneratedDeletionFile, but it is: " + old.getClass());
            }

            if (((GeneratedDeletionFile) old).getInvoked) {
                // 旧文件的 getOrCompute 不应被调用
                throw new IllegalStateException("old should not be get, this is a bug.");
            }

            if (deletionFile == null) {
                return old; // 如果当前文件为空，返回旧文件
            }

            old.clean(); // 清理旧文件
            return this; // 返回当前文件
        }

        @Override
        public void clean() {
            if (deletionFile != null) {
                fileHandler.deleteIndexFile(deletionFile); // 删除删除文件
            }
        }
    }

    /**
     * {@link CompactDeletionFile} 的延迟生成实现。
     */
    class LazyCompactDeletionFile implements CompactDeletionFile {

        private final DeletionVectorsMaintainer maintainer; // 删除向量维护器

        private boolean generated = false; // 标记是否已生成删除文件

        public LazyCompactDeletionFile(DeletionVectorsMaintainer maintainer) {
            this.maintainer = maintainer;
        }

        @Override
        public Optional<IndexFileMeta> getOrCompute() {
            generated = true; // 标记已生成删除文件
            return generateFiles(maintainer).getOrCompute(); // 调用 generateFiles 方法生成删除文件
        }

        @Override
        public CompactDeletionFile mergeOldFile(CompactDeletionFile old) {
            if (!(old instanceof LazyCompactDeletionFile)) {
                // 旧文件必须是 LazyCompactDeletionFile 类型
                throw new IllegalStateException(
                        "old should be a LazyCompactDeletionFile, but it is: " + old.getClass());
            }

            if (((LazyCompactDeletionFile) old).generated) {
                // 旧文件不应已被生成
                throw new IllegalStateException("old should not be generated, this is a bug.");
            }

            return this; // 返回当前文件
        }

        @Override
        public void clean() {
            // 无需清理操作
        }
    }
}
