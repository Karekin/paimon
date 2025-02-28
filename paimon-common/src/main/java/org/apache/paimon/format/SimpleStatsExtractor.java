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

package org.apache.paimon.format;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.Pair;

import java.io.IOException;

/** 从文件中直接提取统计信息。 */
public interface SimpleStatsExtractor {

    /**
     * 从指定路径的文件中提取统计信息。
     *
     * @param fileIO 文件 I/O 实例
     * @param path 文件路径
     * @return 提取到的每个字段的统计信息数组
     * @throws IOException 如果在读取文件时发生 I/O 错误
     */
    SimpleColStats[] extract(FileIO fileIO, Path path) throws IOException;

    /**
     * 从指定路径的文件中提取统计信息，并同时获取文件的信息。
     *
     * @param fileIO 文件 I/O 实例
     * @param path 文件路径
     * @return 包含统计信息和文件信息的 Pair 对象
     * @throws IOException 如果在读取文件时发生 I/O 错误
     */
    Pair<SimpleColStats[], FileInfo> extractWithFileInfo(FileIO fileIO, Path path) throws IOException;

    /**
     * 从物理文件中获取的文件信息。
     */
    class FileInfo {

        private final long rowCount; // 文件的行数

        public FileInfo(long rowCount) {
            this.rowCount = rowCount;
        }

        public long getRowCount() {
            return rowCount;
        }
    }
}
