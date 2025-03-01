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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
/**
 * 提供对 {@link CompactRewriter} 的通用实现。
 *
 * 该抽象类实现了部分 {@link CompactRewriter} 接口的方法，提供了通用的逻辑和基础实现。
 */
public abstract class AbstractCompactRewriter implements CompactRewriter {

    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        // 升级指定的数据文件到指定的输出级别，并返回压缩结果
        return new CompactResult(file, file.upgrade(outputLevel));
    }

    /**
     * 从多个运行列表中提取数据文件元数据。
     *
     * @param sections 运行列表
     * @return 返回提取出的数据文件元数据列表
     */
    protected static List<DataFileMeta> extractFilesFromSections(List<List<SortedRun>> sections) {
        return sections.stream() // 将运行列表转为流
                .flatMap(Collection::stream) // 扁平化运行
                .map(SortedRun::files) // 获取每个运行的文件元数据列表
                .flatMap(Collection::stream) // 扁平化文件元数据
                .collect(Collectors.toList()); // 收集文件元数据
    }

    @Override
    public void close() throws IOException {
        // 空实现，用于关闭资源（如果有需要）。
    }
}
