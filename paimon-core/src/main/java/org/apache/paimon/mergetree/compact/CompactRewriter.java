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

import java.io.Closeable;
import java.util.List;

/**
 * 提供文件的重写功能，用于将文件重写到新的级别。
 *
 * 该接口定义了一个通用的重写器，用于压缩或升级文件。通常实现类将基于不同的压缩策略或文件格式提供具体的实现。通常，这种重写器可能用于对文件进行压缩、优化或升级，以提高存储效率或满足特定的存储需求。
 */
public interface CompactRewriter extends Closeable {

    /**
     * 将多个运行（sections）重写到新的级别。
     *
     * @param outputLevel 新的级别
     * @param dropDelete 是否丢弃删除标记，详情请参考 {@link MergeTreeCompactManager#triggerCompaction}
     * @param sections 运行列表（每个运行是一个 {@link SortedRun} 列表，且运行之间的键区间不重叠）
     * @return 压缩结果
     * @throws Exception 写入过程中可能抛出的异常
     */
    CompactResult rewrite(int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
            throws Exception;

    /**
     * 将文件升级到新的级别。
     * 通常情况下，文件数据不会被重写，只是元数据会被更新。但在某些特定场景中，例如 {@link ChangelogMergeTreeRewriter}，我们也可能需要重写文件数据。
     *
     * @param outputLevel 新级别
     * @param file 数据文件的元数据
     * @return 压缩结果
     */
    CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception;
}
