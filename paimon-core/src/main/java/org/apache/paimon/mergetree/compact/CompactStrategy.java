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

import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;

import java.util.List;
import java.util.Optional;

/**
 * 压缩策略接口，用于决定哪些文件需要被选中进行压缩（compaction）。
 * 提供方法从运行（runs）中挑选压缩单元。
 */
public interface CompactStrategy {

    /**
     * 从运行（runs）中挑选压缩单元（CompactUnit）。
     *
     * <ul>
     *   <li>压缩基于运行（runs），而不是基于文件。
     *   <li>level 0 特殊处理，每个文件对应一个运行；其他级别每个级别对应一个运行。
     *   <li>压缩从较小的级别向较大的级别依次进行。
     * </ul>
     *
     * 参数:
     * - numLevels：运行的总级别数。通常一个文件系统有多个级别，用于分层存储数据。
     * - runs：按级别排序的运行列表，每个运行包含一组排序的文件。
     *
     * 返回值:
     * 返回一个 Optional 的 CompactUnit，表示本次压缩操作需要处理的单元。
     * 如果没有合适的压缩单元可选，则返回空值（Optional.empty()）。
     */
    Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs);

    /**
     * 选择一个由所有现有运行组成的全量压缩单元。
     *
     * 参数:
     * - numLevels：运行的总级别数。
     * - runs：按级别排序的运行列表。
     *
     * 返回值:
     * 返回一个 Optional 的 CompactUnit，表示全量压缩的单元，如果无需压缩则返回空值。
     */
    static Optional<CompactUnit> pickFullCompaction(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1; // 获取最大级别
        if (runs.isEmpty() || (runs.size() == 1 && runs.get(0).level() == maxLevel)) {
            // 如果运行列表为空，或者只有一个运行在最大级别，无需压缩
            return Optional.empty();
        } else {
            // 构造一个由最大级别运行组成的压缩单元
            return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
        }
    }
}
