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
 * 一个 {@link CompactStrategy} 的实现，用于强制压缩 level 0 的文件。
 *
 * 1. 该类主要用于当 level 0 文件需要被压缩时，选择合适的压缩单元（Compact Unit）。
 * 2. 如果通用压缩策略没有选择到合适的压缩单元，将尝试从 level 0 文件中选择。
 *
 */
public class ForceUpLevel0Compaction implements CompactStrategy {

    // 通用压缩策略对象，用于应用通用的压缩逻辑
    private final UniversalCompaction universal;

    // 构造函数，初始化通用压缩策略对象
    public ForceUpLevel0Compaction(UniversalCompaction universal) {
        this.universal = universal;
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        // 首先尝试使用通用压缩策略选择压缩单元
        Optional<CompactUnit> pick = universal.pick(numLevels, runs);
        if (pick.isPresent()) { // 如果通用策略已经选择了压缩单元
            return pick; // 直接返回
        }

        // 收集所有 level 0 的文件
        int candidateCount = 0; // 用于统计 level 0 文件的数量
        for (int i = candidateCount; i < runs.size(); i++) { // 遍历 runs 列表
            if (runs.get(i).level() > 0) { // 如果当前运行的 level 大于 0，说明已经跳出了 level 0
                break; // 跳出循环
            }
            candidateCount++; // 否则，将 level 0 文件的数量加 1
        }

        // 如果没有 level 0 文件候选
        return candidateCount == 0
                ? Optional.empty() // 返回空值
                : Optional.of(
                universal.pickForSizeRatio(numLevels - 1, runs, candidateCount, true)); // 否则，应用基于大小比例的压缩策略
    }
}