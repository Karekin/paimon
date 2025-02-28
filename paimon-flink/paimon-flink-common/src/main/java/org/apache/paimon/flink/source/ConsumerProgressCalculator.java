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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.utils.TableScanUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * 计算消费者消费进度的计算器。
 */
public class ConsumerProgressCalculator {
    // 用于存储每个检查点对应的最小下一个快照 ID
    private final TreeMap<Long, Long> minNextSnapshotPerCheckpoint;

    // 用于存储每个子任务分配的快照 ID
    private final Map<Integer, Long> assignedSnapshotPerReader;

    // 用于存储每个子任务正在消费的快照 ID
    private final Map<Integer, Long> consumingSnapshotPerReader;

    /**
     * 构造函数。
     *
     * @param parallelism 并行度
     */
    public ConsumerProgressCalculator(int parallelism) {
        this.minNextSnapshotPerCheckpoint = new TreeMap<>(); // 初始化最小下一个快照 ID 的映射
        this.assignedSnapshotPerReader = new HashMap<>(parallelism); // 初始化分配的快照 ID 的映射
        this.consumingSnapshotPerReader = new HashMap<>(parallelism); // 初始化正在消费的快照 ID 的映射
    }

    /**
     * 更新消费者的消费进度。
     *
     * @param subtaskId 子任务 ID
     * @param event 消费者消费进度事件
     */
    public void updateConsumeProgress(int subtaskId, ReaderConsumeProgressEvent event) {
        // 更新正在消费的快照 ID
        consumingSnapshotPerReader.put(subtaskId, event.lastConsumeSnapshotId());
    }

    /**
     * 更新分配的信息。
     *
     * @param subtaskId 子任务 ID
     * @param split 文件存储源切片
     */
    public void updateAssignInformation(int subtaskId, FileStoreSourceSplit split) {
        // 获取切片对应的快照 ID 并更新分配的快照 ID
        TableScanUtils.getSnapshotId(split)
                .ifPresent(snapshotId -> assignedSnapshotPerReader.put(subtaskId, snapshotId));
    }

    /**
     * 通知检查点状态。
     *
     * @param checkpointId 检查点 ID
     * @param readersAwaitingSplit 等待分配切片的读者集合
     * @param unassignedCalculationFunction 未分配计算函数
     * @param parallelism 并行度
     */
    public void notifySnapshotState(
            long checkpointId,
            Set<Integer> readersAwaitingSplit,
            Function<Integer, Long> unassignedCalculationFunction,
            int parallelism) {
        // 计算最小下一个快照 ID
        computeMinNextSnapshotId(readersAwaitingSplit, unassignedCalculationFunction, parallelism)
                .ifPresent(
                        minNextSnapshotId ->
                                // 将最小下一个快照 ID 与检查点 ID 关联
                                minNextSnapshotPerCheckpoint.put(checkpointId, minNextSnapshotId));
    }

    /**
     * 通知检查点完成。
     *
     * @param checkpointId 检查点 ID
     * @return 最大的快照 ID
     */
    public OptionalLong notifyCheckpointComplete(long checkpointId) {
        // 获取小于等于当前检查点 ID 的所有快照 ID 的子映射
        NavigableMap<Long, Long> nextSnapshots =
                minNextSnapshotPerCheckpoint.headMap(checkpointId, true);

        // 计算这些快照 ID 中的最大值
        OptionalLong max = nextSnapshots.values().stream().mapToLong(Long::longValue).max();

        // 清除这些快照 ID
        nextSnapshots.clear();

        return max;
    }

    /**
     * 计算所有读者目前正在消费的最小快照 ID。
     *
     * @param readersAwaitingSplit 等待分配切片的读者集合
     * @param unassignedCalculationFunction 未分配计算函数
     * @param parallelism 并行度
     * @return 最小的快照 ID
     */
    private Optional<Long> computeMinNextSnapshotId(
            Set<Integer> readersAwaitingSplit,
            Function<Integer, Long> unassignedCalculationFunction,
            int parallelism) {
        // 初始化全局最小快照 ID
        long globalMinSnapshotId = Long.MAX_VALUE;

        // 遍历所有子任务
        for (int subtask = 0; subtask < parallelism; subtask++) {
            // 计算当前子任务的快照 ID
            Long snapshotIdForSubtask;

            // 如果当前子任务在等待分配切片的集合中
            if (readersAwaitingSplit.contains(subtask)) {
                // 使用未分配计算函数计算快照 ID
                snapshotIdForSubtask = unassignedCalculationFunction.apply(subtask);
            } else {
                // 获取正在消费的快照 ID 和分配的快照 ID
                Long consumingSnapshotId = consumingSnapshotPerReader.get(subtask);
                Long assignedSnapshotId = assignedSnapshotPerReader.get(subtask);

                // 计算较大的快照 ID
                if (consumingSnapshotId != null && assignedSnapshotId != null) {
                    snapshotIdForSubtask = Math.max(consumingSnapshotId, assignedSnapshotId);
                } else {
                    // 如果其中一个为 null，取另一个
                    snapshotIdForSubtask = consumingSnapshotId != null ? consumingSnapshotId : assignedSnapshotId;
                }
            }

            // 如果快照 ID 不为 null，更新全局最小快照 ID
            if (snapshotIdForSubtask != null) {
                globalMinSnapshotId = Math.min(globalMinSnapshotId, snapshotIdForSubtask);
            } else {
                // 如果有任意一个子任务的快照 ID 为 null，返回空
                return Optional.empty();
            }
        }

        // 返回全局最小快照 ID
        return Optional.of(globalMinSnapshotId);
    }
}
