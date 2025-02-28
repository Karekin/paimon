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

package org.apache.paimon.flink.source.assigners;

import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.align.PlaceholderSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 分片按快照粒度分配。当当前快照的分片尚未完全分配且检查点尚未触发时，下一个快照将不会被分配。
 */
public class AlignedSplitAssigner implements SplitAssigner {

    private final Deque<PendingSnapshot> pendingSplitAssignment; // 待分配的分片队列

    private final AtomicInteger numberOfPendingSplits; // 剩余待分配的分片数量

    public AlignedSplitAssigner() {
        this.pendingSplitAssignment = new LinkedList<>();
        this.numberOfPendingSplits = new AtomicInteger(0);
    }

    @Override
    public List<FileStoreSourceSplit> getNext(int subtask, @Nullable String hostname) {
        PendingSnapshot head = pendingSplitAssignment.peek(); // 获取队列头部的快照
        if (head != null && !head.isPlaceHolder) {
            List<FileStoreSourceSplit> subtaskSplits = head.remove(subtask); // 移除对应子任务的分片
            if (subtaskSplits != null) {
                numberOfPendingSplits.getAndAdd(-subtaskSplits.size()); // 更新剩余分片数量
                return subtaskSplits;
            }
        }
        return Collections.emptyList();
    }

    @Override
    public void addSplit(int subtask, FileStoreSourceSplit splits) {
        long snapshotId = ((DataSplit) splits.split()).snapshotId(); // 获取快照ID
        PendingSnapshot last = pendingSplitAssignment.peekLast();
        boolean isPlaceholder = splits.split() instanceof PlaceholderSplit; // 是否是占位分片

        if (last == null || last.snapshotId != snapshotId) {
            // 如果队列为空或快照ID不同，创建新的PendingSnapshot
            last = new PendingSnapshot(snapshotId, isPlaceholder, new HashMap<>());
            pendingSplitAssignment.addLast(last); // 将新的快照添加到队列尾部
        }
        last.add(subtask, splits); // 将分片添加到对应子任务
        numberOfPendingSplits.incrementAndGet(); // 增加剩余分片数量
    }

    @Override
    public void addSplitsBack(int suggestedTask, List<FileStoreSourceSplit> splits) {
        if (splits.isEmpty()) {
            return; // 如果没有分片需要添加回，则直接返回
        }

        long snapshotId = ((DataSplit) splits.get(0).split()).snapshotId(); // 获取快照ID
        boolean isPlaceholder = splits.get(0).split() instanceof PlaceholderSplit;
        PendingSnapshot head = pendingSplitAssignment.peek(); // 获取队列头部的快照

        if (head == null || snapshotId != head.snapshotId) {
            // 如果队列为空或快照ID不同，创建新的PendingSnapshot
            head = new PendingSnapshot(snapshotId, isPlaceholder, new HashMap<>());
            pendingSplitAssignment.addFirst(head); // 将新的快照添加到队列头部
        }
        head.addAll(suggestedTask, splits); // 将分片批量添加到对应子任务
        numberOfPendingSplits.getAndAdd(splits.size()); // 更新剩余分片数量
    }

    @Override
    public Collection<FileStoreSourceSplit> remainingSplits() {
        List<FileStoreSourceSplit> remainingSplits = new ArrayList<>();
        for (PendingSnapshot pendingSnapshot : pendingSplitAssignment) {
            pendingSnapshot.subtaskSplits.values().forEach(remainingSplits::addAll); // 收集所有剩余分片
        }
        return remainingSplits;
    }

    @Override
    public Optional<Long> getNextSnapshotId(int subtask) {
        PendingSnapshot head = pendingSplitAssignment.peek();
        return Optional.ofNullable(head != null ? head.snapshotId : null); // 获取下一个快照ID
    }

    @Override
    public int numberOfRemainingSplits() {
        return numberOfPendingSplits.get();
    }

    public boolean isAligned() {
        PendingSnapshot head = pendingSplitAssignment.peek();
        // 检查队列头部的分片是否为空，或是否为占位分片
        return head != null && head.empty();
    }

    public int remainingSnapshots() {
        return pendingSplitAssignment.size(); // 返回剩余快照的数量
    }

    public void removeFirst() {
        PendingSnapshot head = pendingSplitAssignment.poll(); // 移除队列头部的快照
        Preconditions.checkArgument(
                head != null && head.empty(),
                "队列头部的分片未被分配，请提交问题。"); // 确保队列头部的分片已被完全分配
    }

    private static class PendingSnapshot {
        private final long snapshotId; // 快照ID
        private final boolean isPlaceHolder; // 是否是占位分片
        private final Map<Integer, List<FileStoreSourceSplit>> subtaskSplits; // 子任务对应的分片

        public PendingSnapshot(
                long snapshotId,
                boolean isPlaceHolder,
                Map<Integer, List<FileStoreSourceSplit>> subtaskSplits) {
            this.snapshotId = snapshotId;
            this.isPlaceHolder = isPlaceHolder;
            this.subtaskSplits = subtaskSplits;
        }

        public List<FileStoreSourceSplit> remove(int subtask) {
            return subtaskSplits.remove(subtask); // 移除对应子任务的分片
        }

        public void add(int subtask, FileStoreSourceSplit split) {
            // 确保分片的快照ID与当前快照ID一致
            Preconditions.checkArgument(
                    ((DataSplit) split.split()).snapshotId() == snapshotId,
                    "快照ID不匹配，请提交问题。");
            subtaskSplits.computeIfAbsent(subtask, id -> new ArrayList<>()).add(split);
        }

        public void addAll(int subtask, List<FileStoreSourceSplit> splits) {
            // 确保对应的子任务没有未处理的分片
            Preconditions.checkArgument(
                    !subtaskSplits.containsKey(subtask),
                    "子任务未处理的分片列表不为空，这是一处Bug，请提交问题。");
            // 验证所有分片的快照ID一致
            splits.forEach(
                    split ->
                            Preconditions.checkArgument(
                                    ((DataSplit) split.split()).snapshotId() == snapshotId,
                                    "快照ID不匹配"));
            subtaskSplits.put(subtask, splits); // 批量添加分片
        }

        public boolean empty() {
            return subtaskSplits.isEmpty() || isPlaceHolder; // 检查是否为空或占位分片
        }
    }
}
