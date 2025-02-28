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
import org.apache.paimon.utils.BinPacking;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.flink.utils.TableScanUtils.getSnapshotId;

/**
 * 预先根据权重计算每个任务应该处理哪些分片，然后公平地分配分片。
 */
public class PreAssignSplitAssigner implements SplitAssigner {

    /** 默认分片批量大小，避免超出 `akka.framesize` 限制。 */
    private final int splitBatchSize;

    private final Map<Integer, LinkedList<FileStoreSourceSplit>> pendingSplitAssignment;

    private final AtomicInteger numberOfPendingSplits;

    public PreAssignSplitAssigner(
            int splitBatchSize,
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits) {
        this.splitBatchSize = splitBatchSize;
        this.pendingSplitAssignment =
                createBatchFairSplitAssignment(splits, context.currentParallelism());
        this.numberOfPendingSplits = new AtomicInteger(splits.size());
    }

    @Override
    public List<FileStoreSourceSplit> getNext(int subtask, @Nullable String hostname) {
        // 分批分配操作有两个目的：
        // 当批处理读取时，为了防止少数任务读取所有数据（例如，当前资源只能调度部分任务）
        // 均衡分片分布
        Queue<FileStoreSourceSplit> taskSplits = pendingSplitAssignment.get(subtask);
        List<FileStoreSourceSplit> assignment = new ArrayList<>();
        while (taskSplits != null && !taskSplits.isEmpty() && assignment.size() < splitBatchSize) {
            assignment.add(taskSplits.poll());
        }
        numberOfPendingSplits.getAndAdd(-assignment.size());
        return assignment;
    }

    @Override
    public void addSplit(int suggestedTask, FileStoreSourceSplit split) {
        pendingSplitAssignment.computeIfAbsent(suggestedTask, k -> new LinkedList<>()).add(split);
        numberOfPendingSplits.incrementAndGet();
    }

    @Override
    public void addSplitsBack(int subtask, List<FileStoreSourceSplit> splits) {
        LinkedList<FileStoreSourceSplit> remainingSplits =
                pendingSplitAssignment.computeIfAbsent(subtask, k -> new LinkedList<>());
        ListIterator<FileStoreSourceSplit> iterator = splits.listIterator(splits.size());
        while (iterator.hasPrevious()) {
            remainingSplits.addFirst(iterator.previous());
        }
        numberOfPendingSplits.getAndAdd(splits.size());
    }

    @Override
    public Collection<FileStoreSourceSplit> remainingSplits() {
        List<FileStoreSourceSplit> splits = new ArrayList<>();
        pendingSplitAssignment.values().forEach(splits::addAll);
        return splits;
    }

    /**
     * 此方法仅用于重新加载恢复批处理执行，因为在流模式下，需要将某些桶分配给某些任务。
     */
    private static Map<Integer, LinkedList<FileStoreSourceSplit>> createBatchFairSplitAssignment(
            Collection<FileStoreSourceSplit> splits, int numReaders) {
        List<List<FileStoreSourceSplit>> assignmentList =
                BinPacking.packForFixedBinNumber(
                        splits, split -> split.split().rowCount(), numReaders);
        Map<Integer, LinkedList<FileStoreSourceSplit>> assignment = new HashMap<>();
        for (int i = 0; i < assignmentList.size(); i++) {
            assignment.put(i, new LinkedList<>(assignmentList.get(i)));
        }
        return assignment;
    }

    @Override
    public Optional<Long> getNextSnapshotId(int subtask) {
        LinkedList<FileStoreSourceSplit> pendingSplits = pendingSplitAssignment.get(subtask);
        return (pendingSplits == null || pendingSplits.isEmpty())
                ? Optional.empty()
                : getSnapshotId(pendingSplits.peekFirst());
    }

    @Override
    public int numberOfRemainingSplits() {
        return numberOfPendingSplits.get();
    }
}
