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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;

import static org.apache.paimon.flink.utils.TableScanUtils.getSnapshotId;

/**
 * 分片按任务请求的顺序以抢占式的方式分配。每次只将一个分片分配给任务。不关心分片与子任务之间的关系，只创建一个队列，并按顺序获取分片。
 */
public class FIFOSplitAssigner implements SplitAssigner {

    private final LinkedList<FileStoreSourceSplit> pendingSplitAssignment; // 待分配的分片队列

    public FIFOSplitAssigner(Collection<FileStoreSourceSplit> splits) {
        this.pendingSplitAssignment = new LinkedList<>(splits); // 初始化分片队列
    }

    @Override
    public List<FileStoreSourceSplit> getNext(int subtask, @Nullable String hostname) {
        FileStoreSourceSplit split = pendingSplitAssignment.poll(); // 从队列中获取下一个分片
        // 如果没有分片，则返回空列表，否则返回包含一个分片的列表
        return split == null ? Collections.emptyList() : Collections.singletonList(split);
    }

    @Override
    public void addSplit(int suggestedTask, FileStoreSourceSplit split) {
        pendingSplitAssignment.add(split); // 将新分片添加到队列末尾
    }

    @Override
    public void addSplitsBack(int subtask, List<FileStoreSourceSplit> splits) {
        ListIterator<FileStoreSourceSplit> iterator = splits.listIterator(splits.size()); // 获取分片列表的迭代器
        while (iterator.hasPrevious()) { // 从最后一个分片开始
            pendingSplitAssignment.addFirst(iterator.previous()); // 将分片添加到队列头部
        }
    }

    @Override
    public Collection<FileStoreSourceSplit> remainingSplits() {
        return new ArrayList<>(pendingSplitAssignment); // 返回剩余分片的列表
    }

    @Override
    public Optional<Long> getNextSnapshotId(int subtask) {
        // 如果队列为空，返回空；否则返回队列头部分片的快照 ID
        return pendingSplitAssignment.isEmpty()
                ? Optional.empty()
                : getSnapshotId(pendingSplitAssignment.peekFirst());
    }

    @Override
    public int numberOfRemainingSplits() {
        return pendingSplitAssignment.size(); // 返回剩余分片的数量
    }
}
