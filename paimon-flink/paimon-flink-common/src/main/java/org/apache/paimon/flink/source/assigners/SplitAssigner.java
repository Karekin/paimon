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

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * 切片分配器，负责决定哪些切片应该由哪些节点处理，确定切片处理的顺序和本地性。
 */
public interface SplitAssigner {

    /**
     * 获取下一个切片。
     *
     * <p>当此方法返回空的 {@code Optional} 时，表示切片集合已完成，源将在读取器完成当前切片后结束。
     */
    List<FileStoreSourceSplit> getNext(int subtask, @Nullable String hostname);

    /**
     * 向分配器添加指定子任务的切片。
     *
     * @param suggestedTask 建议的子任务 ID
     * @param splits 切片
     */
    void addSplit(int suggestedTask, FileStoreSourceSplit splits);

    /**
     * 将一组切片添加到分配器中。这通常发生在某些切片处理失败需要重新添加，或发现新的切片需要处理时。
     *
     * @param subtask 子任务 ID
     * @param splits 切片列表
     */
    void addSplitsBack(int subtask, List<FileStoreSourceSplit> splits);

    /**
     * 获取分配器中待处理的剩余切片。
     *
     * @return 剩余切片的集合
     */
    Collection<FileStoreSourceSplit> remainingSplits();

    /**
     * 获取下一个切片的快照 ID。
     *
     * @param subtask 子任务 ID
     * @return 下一个快照 ID
     */
    Optional<Long> getNextSnapshotId(int subtask);

    /**
     * 获取分配器中剩余的切片数量。此方法必须是线程安全的。
     *
     * @return 剩余切片的数量
     */
    int numberOfRemainingSplits();
}
