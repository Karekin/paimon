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

package org.apache.paimon.compact;

import org.apache.paimon.io.DataFileMeta;

import java.io.Closeable;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 *  合并管理器（Compaction Manager），用于提交合并任务（Compaction Task）。
 *
 *  该接口负责管理数据文件的合并任务，包括触发合并、等待合并完成、取消合并任务等功能。
 */
public interface CompactManager extends Closeable {

    /**
     *  是否需要等待最新的合并任务完成。
     *
     *  在某些情况下，为了保证数据一致性或减少 Level 0 文件的堆积，
     *  可能需要等待当前的合并任务完成后再进行新的写入或提交。
     *
     *  @return 如果需要等待合并任务完成，则返回 true，否则返回 false。
     */
    boolean shouldWaitForLatestCompaction();

    /**
     *  在执行检查点（Checkpoint）之前，是否需要等待合并任务完成。
     *
     *  这通常用于防止在检查点过程中由于未完成的合并任务导致数据状态不稳定。
     *
     *  @return 如果需要等待合并任务完成，则返回 true，否则返回 false。
     */
    boolean shouldWaitForPreparingCheckpoint();

    /**
     *  向合并管理器添加一个新数据文件。
     *
     *  该方法用于通知管理器，新生成的文件需要被管理，并可能在后续的合并任务中被处理。
     *
     *  @param file 新的数据文件元信息 {@link DataFileMeta}。
     */
    void addNewFile(DataFileMeta file);

    /**
     *  获取当前管理的所有数据文件列表。
     *
     *  这可以用于查看当前存储的所有数据文件，包括尚未被合并的文件。
     *
     *  @return 返回当前所有的数据文件集合。
     */
    Collection<DataFileMeta> allFiles();

    /**
     *  触发新的合并任务（Compaction Task）。
     *
     *  合并任务用于减少文件数量，提高查询性能，并控制 Level 0 文件的增长。
     *
     *  @param fullCompaction 是否强制执行完整合并（Full Compaction）。
     *         - 若 `fullCompaction=true`，则执行彻底合并，确保合并后的文件结构更加紧凑。
     *         - 若 `fullCompaction=false`，则执行增量合并（Partial Compaction），减少 I/O 开销。
     */
    void triggerCompaction(boolean fullCompaction);

    /**
     *  获取合并任务的结果。
     *
     *  @param blocking 是否阻塞等待合并任务完成：
     *         - 若 `blocking=true`，则调用线程会一直等待，直到合并任务完成并返回结果。
     *         - 若 `blocking=false`，则如果合并任务尚未完成，可能返回空的 `Optional` 结果。
     *
     *  @return 返回合并结果 {@link CompactResult}，如果合并尚未完成且 `blocking=false`，则可能返回 `Optional.empty()`。
     *
     *  @throws ExecutionException  如果合并任务执行过程中发生异常
     *  @throws InterruptedException  如果当前线程在等待合并任务完成时被中断
     */
    Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException;

    /**
     *  取消当前正在运行的合并任务。
     *
     *  在任务取消后，可能会有部分文件未被正确合并，需要在下一次合并任务中重新处理。
     */
    void cancelCompaction();

    /**
     *  检查是否有正在进行的合并任务，或者是否有合并结果尚未被获取。
     *
     *  @return 如果有合并任务正在执行或合并结果尚未被获取，则返回 true，否则返回 false。
     */
    boolean isCompacting();
}

