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

package org.apache.paimon.table.source;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

/**
 * 用于流式处理的 {@link TableScan}，支持 {@link #checkpoint} 和 {@link #restore} 方法。
 *
 * <p>注意：{@link #checkpoint} 方法将返回下一个快照 ID。
 *
 * @since 0.4.0
 */
@Public
public interface StreamTableScan extends TableScan, Restorable<Long> {

    /**
     * 获取当前已消费快照的水印。
     *
     * @return 当前水印（可能为 null，表示没有水印）
     */
    @Nullable
    Long watermark();

    /**
     * 从指定的下一个快照 ID 恢复扫描。
     *
     * @param nextSnapshotId 下一个快照 ID（可能为 null，表示从头开始扫描）
     */
    @Override
    void restore(@Nullable Long nextSnapshotId);

    /**
     * 创建检查点并返回下一个快照 ID。
     *
     * @return 下一个快照 ID（可能为 null，表示没有下一个快照）
     */
    @Nullable
    @Override
    Long checkpoint();

    /**
     * 通知检查点完成，并提供下一个快照 ID。
     *
     * @param nextSnapshot 下一个快照 ID（可能为 null，表示没有下一个快照）
     */
    void notifyCheckpointComplete(@Nullable Long nextSnapshot);
}
