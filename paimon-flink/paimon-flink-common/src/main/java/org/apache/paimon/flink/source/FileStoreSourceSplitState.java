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

import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 读取器的状态，本质上是 {@link FileStoreSourceSplit} 的可变版本。具有一个“跳过记录数”。
 * <p>
 * 该类用于表示文件存储源读取器的状态，主要包括分片信息以及需要跳过的记录数。
 * </p>
 */
public final class FileStoreSourceSplitState {

    private final FileStoreSourceSplit split; // 表示分片信息

    private long recordsToSkip; // 表示需要跳过的记录数

    /**
     * 构造函数，初始化文件存储源读取器状态。
     *
     * @param split 分片信息
     */
    public FileStoreSourceSplitState(FileStoreSourceSplit split) {
        // 验证 split 不为 null，并进行初始化
        this.split = checkNotNull(split);
        // 初始化需要跳过的记录数
        this.recordsToSkip = split.recordsToSkip();
    }

    /**
     * 设置读取器的位置。
     * <p>
     * 该方法用于设置读取器的位置，例如在恢复时需要跳过的记录数。
     * </p>
     *
     * @param position 记录和位置
     */
    public void setPosition(RecordAndPosition<RowData> position) {
        // 验证传入的 position 的偏移量是否为 NO_OFFSET，确保只处理跳过记录数，而不是偏移量
        checkArgument(position.getOffset() == CheckpointedPosition.NO_OFFSET);
        // 设置需要跳过的记录数
        this.recordsToSkip = position.getRecordSkipCount();
    }

    /**
     * 获取需要跳过的记录数。
     *
     * @return 需要跳过的记录数
     */
    public long recordsToSkip() {
        return recordsToSkip;
    }

    /**
     * 转换为文件存储源分片。
     * <p>
     * 根据当前状态（包括跳过的记录数）更新并返回文件存储源分片的对象。
     * </p>
     *
     * @return {@link FileStoreSourceSplit} 对象
     */
    public FileStoreSourceSplit toSourceSplit() {
        // 根据当前的 recordsToSkip 更新并返回文件存储源分片对象
        return split.updateWithRecordsToSkip(recordsToSkip);
    }
}
