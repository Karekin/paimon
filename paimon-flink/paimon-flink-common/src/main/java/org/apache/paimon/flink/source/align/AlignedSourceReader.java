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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.source.FileStoreSourceReader;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplitState;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.table.source.TableRead;

import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/**
 * 与 {@link FileStoreSourceReader} 的区别在于，只有在分配的分片完全消费后，才会进行检查点操作，并请求下一批分片。
 * <p>该类实现了对齐的源读取器，确保在分片完全处理后才进行检查点，从而保证数据的一致性。</p>
 */
public class AlignedSourceReader extends FileStoreSourceReader
        implements ExternallyInducedSourceReader<RowData, FileStoreSourceSplit> {

    /**
     * 用于存储记录的队列，包含分片 ID 和记录迭代器。
     * <p>该队列用于在读取器中缓存从分片中读取的记录。</p>
     */
    private final FutureCompletingBlockingQueue<
            RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>>>
            elementsQueue;

    /**
     * 下一个检查点的 ID。
     * <p>用于记录即将触发的检查点 ID，以便在分片处理完成后触发检查点。</p>
     */
    private Long nextCheckpointId;

    /**
     * 构造函数，初始化对齐的源读取器。
     *
     * @param readerContext 读取器上下文，包含读取器的配置和状态信息
     * @param tableRead 表读取器，用于从表中读取数据
     * @param metrics 读取器的性能指标
     * @param ioManager IO 管理器，用于管理输入输出资源
     * @param limit 读取限制，可选参数
     * @param elementsQueue 记录队列，用于存储从分片中读取的记录
     */
    public AlignedSourceReader(
            SourceReaderContext readerContext,
            TableRead tableRead,
            FileStoreSourceReaderMetrics metrics,
            IOManager ioManager,
            @Nullable Long limit,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>>>
                    elementsQueue) {
        super(readerContext, tableRead, metrics, ioManager, limit, elementsQueue); // 调用父类构造函数
        this.elementsQueue = elementsQueue; // 保存记录队列
        this.nextCheckpointId = null; // 初始化下一个检查点 ID 为 null
    }

    /**
     * 处理源事件。
     * <p>如果事件是检查点事件，则更新下一个检查点 ID 并通知队列有可用元素。</p>
     *
     * @param sourceEvent 源事件
     */
    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof CheckpointEvent) { // 如果事件是检查点事件
            nextCheckpointId = ((CheckpointEvent) sourceEvent).getCheckpointId(); // 更新下一个检查点 ID
            elementsQueue.notifyAvailable(); // 通知队列有可用元素
        } else {
            super.handleSourceEvents(sourceEvent); // 调用父类方法处理其他事件
        }
    }

    /**
     * 当分片处理完成时调用的方法。
     * <p>在对齐的源读取器中，该方法不执行任何操作，忽略分片处理完成的事件。</p>
     *
     * @param finishedSplitIds 已完成的分片 ID 映射
     */
    @Override
    protected void onSplitFinished(Map<String, FileStoreSourceSplitState> finishedSplitIds) {
        // ignore（忽略）
    }

    /**
     * 判断是否应该触发检查点。
     * <p>如果当前没有分配的分片，并且存在下一个检查点 ID，则触发检查点并返回检查点 ID。</p>
     *
     * @return 如果应该触发检查点，则返回检查点 ID，否则返回空
     */
    @Override
    public Optional<Long> shouldTriggerCheckpoint() {
        if (getNumberOfCurrentlyAssignedSplits() == 0 && nextCheckpointId != null) { // 如果没有分配的分片且存在下一个检查点 ID
            long checkpointId = nextCheckpointId; // 保存检查点 ID
            nextCheckpointId = null; // 清空下一个检查点 ID
            context.sendSplitRequest(); // 请求分配新的分片
            return Optional.of(checkpointId); // 返回检查点 ID
        }
        return Optional.empty(); // 如果不满足条件，返回空
    }
}
