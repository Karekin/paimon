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

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.table.source.TableRead;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/**
 * 一个从 {@link FileStoreSourceSplit} 中读取记录的 {@link SourceReader}。
 * <p>该 {@link SourceReader} 的具体实现，用于读取文件存储中的数据。</p>
 */
public class FileStoreSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
        RecordIterator<RowData>, RowData, FileStoreSourceSplit, FileStoreSourceSplitState> {

    private final IOManager ioManager; // 基础设施管理器，用于管理输入输出资源

    private long lastConsumeSnapshotId = Long.MIN_VALUE; // 记录最近消费的快照ID

    /**
     * 构造函数，初始化文件存储源读取器。
     * <p>主要用于初始化单线程模式下的源读取器。</p>
     *
     * @param readerContext 读取器上下文，包含读取器的配置和状态信息
     * @param tableRead 表读取接口，用于从表中读取数据
     * @param metrics 读取器的性能指标
     * @param ioManager 基础设施管理器，用于管理IO资源
     * @param limit 读取限制，可选参数
     */
    public FileStoreSourceReader(
            SourceReaderContext readerContext,
            TableRead tableRead,
            FileStoreSourceReaderMetrics metrics,
            IOManager ioManager,
            @Nullable Long limit) {
        // 初始化父类，设置单线程模式下的源读取器
        super(
                () -> new FileStoreSourceSplitReader(tableRead, RecordLimiter.create(limit), metrics),
                // 定义记录的输出方式
                (element, output, state) -> FlinkRecordsWithSplitIds.emitRecord(
                        readerContext, element, output, state, metrics),
                readerContext.getConfiguration(),
                readerContext);
        this.ioManager = ioManager; // 保存基础设施管理器
    }

    /**
     * 构造函数，初始化文件存储源读取器。
     * <p>主要用于初始化线程安全的源读取器，适用于多线程或多任务环境。</p>
     *
     * @param readerContext 读取器上下文
     * @param tableRead 表读取接口
     * @param metrics 读取器的性能指标
     * @param ioManager 基础设施管理器
     * @param limit 读取限制
     * @param elementsQueue 记录队列，用于存储读取的记录
     */
    public FileStoreSourceReader(
            SourceReaderContext readerContext,
            TableRead tableRead,
            FileStoreSourceReaderMetrics metrics,
            IOManager ioManager,
            @Nullable Long limit,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordIterator<RowData>>> elementsQueue) {
        // 初始化父类，设置线程安全的源读取器
        super(
                elementsQueue,
                () -> new FileStoreSourceSplitReader(tableRead, RecordLimiter.create(limit), metrics),
                // 定义记录的输出方式
                (element, output, state) -> FlinkRecordsWithSplitIds.emitRecord(
                        readerContext, element, output, state, metrics),
                readerContext.getConfiguration(),
                readerContext);
        this.ioManager = ioManager; // 保存基础设施管理器
    }

    @Override
    public void start() {
        // 如果在检查点恢复过程中没有获取到分配的分片，则请求获取分片
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest(); // 向协调器请求分配分片
        }
    }

    @Override
    protected void onSplitFinished(Map<String, FileStoreSourceSplitState> finishedSplitIds) {
        // 每次消费一个分片时调用此方法
        // 可能会一次性从协调器接收到多个分片，需要在处理完所有分片后请求更多分片
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest(); // 向协调器请求更多分片
        }

        // 计算已完成分片中的最大快照ID
        long maxFinishedSplits =
                finishedSplitIds.values().stream()
                        .map(splitState -> TableScanUtils.getSnapshotId(splitState.toSourceSplit()))
                        .filter(Optional::isPresent)
                        .mapToLong(Optional::get)
                        .max()
                        .orElse(Long.MIN_VALUE);

        // 如果当前的快照ID小于最大快照ID，则更新并发送进度事件
        if (lastConsumeSnapshotId < maxFinishedSplits) {
            lastConsumeSnapshotId = maxFinishedSplits;
            context.sendSourceEventToCoordinator(
                    new ReaderConsumeProgressEvent(lastConsumeSnapshotId)); // 发送消费进度事件
        }
    }

    @Override
    protected FileStoreSourceSplitState initializedState(FileStoreSourceSplit split) {
        // 初始化分片状态
        return new FileStoreSourceSplitState(split);
    }

    @Override
    protected FileStoreSourceSplit toSplitType(
            String splitId, FileStoreSourceSplitState splitState) {
        // 将分片状态转换为分片类型
        return splitState.toSourceSplit();
    }

    @Override
    public void close() throws Exception {
        super.close(); // 调用父类的关闭方法
        ioManager.close(); // 关闭基础设施管理器
    }
}
