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

import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.utils.Reference;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Set;

/**
 * 实现 RecordsWithSplitIds 接口，用于封装一个包含单个迭代器记录（RecordIterator）的类。
 * 此类确保在消费迭代器记录时不会发生 checkpoint 的分割。
 */
public class FlinkRecordsWithSplitIds implements RecordsWithSplitIds<RecordIterator<RowData>> {

    // 当前分片标识，可为空（@Nullable 表示可能为 null）
    @Nullable private String splitId;

    // 当前分片对应的记录迭代器引用（Reference包装类），可为空
    @Nullable private Reference<RecordIterator<RowData>> recordsForSplitCurrent;

    // 当前分片的记录迭代器，可为空
    @Nullable private final RecordIterator<RowData> recordsForSplit;

    // 已消费完毕的分片标识集合
    private final Set<String> finishedSplits;

    /**
     * 构造方法，初始化 FlinkRecordsWithSplitIds 对象。
     * @param splitId 当前分片标识，可为空
     * @param recordsForSplit 当前分片的记录迭代器，可为空
     * @param finishedSplits 已消费完毕的分片标识集合
     */
    private FlinkRecordsWithSplitIds(
            @Nullable String splitId,
            @Nullable RecordIterator<RowData> recordsForSplit,
            Set<String> finishedSplits) {
        this.splitId = splitId;
        this.recordsForSplit = recordsForSplit;
        this.finishedSplits = finishedSplits;
    }

    /**
     * 获取下一个分片标识。
     * 调用此方法后，当前分片标识会被置空，并且当前分片的记录迭代器会被包装到 Reference 中。
     * @return 下一个分片标识，如果没有则返回 null
     */
    @Nullable
    @Override
    public String nextSplit() {
        // 将当前分片标识保存到临时变量
        final String nextSplit = this.splitId;
        this.splitId = null; // 当前分片标识置空

        // 根据 nextSplit 是否为 null，决定 recordsForSplitCurrent 的值
        this.recordsForSplitCurrent =
                nextSplit != null ? new Reference<>(this.recordsForSplit) : null;

        return nextSplit;
    }

    /**
     * 获取当前分片的记录迭代器。
     * @return 当前分片的记录迭代器，如果没有则抛出异常
     */
    @Nullable
    @Override
    public RecordIterator<RowData> nextRecordFromSplit() {
        if (this.recordsForSplitCurrent == null) {
            throw new IllegalStateException("No records available for the current split");
        }

        // 获取记录迭代器并置空记录引用
        RecordIterator<RowData> recordsForSplit = this.recordsForSplitCurrent.get();
        this.recordsForSplitCurrent.set(null);
        return recordsForSplit;
    }

    /**
     * 获取已消费完毕的分片标识集合。
     * @return 已消费完毕的分片标识集合
     */
    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }

    /**
     * 回收资源（例如释放记录迭代器的批处理）。
     */
    @Override
    public void recycle() {
        if (recordsForSplit != null) {
            recordsForSplit.releaseBatch(); // 释放记录迭代器的批处理
        }
    }

    /**
     * 创建一个新的 FlinkRecordsWithSplitIds 实例，包含给定的分片标识和记录迭代器。
     * @param splitId 分片标识
     * @param recordsForSplit 记录迭代器
     * @return 新的 FlinkRecordsWithSplitIds 实例
     */
    public static FlinkRecordsWithSplitIds forRecords(
            String splitId, RecordIterator<RowData> recordsForSplit) {
        return new FlinkRecordsWithSplitIds(splitId, recordsForSplit, Collections.emptySet());
    }

    /**
     * 创建一个新的 FlinkRecordsWithSplitIds 实例，表示一个已消费完毕的分片。
     * @param splitId 已消费完毕的分片标识
     * @return 新的 FlinkRecordsWithSplitIds 实例
     */
    public static FlinkRecordsWithSplitIds finishedSplit(String splitId) {
        return new FlinkRecordsWithSplitIds(null, null, Collections.singleton(splitId));
    }

    /**
     * 发送记录到消费者的输出流中，并更新相关的指标和状态。
     * @param context 消费者上下文（SourceReaderContext）
     * @param element 记录迭代器（RecordIterator<RowData>）
     * @param output 输出流（SourceOutput<RowData>）
     * @param state 分片状态（FileStoreSourceSplitState）
     * @param metrics 相关指标（FileStoreSourceReaderMetrics）
     */
    public static void emitRecord(
            SourceReaderContext context,
            RecordIterator<RowData> element,
            SourceOutput<RowData> output,
            FileStoreSourceSplitState state,
            FileStoreSourceReaderMetrics metrics) {
        long timestamp = TimestampAssigner.NO_TIMESTAMP;
        // 如果最新的文件创建时间已被定义，则使用该时间作为时间戳
        if (metrics.getLatestFileCreationTime() != FileStoreSourceReaderMetrics.UNDEFINED) {
            timestamp = metrics.getLatestFileCreationTime();
        }

        // 获取输入记录的计数器
        org.apache.flink.metrics.Counter numRecordsIn =
                context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();
        boolean firstRecord = true;

        RecordAndPosition<RowData> record;
        // 逐条处理记录迭代器中的记录
        while ((record = element.next()) != null) {
            // 第一条记录已经被 SourceReaderBase.pollNext 计数
            if (firstRecord) {
                firstRecord = false;
            } else {
                numRecordsIn.inc(); // 增加记录计数
            }

            // 将记录发送到输出流
            output.collect(record.getRecord(), timestamp);
            // 更新分片状态的位置
            state.setPosition(record);
        }
    }
}
