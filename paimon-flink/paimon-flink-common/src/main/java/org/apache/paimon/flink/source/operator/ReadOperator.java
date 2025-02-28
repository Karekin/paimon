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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

/**
 * 读取从前面的 {@link MonitorFunction} 接收到的 {@link Split splits} 的操作符。
 * 与单例 {@link MonitorFunction} 不同，此操作符可以具有 DOP（并行度）> 1。
 */
public class ReadOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<Split, RowData> {

    private static final long serialVersionUID = 1L;

    // 读取构建器
    private final ReadBuilder readBuilder;

    // 用于读取切片数据的 TableRead 对象（暂留）
    private transient TableRead read;

    // 用于重新使用记录（暂留）
    private transient StreamRecord<RowData> reuseRecord;

    // 用于存储重复使用的 row 数据
    private transient FlinkRowData reuseRow;

    // I/O 管理器，用于管理 I/O 资源
    private transient IOManager ioManager;

    // 源 reader 度量
    private transient FileStoreSourceReaderMetrics sourceReaderMetrics;

    // 输入记录计数器
    private transient Counter numRecordsIn;

    public ReadOperator(ReadBuilder readBuilder) {
        this.readBuilder = readBuilder;
    }

    @Override
    public void open() throws Exception {
        super.open();

        // 初始化来源读取度量指标
        this.sourceReaderMetrics = new FileStoreSourceReaderMetrics(getMetricGroup());
        // 创建事件时间延迟指标
        getMetricGroup()
                .gauge(
                        MetricNames.CURRENT_EMIT_EVENT_TIME_LAG,
                        () -> {
                            long eventTime = sourceReaderMetrics.getLatestFileCreationTime();
                            if (eventTime == FileStoreSourceReaderMetrics.UNDEFINED) {
                                return FileStoreSourceReaderMetrics.UNDEFINED;
                            } else {
                                return System.currentTimeMillis() - eventTime;
                            }
                        });
        // 获取输入记录计数器
        this.numRecordsIn =
                InternalSourceReaderMetricGroup.wrap(getMetricGroup())
                        .getIOMetricGroup()
                        .getNumRecordsInCounter();

        // 创建 I/O 管理器
        this.ioManager =
                IOManager.create(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        // 初始化读取器
        this.read = readBuilder.newRead().withIOManager(ioManager);
        // 初始化重复使用的 row 数据
        this.reuseRow = new FlinkRowData(null);
        // 初始化重复使用的记录
        this.reuseRecord = new StreamRecord<>(reuseRow);
    }

    @Override
    public void processElement(StreamRecord<Split> record) throws Exception {
        // 获取切片
        Split split = record.getValue();
        // 更新度量
        long eventTime =
                ((DataSplit) split)
                        .earliestFileCreationEpochMillis()
                        .orElse(FileStoreSourceReaderMetrics.UNDEFINED);
        sourceReaderMetrics.recordSnapshotUpdate(eventTime);

        // 是否是第一个记录的标志
        boolean firstRecord = true;

        // 创建 CloseableIterator 并遍历切片中的数据
        try (CloseableIterator<InternalRow> iterator =
                     read.createReader(split).toCloseableIterator()) {
            while (iterator.hasNext()) {
                // 跳过第一个记录的计数
                if (firstRecord) {
                    firstRecord = false;
                } else {
                    numRecordsIn.inc(); // 输入记录计数器递增
                }

                // 读取并发送记录
                reuseRow.replace(iterator.next());
                output.collect(reuseRecord);
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ioManager != null) {
            ioManager.close(); // 关闭 I/O 管理器
        }
    }
}
