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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;

/**
 * {@link ChannelComputer} 的实现类，用于 {@link InternalRow} 类型的数据分发。
 *
 * <p>该类的作用是基于表模式（schema）和日志 Sink 配置，计算 InternalRow 记录应当分发到哪个下游通道（channel）。
 */
public class RowDataChannelComputer implements ChannelComputer<InternalRow> {

    private static final long serialVersionUID = 1L;

    // 表的模式（Schema），用于提取分区字段
    private final TableSchema schema;

    // 是否有日志 Sink（如 Kafka），如果有，则分区字段不会影响通道选择
    private final boolean hasLogSink;

    // 下游通道数量，需在 setup 方法中初始化
    private transient int numChannels;

    // 负责提取分区键和桶 ID 的工具类
    private transient KeyAndBucketExtractor<InternalRow> extractor;

    /**
     * 构造方法，初始化表模式（schema）和日志 Sink 标志。
     *
     * @param schema 表的模式信息
     * @param hasLogSink 是否存在日志 Sink（如 Kafka）
     */
    public RowDataChannelComputer(TableSchema schema, boolean hasLogSink) {
        this.schema = schema;
        this.hasLogSink = hasLogSink;
    }

    /**
     * 初始化通道数量，并创建用于提取分区键和桶 ID 的工具类。
     *
     * @param numChannels 下游通道的数量
     */
    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.extractor = new FixedBucketRowKeyExtractor(schema);
    }

    /**
     * 计算指定记录应当被分配到的通道。
     *
     * @param record 要处理的 InternalRow 记录
     * @return 该记录应该被发送到的通道索引
     */
    @Override
    public int channel(InternalRow record) {
        // 设置当前记录，以便从中提取分区键和桶 ID
        extractor.setRecord(record);
        return channel(extractor.partition(), extractor.bucket());
    }

    /**
     * 根据分区键和桶 ID 计算通道索引。
     *
     * <p>对于日志 Sink（如 Kafka），仅基于 bucket 计算通道索引，而忽略分区字段。
     * 这样即使是不同的分区，但相同的 bucket 仍然会映射到相同的通道。
     *
     * <p>对于其他情况，使用分区键和 bucket 共同计算通道索引。
     *
     * @param partition 记录的分区键
     * @param bucket 记录的桶 ID
     * @return 计算出的通道索引
     */
    public int channel(BinaryRow partition, int bucket) {
        return hasLogSink
                ? ChannelComputer.select(bucket, numChannels)  // 仅基于 bucket 计算
                : ChannelComputer.select(partition, bucket, numChannels);  // 结合分区键计算
    }

    /**
     * 返回该计算器的描述信息。
     *
     * @return 返回 "shuffle by bucket"，表示该计算器基于 bucket 进行数据分发。
     */
    @Override
    public String toString() {
        return "shuffle by bucket";
    }
}
