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

import org.apache.paimon.table.sink.ChannelComputer;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * {@link StreamPartitioner} 的一个具体实现，封装了 {@link ChannelComputer}，
 * 用于根据自定义的通道计算逻辑来决定数据流的分区。
 */
public class FlinkStreamPartitioner<T> extends StreamPartitioner<T> {

    // 通道计算器，用于确定数据流中元素的目标通道
    private final ChannelComputer<T> channelComputer;

    /**
     * 构造函数，初始化通道计算器。
     *
     * @param channelComputer 负责计算数据应该发送到哪个通道的通道计算器
     */
    public FlinkStreamPartitioner(ChannelComputer<T> channelComputer) {
        this.channelComputer = channelComputer;
    }

    /**
     * 设置分区器的通道数量，并初始化通道计算器。
     *
     * @param numberOfChannels 下游的并行子任务数量，即可用的通道数
     */
    @Override
    public void setup(int numberOfChannels) {
        super.setup(numberOfChannels);
        channelComputer.setup(numberOfChannels);
    }

    /**
     * 选择数据记录的目标通道。
     *
     * @param record 需要分区的数据记录，包含序列化代理
     * @return 计算出的目标通道索引
     */
    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        // 通过 channelComputer 计算数据应该发送到哪个通道
        return channelComputer.channel(record.getInstance().getValue());
    }

    /**
     * 复制当前分区器。由于该分区器本身是无状态的，直接返回自身。
     *
     * @return 当前分区器自身
     */
    @Override
    public StreamPartitioner<T> copy() {
        return this;
    }

    /**
     * 获取下游子任务的状态映射方式。
     * 该分区器使用 FULL 映射，即所有数据分布方式都完全复制到所有并行任务。
     *
     * @return {@link SubtaskStateMapper#FULL}
     */
    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.FULL;
    }

    /**
     * 指示该分区器是否为点对点（pointwise）模式。
     * 由于通道计算逻辑可能不是严格的点对点映射，因此返回 false。
     *
     * @return false，表示该分区策略不是点对点模式
     */
    @Override
    public boolean isPointwise() {
        return false;
    }

    /**
     * 返回通道计算器的字符串表示形式。
     *
     * @return 通道计算器的字符串表示
     */
    @Override
    public String toString() {
        return channelComputer.toString();
    }

    /**
     * 对输入流应用自定义的分区策略。
     *
     * @param input           需要分区的输入数据流
     * @param channelComputer 负责决定数据流元素分区的通道计算器
     * @param parallelism     指定的并行度（可选），如果为 null，则沿用输入流的并行度
     * @return 经过自定义分区的 {@link DataStream}
     */
    public static <T> DataStream<T> partition(
            DataStream<T> input, ChannelComputer<T> channelComputer, Integer parallelism) {
        // 创建自定义分区器
        FlinkStreamPartitioner<T> partitioner = new FlinkStreamPartitioner<>(channelComputer);

        // 创建分区转换，将输入流应用自定义分区器
        PartitionTransformation<T> partitioned =
                new PartitionTransformation<>(input.getTransformation(), partitioner);

        // 如果指定了并行度，则应用该并行度
        if (parallelism != null) {
            partitioned.setParallelism(parallelism);
        }

        // 返回新的数据流，包含了自定义分区逻辑
        return new DataStream<>(input.getExecutionEnvironment(), partitioned);
    }
}

