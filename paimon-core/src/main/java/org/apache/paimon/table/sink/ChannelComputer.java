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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.SerializableFunction;

import java.io.Serializable;

/**
 * A utility class to compute which downstream channel a given record should be sent to.
 *
 * @param <T> type of record
 *
 * <p>该接口是一个工具类，用于计算给定记录应被发送到哪一个下游通道（channel）。
 * 用户可以在数据分发或分区等场景下使用此逻辑来确定数据流向。
 */
public interface ChannelComputer<T> extends Serializable {

    /**
     * 在实际使用前进行初始化，用于设置下游通道的数量。
     *
     * @param numChannels 下游通道的总数
     */
    void setup(int numChannels);

    /**
     * 根据给定的记录计算应该发送到哪个通道。
     *
     * @param record 待分配通道的记录
     * @return 该记录应被发送到的通道索引
     */
    int channel(T record);

    /**
     * 根据给定的分区键（partition）和桶（bucket），以及通道数量，计算目标通道。
     *
     * @param partition 分区键，使用其 hashCode 进行初始通道计算
     * @param bucket 需要偏移的桶索引
     * @param numChannels 下游通道的总数
     * @return 选择后的通道索引
     */
    static int select(BinaryRow partition, int bucket, int numChannels) {
        int startChannel = Math.abs(partition.hashCode()) % numChannels;
        return (startChannel + bucket) % numChannels;
    }

    /**
     * 根据桶（bucket）和通道数量，计算目标通道。适用于只需要根据桶进行分发的场景。
     *
     * @param bucket 桶编号
     * @param numChannels 下游通道的总数
     * @return 选择后的通道索引
     */
    static int select(int bucket, int numChannels) {
        return bucket % numChannels;
    }

    /**
     * 将一个 ChannelComputer 逻辑转换并应用到另一种类型的记录上。
     *
     * <p>转换的原理是先将新记录类型 R 通过 {@code converter} 函数转换为旧记录类型 T，
     * 然后使用原有的 {@code ChannelComputer} {@code input} 进行通道计算。
     *
     * @param input 原始的 ChannelComputer，用于对类型 T 的记录做分发计算
     * @param converter 一个可序列化的函数，用于将类型 R 转换为 T
     * @param <T> 原始记录类型
     * @param <R> 新的记录类型
     * @return 一个新的 {@code ChannelComputer<R>}，可用于对类型 R 的记录做通道选择
     */
    static <T, R> ChannelComputer<R> transform(
            ChannelComputer<T> input, SerializableFunction<R, T> converter) {
        return new ChannelComputer<R>() {
            @Override
            public void setup(int numChannels) {
                // 在内部依旧调用原 ChannelComputer 的 setup
                input.setup(numChannels);
            }

            @Override
            public int channel(R record) {
                // 在内部将 R 转换为 T，并让原 ChannelComputer 进行通道计算
                return input.channel(converter.apply(record));
            }
        };
    }
}

