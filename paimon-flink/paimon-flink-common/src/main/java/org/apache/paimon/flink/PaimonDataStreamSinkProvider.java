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

package org.apache.paimon.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.data.RowData;

import java.util.function.Function;

/**
 * Paimon 的 {@link DataStreamSinkProvider} 实现类。
 * 该类用于创建 DataStreamSink，作为 Flink 任务的最终数据接收器（Sink）。
 */
public class PaimonDataStreamSinkProvider implements DataStreamSinkProvider {

    /** 数据流转换函数，负责将 DataStream<RowData> 转换为 DataStreamSink<?> */
    private final Function<DataStream<RowData>, DataStreamSink<?>> producer;

    /**
     * 构造方法，初始化 PaimonDataStreamSinkProvider。
     *
     * @param producer 一个函数式接口，接收 DataStream<RowData> 并返回 DataStreamSink<?>，
     *                 用于定义 Sink 逻辑
     */
    public PaimonDataStreamSinkProvider(Function<DataStream<RowData>, DataStreamSink<?>> producer) {
        this.producer = producer;
    }

    /**
     * 消费数据流，并生成最终的 Sink。
     * 该方法会调用 producer.apply(dataStream)，将 DataStream<RowData> 转换为 DataStreamSink<?>。
     *
     * @param providerContext 提供者上下文（ProviderContext），可用于获取相关配置信息
     * @param dataStream 需要写入的 Flink 数据流
     * @return 转换后的 DataStreamSink，用于最终的数据存储
     */
    @Override
    public DataStreamSink<?> consumeDataStream(
            ProviderContext providerContext, DataStream<RowData> dataStream) {
        return producer.apply(dataStream);
    }
}

