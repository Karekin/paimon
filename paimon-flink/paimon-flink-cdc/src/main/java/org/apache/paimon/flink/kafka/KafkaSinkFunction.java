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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.flink.sink.LogSinkFunction;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaException;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * 一个 {@link FlinkKafkaProducer} 的扩展类，实现了 {@link LogSinkFunction} 接口，用于注册 {@link WriteCallback}。
 */
public class KafkaSinkFunction extends FlinkKafkaProducer<SinkRecord> implements LogSinkFunction {

    private WriteCallback writeCallback; // 写入回调函数

    /**
     * 创建一个 {@link KafkaSinkFunction}，用于将数据写入指定的 Kafka 主题。
     * 该 sink 接受一个 {@link KafkaSerializationSchema}，用于将用户对象序列化为 Kafka 可消费的字节数组，并支持消息的分区信息。
     *
     * @param defaultTopic 默认要写入数据的主题
     * @param serializationSchema 可序列化的序列化方案，用于将用户对象转换为 Kafka 能够消费的字节数组（支持键/值消息）
     * @param producerConfig Kafka 生产者配置属性。'bootstrap.servers.' 是唯一必需的参数，用于指定 Kafka 集群的地址。
     * @param semantic 定义该生产者使用的语义（如 {@link KafkaSinkFunction.Semantic} 中的语义）
     */
    public KafkaSinkFunction(
            String defaultTopic,
            KafkaSerializationSchema<SinkRecord> serializationSchema,
            Properties producerConfig,
            KafkaSinkFunction.Semantic semantic) {
        super(defaultTopic, serializationSchema, producerConfig, semantic);
    }

    /**
     * 设置写入回调函数。
     * @param writeCallback 要设置的写入回调函数
     */
    public void setWriteCallback(WriteCallback writeCallback) {
        this.writeCallback = writeCallback;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration); // 调用父类的 open 方法，初始化 Kafka 生产者
        Callback baseCallback = requireNonNull(callback); // 获取基础回调函数

        // 设置一个新的回调函数，该回调函数在消息发送成功后触发写入回调
        callback = (metadata, exception) -> {
            // 如果写入回调函数不为空且返回的元数据包含偏移量信息
            if (writeCallback != null && metadata != null && metadata.hasOffset()) {
                writeCallback.onCompletion(metadata.partition(), metadata.offset()); // 调用写入回调函数，通知写入完成
            }
            baseCallback.onCompletion(metadata, exception); // 调用基础回调函数，完成消息发送的清理工作
        };
    }

    @Override
    public void flush() throws FlinkKafkaException {
        super.preCommit(super.currentTransaction()); // 调用父类的预提交方法，刷新缓存的记录
    }
}
