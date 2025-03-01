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

package org.apache.paimon.flink.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/**
 * 查询服务拓扑构建类。
 * 用于构建查询服务的处理拓扑，包括数据流的读取、分区、执行和结果注册。
 */
public class QueryService {

    /**
     * 构建查询服务拓扑。
     *
     * @param env 流执行环境
     * @param table 目标表
     * @param parallelism 并行度
     */
    public static void build(StreamExecutionEnvironment env, Table table, int parallelism) {
        // 获取当前环境配置
        ReadableConfig conf = env.getConfiguration();

        // 确保查询服务在流模式下运行
        Preconditions.checkArgument(
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING,
                "查询服务仅支持流处理模式。");

        // 将表对象转换为文件存储表
        FileStoreTable storeTable = (FileStoreTable) table;

        // 检查文件存储表的存储桶模式和主键
        if (storeTable.bucketMode() != BucketMode.HASH_FIXED
                || storeTable.schema().primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "表 “"
                            + table.name()
                            + "” 的存储桶模式不是固定模式，或者表没有主键。");
        }

        // 获取查询文件监控器构建的数据流
        DataStream<InternalRow> stream = QueryFileMonitor.build(env, table);

        // 按照通道计算机对数据流进行分区
        stream = partition(stream, QueryFileMonitor.createChannelComputer(), parallelism);

        // 创建查询执行器操作符
        QueryExecutorOperator executorOperator = new QueryExecutorOperator(table);

        // 将数据流转换为执行器操作符，并设置并行度
        DataStreamSink<?> sink =
                stream.transform(
                                "Executor", // 转换操作符名称
                                InternalTypeInfo.fromRowType(QueryExecutorOperator.outputType()), // 输出类型
                                executorOperator // 操作符
                        )
                        .setParallelism(parallelism) // 设置并行度
                        .addSink(new QueryAddressRegister(table)) // 添加地址注册接收器
                        .setParallelism(1); // 地址注册接收器并行度为 1

        // 设置接收器的最大并行度为 1
        sink.getTransformation().setMaxParallelism(1);
    }
}
