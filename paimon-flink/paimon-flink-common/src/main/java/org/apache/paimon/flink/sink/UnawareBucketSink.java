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

import org.apache.paimon.flink.source.AppendBypassCoordinateOperatorFactory;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * 用于非分区表的 Sink。
 *
 * <p>注意：在非分区模式下，插入数据时不按分区进行洗牌。我们可以将整理操作与插入作业分开执行。
 */
public abstract class UnawareBucketSink<T> extends FlinkWriteSink<T> {

    protected final FileStoreTable table; // 文件存储表对象
    protected final LogSinkFunction logSinkFunction; // 日志写入函数

    @Nullable protected final Integer parallelism; // 并行度，可为空

    public UnawareBucketSink(
            FileStoreTable table,
            @Nullable Map<String, String> overwritePartitions,
            LogSinkFunction logSinkFunction,
            @Nullable Integer parallelism) {
        super(table, overwritePartitions); // 调用父类构造方法
        this.table = table;
        this.logSinkFunction = logSinkFunction;
        this.parallelism = parallelism;
    }

    @Override
    public DataStream<Committable> doWrite(
            DataStream<T> input, String initialCommitUser, @Nullable Integer parallelism) {
        DataStream<Committable> written = super.doWrite(input, initialCommitUser, this.parallelism);

        boolean enableCompaction = !table.coreOptions().writeOnly(); // 是否启用压缩
        boolean isStreamingMode =
                input.getExecutionEnvironment()
                        .getConfiguration()
                        .get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING; // 是否为流处理模式

        // 如果启用压缩且处于流处理模式，则添加压缩拓扑
        if (enableCompaction && isStreamingMode) {
            written =
                    written.transform(
                                    // 添加紧凑协调器操作
                                    "Compact Coordinator: " + table.name(),
                                    new EitherTypeInfo<>(new CommittableTypeInfo(), new CompactionTaskTypeInfo()),
                                    new AppendBypassCoordinateOperatorFactory<>(table))
                            .forceNonParallel() // 强制非并行
                            .transform(
                                    // 添加紧凑工作线程操作
                                    "Compact Worker: " + table.name(),
                                    new CommittableTypeInfo(),
                                    new AppendBypassCompactWorkerOperator(table, initialCommitUser))
                            .setParallelism(written.getParallelism()); // 设置并行度
        }

        return written;
    }
}
