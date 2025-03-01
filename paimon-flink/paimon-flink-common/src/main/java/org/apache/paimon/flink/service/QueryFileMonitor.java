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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.system.FileMonitorTable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * 文件监测器类，负责以下任务：
 * 1. 从表中读取增量文件。
 * 2. 将文件分配给下游任务进行进一步处理。
 */
public class QueryFileMonitor extends RichSourceFunction<InternalRow> {

    private static final long serialVersionUID = 1L;

    // 当前监测的表
    private final Table table;
    // 文件监测间隔时间
    private final long monitorInterval;

    // 源上下文，用于收集数据
    private transient SourceContext<InternalRow> ctx;
    // 文件扫描对象
    private transient StreamTableScan scan;
    // 文件读取对象
    private transient TableRead read;

    // 是否继续运行的标志
    private volatile boolean isRunning = true;

    /**
     * 构造函数，初始化文件监测器。
     *
     * @param table 目标表
     */
    public QueryFileMonitor(Table table) {
        this.table = table;
        // 获取监测间隔时间，单位为毫秒
        this.monitorInterval =
                Options.fromMap(table.options())
                        .get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL)
                        .toMillis();
    }

    /**
     * 源函数生命周期方法，用于初始化资源。
     *
     * @param parameters 配置参数
     * @throws Exception 初始化异常
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建文件监测表
        FileMonitorTable monitorTable = new FileMonitorTable((FileStoreTable) table);

        // 创建读取构建器
        ReadBuilder readBuilder = monitorTable.newReadBuilder();

        // 初始化流扫描和读取对象
        this.scan = readBuilder.newStreamScan();
        this.read = readBuilder.newRead();
    }

    /**
     * 源函数的核心逻辑，负责从文件中读取数据并发送到下游。
     *
     * @param ctx 源上下文
     * @throws Exception 运行异常
     */
    @Override
    public void run(SourceContext<InternalRow> ctx) throws Exception {
        this.ctx = ctx;

        // 不断循环监测文件
        while (isRunning) {
            boolean isEmpty;
            // 同步收集数据，确保线程安全
            synchronized (ctx.getCheckpointLock()) {
                if (!isRunning) {
                    return;
                }
                // 执行扫描
                isEmpty = doScan();
            }

            // 如果没有读取到数据，等待一段时间
            if (isEmpty) {
                Thread.sleep(monitorInterval);
            }
        }
    }

    /**
     * 执行文件扫描操作。
     *
     * @return 是否没有数据
     * @throws Exception 扫描异常
     */
    private boolean doScan() throws Exception {
        List<InternalRow> records = new ArrayList<>();
        // 读取文件数据
        read.createReader(scan.plan()).forEachRemaining(records::add);

        // 将记录发送到下游
        records.forEach(ctx::collect);

        // 返回是否为空
        return records.isEmpty();
    }

    /**
     * 取消任务，停止循环。
     */
    @Override
    public void cancel() {
        // 如果源上下文已初始化，同步设置停止标志
        if (ctx != null) {
            synchronized (ctx.getCheckpointLock()) {
                isRunning = false;
            }
        } else {
            // 如果源上下文未初始化，直接设置停止标志
            isRunning = false;
        }
    }

    /**
     * 构建文件监测器数据流。
     *
     * @param env 执行环境
     * @param table 目标表
     * @return 文件监测器数据流
     */
    public static DataStream<InternalRow> build(
            StreamExecutionEnvironment env, Table table) {
        return env.addSource(
                new QueryFileMonitor(table), // 创建文件监测器
                "FileMonitor-" + table.name(), // 设置源名称
                InternalTypeInfo.fromRowType(FileMonitorTable.getRowType())); // 设置类型信息
    }

    /**
     * 创建通道计算处理器，用于计算记录的通道。
     *
     * @return 通道计算处理器
     */
    public static ChannelComputer<InternalRow> createChannelComputer() {
        return new FileMonitorChannelComputer();
    }

    /**
     * 文件监测通道计算处理器，用于处理从文件监测表中读取的行。
     */
    private static class FileMonitorChannelComputer implements ChannelComputer<InternalRow> {

        // 通道数目
        private int numChannels;

        /**
         * 初始化通道数目。
         *
         * @param numChannels 通道数目
         */
        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
        }

        /**
         * 计算行所属的通道。
         *
         * @param row 行数据
         * @return 通道编号
         */
        @Override
        public int channel(InternalRow row) {
            // 获取分区和桶编号
            BinaryRow partition = deserializeBinaryRow(row.getBinary(1));
            int bucket = row.getInt(2);

            // 选择通道
            return ChannelComputer.select(partition, bucket, numChannels);
        }

        /**
         * 返回通道计算处理器的字符串表示。
         *
         * @return 字符串表示
         */
        @Override
        public String toString() {
            return "FileMonitorChannelComputer{" + "numChannels=" + numChannels + '}';
        }
    }
}
