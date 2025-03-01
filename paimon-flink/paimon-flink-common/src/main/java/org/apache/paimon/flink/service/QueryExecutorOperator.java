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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.service.network.NetworkUtils;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.service.server.KvQueryServer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * 查询执行器操作符类。
 * 用于执行查询操作，并与查询服务器进行通信。
 */
public class QueryExecutorOperator extends AbstractStreamOperator<InternalRow>
        implements OneInputStreamOperator<InternalRow, InternalRow> {

    private static final long serialVersionUID = 1L;

    // 当前处理的表
    private final Table table;

    // 本地表查询对象
    private transient LocalTableQuery query;

    // 输入输出管理器
    private transient IOManager ioManager;

    /**
     * 构造函数，初始化查询执行器操作符。
     *
     * @param table 目标表
     */
    public QueryExecutorOperator(Table table) {
        this.table = table;
    }

    /**
     * 定义查询执行器操作符的输出类型。
     *
     * @return 输出类型
     */
    public static RowType outputType() {
        // 输出类型为包含四个字段的行类型
        return RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.INT());
    }

    /**
     * 初始化算子状态。
     *
     * @param context 状态初始化上下文
     * @throws Exception 初始化异常
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        // 创建输入输出管理器
        this.ioManager =
                IOManager.create(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        // 创建本地表查询对象
        this.query = ((FileStoreTable) table).newLocalTableQuery().withIOManager(ioManager);
        // 创建键值查询服务器
        KvQueryServer server =
                new KvQueryServer(
                        getRuntimeContext().getIndexOfThisSubtask(), // 当前子任务索引
                        getRuntimeContext().getNumberOfParallelSubtasks(), // 并行度
                        NetworkUtils.findHostAddress(), // 主机地址
                        Collections.singletonList(0).iterator(), // 端口号
                        1, // 线程数
                        1, // 扩展因子
                        query, // 查询对象
                        new DisabledServiceRequestStats()); // 服务请求统计

        try {
            // 启动查询服务器
            server.start();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        // 获取服务器地址
        InetSocketAddress address = server.getServerAddress();

        // 发送服务器地址和端口到下游
        this.output.collect(
                new StreamRecord<>(
                        GenericRow.of(
                                getRuntimeContext().getNumberOfParallelSubtasks(), // 并行度
                                getRuntimeContext().getIndexOfThisSubtask(), // 子任务索引
                                BinaryString.fromString(address.getHostName()), // 主机名
                                address.getPort()))); // 端口号
    }

    /**
     * 处理输入数据。
     *
     * @param streamRecord 输入数据记录
     * @throws Exception 处理异常
     */
    @Override
    public void processElement(StreamRecord<InternalRow> streamRecord) throws Exception {
        InternalRow row = streamRecord.getValue();
        // 获取分区信息
        BinaryRow partition = deserializeBinaryRow(row.getBinary(1));

        // 获取桶编号
        int bucket = row.getInt(2);

        // 反序列化变更前和数据文件
        DataFileMetaSerializer fileMetaSerializer = new DataFileMetaSerializer();
        List<DataFileMeta> beforeFiles = fileMetaSerializer.deserializeList(row.getBinary(3));
        List<DataFileMeta> dataFiles = fileMetaSerializer.deserializeList(row.getBinary(4));

        // 刷新查询文件
        query.refreshFiles(partition, bucket, beforeFiles, dataFiles);
    }

    /**
     * 关闭算子，释放资源。
     *
     * @throws Exception 关闭异常
     */
    @Override
    public void close() throws Exception {
        super.close();
        // 关闭查询对象
        if (query != null) {
            query.close();
        }
        // 关闭输入输出管理器
        if (ioManager != null) {
            ioManager.close();
        }
    }
}
