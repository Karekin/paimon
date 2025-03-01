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

package org.apache.paimon.service.server;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.service.exceptions.UnknownPartitionBucketException;
import org.apache.paimon.service.messages.KvRequest;
import org.apache.paimon.service.messages.KvResponse;
import org.apache.paimon.service.network.AbstractServerHandler;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.ServiceRequestStats;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandler;

import java.util.concurrent.CompletableFuture;

import static org.apache.paimon.table.sink.ChannelComputer.select;

/**
 * 键值服务器请求处理器。
 * 负责分发异步查询任务，并将查询结果写入到网络通道。
 * 网络线程接收消息、反序列化消息并分发查询任务。查询操作在单独的线程中执行，以免阻塞网络线程（如文件 I/O 等）。
 */
@ChannelHandler.Sharable
public class KvServerHandler extends AbstractServerHandler<KvRequest, KvResponse> {

    // 当前服务器的 ID
    private final int serverId;
    // 服务器总数
    private final int numServers;
    // 查询对象，用于执行实际查询
    private final TableQuery lookup;
    // 值序列化器，用于序列化查询结果
    private final InternalRowSerializer valueSerializer;

    /**
     * 创建键值查询服务器处理器。
     *
     * @param server 键值查询服务器
     * @param serverId 当前服务器 ID
     * @param numServers 服务器总数
     * @param lookup 查询对象
     * @param serializer 消息序列化器
     * @param stats 服务器统计信息收集器
     */
    public KvServerHandler(
            final KvQueryServer server,
            final int serverId,
            final int numServers,
            final TableQuery lookup,
            final MessageSerializer<KvRequest, KvResponse> serializer,
            final ServiceRequestStats stats) {
        super(server, serializer, stats);
        this.serverId = serverId;
        this.numServers = numServers;
        this.lookup = Preconditions.checkNotNull(lookup); // 确保查询对象不为空
        this.valueSerializer = lookup.createValueSerializer(); // 创建值序列化器
    }

    /**
     * 处理查询请求。
     *
     * @param requestId 请求 ID
     * @param request 请求对象
     * @return 包含查询结果的异步响应
     */
    @Override
    public CompletableFuture<KvResponse> handleRequest(
            final long requestId, final KvRequest request) {
        final CompletableFuture<KvResponse> responseFuture = new CompletableFuture<>();

        // 计算应由哪个服务器处理当前请求
        int selectServerId = select(request.partition(), request.bucket(), numServers);

        // 判断是否由当前服务器处理
        if (selectServerId != serverId) {
            // 不属于当前服务器，返回异常
            responseFuture.completeExceptionally(
                    new UnknownPartitionBucketException(getServerName()));
            return responseFuture;
        }

        try {
            // 获取请求中的键列表
            BinaryRow[] keys = request.keys();
            // 准备存储查询结果的数组
            BinaryRow[] values = new BinaryRow[keys.length];

            for (int i = 0; i < values.length; i++) {
                // 根据分区、桶和键执行查询
                InternalRow value =
                        this.lookup.lookup(request.partition(), request.bucket(), keys[i]);

                // 如果查询结果不为空，进行序列化
                if (value != null) {
                    values[i] = valueSerializer.toBinaryRow(value).copy();
                }
            }

            // 返回查询结果
            responseFuture.complete(new KvResponse(values));
            return responseFuture;
        } catch (Throwable t) {
            // 捕获异常并记录错误信息
            String errMsg =
                    "处理请求 ID "
                            + requestId
                            + " 时发生错误。原因："
                            + ExceptionUtils.stringifyException(t);
            responseFuture.completeExceptionally(new RuntimeException(errMsg));
            return responseFuture;
        }
    }

    /**
     * 关闭处理器。
     *
     * @return 表示关闭操作完成的异步任务
     */
    @Override
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.completedFuture(null); // 直接返回完成的异步任务
    }
}