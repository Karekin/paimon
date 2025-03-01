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

package org.apache.paimon.flink.query;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.query.QueryLocationImpl;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.service.client.KvQueryClient;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;

/**
 * TableQuery 接口的实现类，用于通过远程服务查询数据。
 */
public class RemoteTableQuery implements TableQuery {

    // 文件存储表
    private final FileStoreTable table;

    // 键值查询客户端
    private final KvQueryClient client;

    // 键序列化器
    private final InternalRowSerializer keySerializer;

    // 值投影（可选）
    @Nullable private int[] projection;

    /**
     * 远程表查询构造函数。
     *
     * @param table 文件存储表
     */
    public RemoteTableQuery(Table table) {
        this.table = (FileStoreTable) table;

        // 创建服务管理器
        ServiceManager manager = this.table.store().newServiceManager();

        // 初始化查询客户端
        this.client = new KvQueryClient(new QueryLocationImpl(manager), 1);

        // 创建键序列化器，基于表的主键
        this.keySerializer =
                InternalSerializers.create(
                        TypeUtils.project(table.rowType(), table.primaryKeys()));
    }

    /**
     * 检查远程服务是否可用。
     *
     * @param table 文件存储表
     * @return 远程服务是否可用
     */
    public static boolean isRemoteServiceAvailable(FileStoreTable table) {
        return table.store().newServiceManager().service(PRIMARY_KEY_LOOKUP).isPresent();
    }

    /**
     * 根据键查询数据。
     *
     * @param partition 分区
     * @param bucket 桶编号
     * @param key 键
     * @return 查询结果
     * @throws IOException 查询异常
     */
    @Nullable
    @Override
    public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException {
        BinaryRow row;

        try {
            // 使用查询客户端获取值
            row =
                    client.getValues(
                                    partition,
                                    bucket,
                                    new BinaryRow[] {keySerializer.toBinaryRow(key)})
                            .get()[0];
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }

        // 如果没有投影，直接返回结果
        if (projection == null) {
            return row;
        }

        // 如果查询结果为空，返回 null
        if (row == null) {
            return null;
        }

        // 返回投影后的结果
        return ProjectedRow.from(projection).replaceRow(row);
    }

    /**
     * 设置值投影（基于顶层字段索引）。
     *
     * @param projection 投影
     * @return RemoteTableQuery 实例
     */
    @Override
    public RemoteTableQuery withValueProjection(int[] projection) {
        return withValueProjection(Projection.of(projection).toNestedIndexes());
    }

    /**
     * 设置值投影（基于嵌套字段索引）。
     *
     * @param projection 投影
     * @return RemoteTableQuery 实例
     */
    @Override
    public RemoteTableQuery withValueProjection(int[][] projection) {
        // 将嵌套投影转换为顶层投影
        this.projection = Projection.of(projection).toTopLevelIndexes();
        return this;
    }

    /**
     * 创建值序列化器。
     *
     * @return 值序列化器
     */
    @Override
    public InternalRowSerializer createValueSerializer() {
        // 根据投影创建序列化器
        return InternalSerializers.create(
                TypeUtils.project(table.rowType(), projection));
    }

    /**
     * 关闭查询客户端。
     *
     * @throws IOException 关闭异常
     */
    @Override
    public void close() throws IOException {
        client.shutdown();
    }

    /**
     * 发起查询取消请求（仅用于测试）。
     *
     * @return 代表取消操作的 CompletableFuture
     */
    @VisibleForTesting
    public CompletableFuture<Void> cancel() {
        // 返回查询客户端的关闭 Future
        return client.shutdownFuture();
    }
}