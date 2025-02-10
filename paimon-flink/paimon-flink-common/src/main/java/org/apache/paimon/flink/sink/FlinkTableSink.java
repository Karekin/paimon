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

import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.abilities.SupportsTruncate;
import org.apache.flink.table.factories.DynamicTableFactory;

import javax.annotation.Nullable;

/**
 * 自定义 Flink Table Sink，实现数据表的写入能力，并支持行级操作和表截断功能。
 */
public class FlinkTableSink extends SupportsRowLevelOperationFlinkTableSink
        implements SupportsTruncate {

    /**
     * 构造方法，初始化 FlinkTableSink。
     *
     * @param tableIdentifier      表的唯一标识符（包括数据库和表名）
     * @param table                需要写入的目标表
     * @param context              动态表工厂的上下文信息，包含配置信息
     * @param logStoreTableFactory （可选）日志存储表工厂，用于增量数据存储
     */
    public FlinkTableSink(
            ObjectIdentifier tableIdentifier,
            Table table,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        // 调用父类构造方法，初始化表标识、表对象、上下文和日志存储表工厂
        super(tableIdentifier, table, context, logStoreTableFactory);
    }

    /**
     * 执行表截断操作，清空表中的所有数据。
     * 该方法使用表的批量提交功能（BatchTableCommit）来执行表截断。
     * 如果执行过程中发生异常，则抛出运行时异常。
     */
    @Override
    public void executeTruncation() {
        try (BatchTableCommit batchTableCommit = table.newBatchWriteBuilder().newCommit()) {
            // 调用 BatchTableCommit 的 truncateTable 方法，清空表数据
            batchTableCommit.truncateTable();
        } catch (Exception e) {
            // 捕获异常并包装为运行时异常抛出
            throw new RuntimeException(e);
        }
    }
}

