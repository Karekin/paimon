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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.service.QueryService;
import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/**
 * 查询服务过程。使用方法如下：
 * CALL sys.query_service('tableId', 'parallelism')
 */
public class QueryServiceProcedure extends ProcedureBase {

    // 定义查询服务标识符常量
    public static final String IDENTIFIER = "query_service";

    /**
     * 重写父类方法，返回当前过程的标识符。
     * @return 标识符字符串
     */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * 查询服务过程的主要执行方法。
     * @param procedureContext 过程上下文对象
     * @param tableId 表标识符
     * @param parallelism 并行度
     * @return 执行结果字符串数组
     * @throws Exception 可能抛出的异常
     */
    @ProcedureHint(
            argument = {
                    @ArgumentHint(name = "table", type = @DataTypeHint("STRING")), // 表名称
                    @ArgumentHint(name = "parallelism", type = @DataTypeHint("INT")) // 并行度
            })
    public String[] call(ProcedureContext procedureContext, String tableId, int parallelism)
            throws Exception {

        // 从目录中获取表对象
        Table table = catalog.getTable(Identifier.fromString(tableId));

        // 获取执行环境
        StreamExecutionEnvironment env = procedureContext.getExecutionEnvironment();

        // 构建查询服务
        QueryService.build(env, table, parallelism);

        // 执行查询服务，并返回结果
        return execute(env, IDENTIFIER);
    }
}
