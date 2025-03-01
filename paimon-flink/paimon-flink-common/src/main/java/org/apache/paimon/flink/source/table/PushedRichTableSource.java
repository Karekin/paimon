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

package org.apache.paimon.flink.source.table;

import org.apache.paimon.flink.source.FlinkTableSource;

import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.List;

/**
 * 一个具有下推功能的 RichTableSource。
 * 该类继承自 `RichTableSource`，并实现了过滤下推、投影下推和限制下推的功能。
 */
public class PushedRichTableSource extends RichTableSource
        implements SupportsFilterPushDown, SupportsProjectionPushDown, SupportsLimitPushDown {

    private final FlinkTableSource source; // 表源实例，用于实际的数据读取和处理

    public PushedRichTableSource(FlinkTableSource source) {
        super(source);
        this.source = source; // 初始化表源实例
    }

    @Override
    public PushedRichTableSource copy() {
        // 创建一个副本，确保每个实例独立
        return new PushedRichTableSource(source.copy());
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        // 应用过滤下推逻辑
        return Result.of(
                filters, // 剩余的过滤条件
                source.pushFilters(filters) // 下推过滤条件到表源
        );
    }

    @Override
    public void applyLimit(long limit) {
        // 应用限制下推逻辑
        source.pushLimit(limit); // 下推限制到表源
    }

    @Override
    public boolean supportsNestedProjection() {
        // 返回是否支持嵌套投影
        return false; // 不支持嵌套投影
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        // 应用投影下推逻辑
        source.pushProjection(projectedFields); // 下推投影到表源
    }
}
