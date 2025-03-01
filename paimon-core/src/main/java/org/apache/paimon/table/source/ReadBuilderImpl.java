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

package org.apache.paimon.table.source;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.TypeUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * ReadBuilder 的实现类。
 * 该类用于构建表的读取操作，包括设置过滤条件、投影、限制等。
 */
public class ReadBuilderImpl implements ReadBuilder {

    private static final long serialVersionUID = 1L; // 序列化版本号

    private final InnerTable table; // 表实例，用于实际的数据操作

    private Predicate filter; // 过滤条件
    private int[][] projection; // 投影信息，指定要读取的列

    private Integer limit = null; // 限制结果集的大小

    private Integer shardIndexOfThisSubtask; // 当前子任务的索引
    private Integer shardNumberOfParallelSubtasks; // 并行子任务的总数

    private Map<String, String> partitionSpec; // 分区过滤条件

    private Filter<Integer> bucketFilter; // 桶过滤条件

    public ReadBuilderImpl(InnerTable table) {
        this.table = table; // 初始化表实例
    }

    @Override
    public String tableName() {
        return table.name(); // 返回表的名称
    }

    @Override
    public RowType readType() {
        if (projection == null) {
            return table.rowType(); // 如果没有投影，返回表的完整行类型
        }
        // 应用投影，返回指定列的行类型
        return TypeUtils.project(
                table.rowType(), Projection.of(projection).toTopLevelIndexes());
    }

    @Override
    public ReadBuilder withFilter(Predicate filter) {
        if (this.filter == null) {
            this.filter = filter; // 设置过滤条件
        } else {
            // 将新过滤条件与现有条件组合
            this.filter = PredicateBuilder.and(this.filter, filter);
        }
        return this; // 返回自身，支持链式调用
    }

    @Override
    public ReadBuilder withPartitionFilter(Map<String, String> partitionSpec) {
        this.partitionSpec = partitionSpec; // 设置分区过滤条件
        return this; // 返回自身，支持链式调用
    }

    @Override
    public ReadBuilder withProjection(int[][] projection) {
        this.projection = projection; // 设置投影信息
        return this; // 返回自身，支持链式调用
    }

    @Override
    public ReadBuilder withLimit(int limit) {
        this.limit = limit; // 设置限制结果集的大小
        return this; // 返回自身，支持链式调用
    }

    @Override
    public ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        this.shardIndexOfThisSubtask = indexOfThisSubtask; // 设置当前子任务的索引
        this.shardNumberOfParallelSubtasks = numberOfParallelSubtasks; // 设置并行子任务的总数
        return this; // 返回自身，支持链式调用
    }

    @Override
    public ReadBuilder withBucketFilter(Filter<Integer> bucketFilter) {
        this.bucketFilter = bucketFilter; // 设置桶过滤条件
        return this; // 返回自身，支持链式调用
    }

    @Override
    public TableScan newScan() {
        // 创建并配置表扫描实例
        InnerTableScan tableScan = configureScan(table.newScan());
        if (limit != null) {
            tableScan.withLimit(limit); // 应用限制条件
        }
        return tableScan; // 返回配置好的表扫描实例
    }

    @Override
    public StreamTableScan newStreamScan() {
        // 创建并配置流式表扫描实例
        return (StreamTableScan) configureScan(table.newStreamScan());
    }

    private InnerTableScan configureScan(InnerTableScan scan) {
        // 配置表扫描实例
        scan.withFilter(filter) // 应用过滤条件
                .withPartitionFilter(partitionSpec); // 应用分区过滤条件

        // 检查桶过滤条件和分片配置是否冲突
        checkState(
                bucketFilter == null || shardIndexOfThisSubtask == null,
                "Bucket filter and shard configuration cannot be used together. "
                        + "Please choose one method to specify the data subset.");

        // 应用分片配置
        if (shardIndexOfThisSubtask != null) {
            if (scan instanceof DataTableScan) {
                // 如果是 DataTableScan 类型，应用分片配置
                return ((DataTableScan) scan)
                        .withShard(shardIndexOfThisSubtask, shardNumberOfParallelSubtasks);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported table scan type for shard configuring, the scan is: " + scan);
            }
        }

        // 应用桶过滤条件
        if (bucketFilter != null) {
            scan.withBucketFilter(bucketFilter);
        }

        return scan; // 返回配置好的表扫描实例
    }

    @Override
    public TableRead newRead() {
        // 创建并配置表读取实例
        InnerTableRead read = table.newRead().withFilter(filter); // 应用过滤条件

        // 应用投影
        if (projection != null) {
            read.withProjection(projection);
        }

        return read; // 返回配置好的表读取实例
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true; // 如果是同一个对象，返回 true
        }
        if (o == null || getClass() != o.getClass()) {
            return false; // 如果是 null 或类型不匹配，返回 false
        }
        ReadBuilderImpl that = (ReadBuilderImpl) o;

        // 比较表名称、过滤条件和投影信息
        return Objects.equals(table.name(), that.table.name())
                && Objects.equals(filter, that.filter)
                && Arrays.deepEquals(projection, that.projection);
    }

    @Override
    public int hashCode() {
        // 计算对象的哈希码
        int result = Objects.hash(table.name(), filter);
        result = 31 * result + Arrays.deepHashCode(projection);
        return result;
    }
}
