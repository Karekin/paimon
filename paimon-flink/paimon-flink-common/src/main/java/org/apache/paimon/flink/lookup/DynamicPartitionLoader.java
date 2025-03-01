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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_DYNAMIC_PARTITION;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 动态分区加载器，用于文件存储查找表函数。
 * 它实现了动态分区的加载和维护，支持定期刷新分区信息，
 * 并提供了分区信息的更新和获取功能，确保数据查询的准确性。
 */
public class DynamicPartitionLoader implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 用于标记最大分区的字符串标记。
     * 它表示分区的上界值，通常用于数据过滤或分区过滤的条件。
     */
    private static final String MAX_PT = "max_pt()";

    /**
     * 文件存储的表对象，包含了表的元数据和数据路径等信息。
     */
    private final Table table;

    /**
     * 分区刷新的时间间隔，类型为 {@link java.time.Duration}。
     * 该字段用于控制分区信息的刷新频率。
     */
    private final Duration refreshInterval;

    /**
     * 表扫描对象，用于读取表数据。
     */
    private TableScan scan;

    /**
     * 内部行比较器，用于比较分区值的大小。
     * 它基于分区键的类型生成对应的比较逻辑。
     */
    private Comparator<InternalRow> comparator;

    /**
     * 上次分区刷新的时间。
     */
    private LocalDateTime lastRefresh;

    /**
     * 当前分区的信息，以二进制行的形式表示。
     */
    @Nullable private BinaryRow partition;

    /**
     * 构造方法，用于初始化动态分区加载器。
     *
     * @param table          文件存储的表对象。
     * @param refreshInterval 分区刷新的时间间隔。
     */
    private DynamicPartitionLoader(Table table, Duration refreshInterval) {
        this.table = table;
        this.refreshInterval = refreshInterval;
    }

    /**
     * 打开动态分区加载器，初始化表扫描和比较器。
     */
    public void open() {
        /**
         * 创建表扫描对象，用于读取表的分区信息。
         */
        this.scan = table.newReadBuilder().newScan();

        /**
         * 获取分区键对应的行类型。
         */
        RowType partitionType = table.rowType().project(table.partitionKeys());

        /**
         * 创建基于分区键类型的比较器。
         * 用于比较分区值的大小，确定分区的顺序。
         */
        this.comparator = CodeGenUtils.newRecordComparator(partitionType.getFieldTypes());
    }

    /**
     * 将分区键添加到连接键和投影字段中。
     *
     * @param joinKeys       连接键列表。
     * @param projectFields  投影字段列表。
     */
    public void addPartitionKeysTo(List<String> joinKeys, List<String> projectFields) {
        List<String> partitionKeys = table.partitionKeys();

        /**
         * 确保连接键中不包含分区键。
         */
        checkArgument(joinKeys.stream().noneMatch(partitionKeys::contains));

        /**
         * 将分区键添加到连接键列表。
         */
        joinKeys.addAll(partitionKeys);

        /**
         * 将分区键添加到投影字段列表。
         */
        partitionKeys.stream()
                .filter(k -> !projectFields.contains(k))
                .forEach(projectFields::add);
    }

    /**
     * 获取当前分区的信息。
     *
     * @return 当前分区的信息。
     */
    @Nullable
    public BinaryRow partition() {
        return partition;
    }

    /**
     * 检查分区是否需要刷新，并返回是否发生了变化。
     *
     * @return 如果分区发生变化返回 true，否则返回 false。
     */
    public boolean checkRefresh() {
        /**
         * 如果未超过刷新时间间隔，直接返回 false。
         */
        if (lastRefresh != null
                && !lastRefresh.plus(refreshInterval).isBefore(LocalDateTime.now())) {
            return false;
        }

        /**
         * 记录当前分区的旧值。
         */
        BinaryRow previous = this.partition;

        /**
         * 获取最新的分区值。
         */
        this.partition = scan.listPartitions().stream().max(comparator).orElse(null);

        /**
         * 更新上次刷新时间。
         */
        this.lastRefresh = LocalDateTime.now();

        /**
         * 比较新旧分区值是否相同。
         */
        return !Objects.equals(previous, this.partition);
    }

    /**
     * 根据表的选项创建动态分区加载器。
     *
     * @param table 表对象。
     * @return 动态分区加载器。
     */
    @Nullable
    public static DynamicPartitionLoader of(Table table) {
        /**
         * 从表的配置中读取动态分区选项。
         */
        Options options = Options.fromMap(table.options());
        String dynamicPartition = options.get(LOOKUP_DYNAMIC_PARTITION);

        if (dynamicPartition == null) {
            /**
             * 如果没有配置动态分区选项，返回 null。
             */
            return null;
        }

        if (!dynamicPartition.equalsIgnoreCase(MAX_PT)) {
            /**
             * 如果动态分区模式不支持，抛出异常。
             */
            throw new UnsupportedOperationException(
                    "Unsupported dynamic partition pattern: " + dynamicPartition);
        }

        /**
         * 获取动态分区的刷新间隔。
         */
        Duration refresh =
                options.get(FlinkConnectorOptions.LOOKUP_DYNAMIC_PARTITION_REFRESH_INTERVAL);

        /**
         * 创建动态分区加载器。
         */
        return new DynamicPartitionLoader(table, refresh);
    }
}
