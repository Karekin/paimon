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

package org.apache.paimon.format;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.util.Arrays;

import static org.apache.paimon.statistics.SimpleColStatsCollector.createFullStatsFactories;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 从一系列记录中提取各个列的统计信息的收集器。
 */
public class SimpleStatsCollector {

    // 将 RowData 转换为对象数组的工具类
    private final RowDataToObjectArrayConverter converter;

    // 各个列的统计信息收集器数组
    private final SimpleColStatsCollector[] statsCollectors;

    // 各个字段的序列化器数组
    private final Serializer<Object>[] fieldSerializers;

    // 标记收集器是否被禁用（当所有统计信息收集器都是 NoneSimpleColStatsCollector 时，认为被禁用）
    private final boolean isDisabled;

    /**
     * 构造一个基于给定 RowType 的 SimpleStatsCollector。
     * 初始化统计信息收集器为完整统计信息工厂。
     *
     * @param rowType RowType 表示行的 Schema
     */
    public SimpleStatsCollector(RowType rowType) {
        this(rowType, createFullStatsFactories(rowType.getFieldCount()));
    }

    /**
     * 构造一个基于给定 RowType 和统计信息收集器工厂数组的 SimpleStatsCollector。
     *
     * @param rowType RowType 表示行的 Schema
     * @param collectorFactory 统计信息收集器工厂数组
     */
    public SimpleStatsCollector(
            RowType rowType, SimpleColStatsCollector.Factory[] collectorFactory) {
        int numFields = rowType.getFieldCount();
        checkArgument(
                numFields == collectorFactory.length,
                "字段数量 %s 应该等于统计信息工厂的数量 %s。",
                numFields,
                collectorFactory.length);
        this.statsCollectors = SimpleColStatsCollector.create(collectorFactory);
        this.converter = new RowDataToObjectArrayConverter(rowType);
        this.fieldSerializers = new Serializer[numFields];
        for (int i = 0; i < numFields; i++) {
            // 根据每个字段的类型创建序列化器
            fieldSerializers[i] = InternalSerializers.create(rowType.getTypeAt(i));
        }
        // 如果所有统计信息收集器都是 NoneSimpleColStatsCollector，标记收集器被禁用
        this.isDisabled =
                Arrays.stream(statsCollectors)
                        .allMatch(p -> p instanceof NoneSimpleColStatsCollector);
    }

    /**
     * 获取收集器是否被禁用。
     * 如果所有统计信息收集器都是 NoneSimpleColStatsCollector，则返回 true。
     *
     * @return 收集器是否被禁用
     */
    public boolean isDisabled() {
        return isDisabled;
    }

    /**
     * 使用一个新的行数据更新统计信息。
     * <p><b>重要</b>：该行的字段不应被复用，因为它们会被直接存储在收集器中。
     *
     * @param row InternalRow 表示行数据
     */
    public void collect(InternalRow row) {
        // 将 InternalRow 转换为对象数组
        Object[] objects = converter.convert(row);
        for (int i = 0; i < row.getFieldCount(); i++) {
            SimpleColStatsCollector collector = statsCollectors[i];
            Object obj = objects[i];
            // 调用收集器的 collect 方法，收集当前字段的值
            collector.collect(obj, fieldSerializers[i]);
        }
    }

    /**
     * 提取统计信息，并返回一个包含各个列统计信息的数组。
     *
     * @return 各个列的统计信息数组
     */
    public SimpleColStats[] extract() {
        SimpleColStats[] stats = new SimpleColStats[this.statsCollectors.length];
        for (int i = 0; i < stats.length; i++) {
            stats[i] = this.statsCollectors[i].result();
        }
        return stats;
    }
}