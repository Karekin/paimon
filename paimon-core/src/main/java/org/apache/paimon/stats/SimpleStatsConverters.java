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

package org.apache.paimon.stats;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.paimon.schema.SchemaEvolutionUtil.createIndexCastMapping;

/**
 * Converters to create col stats array serializer.
 * 创建列统计数组序列化的转换器工具类。
 * @author [Original Author]
 * @modified [Your Name]
 * @since [Version]
 */
// 这个类用于根据不同的 schema 转换生成列统计数组序列器
public class SimpleStatsConverters {

    // 用于获取 schema 的字段列表
    private final Function<Long, List<DataField>> schemaFields;

    // 当前表的 schema ID
    private final long tableSchemaId;

    // 当前表的数据字段列表
    private final List<DataField> tableDataFields;

    // 存储表字段的原子引用，用于线程安全操作
    private final AtomicReference<List<DataField>> tableFields;

    // 缓存已创建的 SimpleStatsConverter 对象，避免重复创建
    private final ConcurrentMap<Long, SimpleStatsConverter> converters;

    /**
     * 构造函数，初始化 SimpleStatsConverters 对象。
     *
     * @param schemaFields 获取 schema 字段列表的方法
     * @param tableSchemaId 当前表的 schema ID
     */
    public SimpleStatsConverters(
            Function<Long, List<DataField>> schemaFields, long tableSchemaId) {
        // 初始化成员变量
        this.schemaFields = schemaFields;
        this.tableSchemaId = tableSchemaId;
        // 获取当前表的数据字段列表
        this.tableDataFields = schemaFields.apply(tableSchemaId);
        // 初始化表字段的原子引用
        this.tableFields = new AtomicReference<>();
        // 初始化转换器缓存
        this.converters = new ConcurrentHashMap<>();
    }

    /**
     * 获取或创建指定 schema ID 的 SimpleStatsConverter 对象。
     *
     * @param dataSchemaId 数据 schema ID
     * @return SimpleStatsConverter 对象
     */
    public SimpleStatsConverter getOrCreate(long dataSchemaId) {
        // 使用 ConcurrentMap 的 computeIfAbsent 方法进行双重检查锁定，确保线程安全
        return converters.computeIfAbsent(
                dataSchemaId,
                id -> {
                    if (tableSchemaId == id) {
                        // 如果 schema ID 与当前表一致，直接创建 SimpleStatsConverter
                        return new SimpleStatsConverter(new RowType(schemaFields.apply(id)));
                    }

                    // 获取表字段的原子引用
                    List<DataField> schemaTableFields =
                            tableFields.updateAndGet(v -> v == null ? tableDataFields : v);

                    // 获取目标 schema 的字段列表
                    List<DataField> dataFields = schemaFields.apply(id);

                    // 创建字段索引映射和类型转换映射
                    IndexCastMapping indexCastMapping =
                            createIndexCastMapping(schemaTableFields, dataFields);

                    // 获取字段索引映射
                    @Nullable int[] indexMapping = indexCastMapping.getIndexMapping();

                    // 创建考虑 schema 演化的列统计数组序列器
                    return new SimpleStatsConverter(
                            new RowType(dataFields), // 目标 schema 的字段类型
                            indexMapping,           // 字段索引映射
                            indexCastMapping.getCastMapping()); // 字段类型转换映射
                });
    }

    /**
     * 转换过滤条件，适配不同的 schema。
     *
     * @param dataSchemaId 数据 schema ID
     * @param filter 过滤条件
     * @return 转换后的过滤条件
     */
    public Predicate convertFilter(long dataSchemaId, Predicate filter) {
        if (tableSchemaId == dataSchemaId) {
            // 如果 schema ID 与当前表一致，直接返回原始过滤条件
            return filter;
        }

        // 如果 schema ID 不一致，使用 SchemaEvolutionUtil 工具类转换过滤条件
        return Objects.requireNonNull(
                SchemaEvolutionUtil.createDataFilters(
                        schemaFields.apply(tableSchemaId), // 当前表的字段列表
                        schemaFields.apply(dataSchemaId), // 目标 schema 的字段列表
                        Collections.singletonList(filter) // 原始过滤条件
                )
        ).get(0);
    }

    /**
     * 获取当前表的数据字段列表。
     *
     * @return 数据字段列表
     */
    public List<DataField> tableDataFields() {
        return tableDataFields;
    }
}
