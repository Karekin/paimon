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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link StoreSinkWrite} 的状态管理类。
 *
 * <p>状态数据首先按照表名（table name）分类，然后按照键名（key name）存储。
 * 该类应在 Sink 运算符（Operator）中初始化，并传递给 {@link StoreSinkWrite} 用于管理写入状态。
 */
public class StoreSinkWriteState {

    /** 用于筛选状态值的过滤器 */
    private final StateValueFilter stateValueFilter;

    /** Flink 的 ListState 存储状态，包含表名、键名、分区信息、桶编号和状态值 */
    private final ListState<Tuple5<String, String, byte[], Integer, byte[]>> listState;

    /** 内存中的状态映射表，存储格式为：表名 -> (键名 -> 状态值列表) */
    private final Map<String, Map<String, List<StateValue>>> map;

    /**
     * 构造方法，初始化状态存储和状态映射表。
     *
     * @param context        Flink 的状态初始化上下文
     * @param stateValueFilter 状态值过滤器
     * @throws Exception 如果状态初始化过程中出现错误
     */
    @SuppressWarnings("unchecked")
    public StoreSinkWriteState(
            StateInitializationContext context, StateValueFilter stateValueFilter)
            throws Exception {
        this.stateValueFilter = stateValueFilter;

        // 定义 ListState 的序列化器，用于存储表名、键名、分区数据、桶编号和状态值
        TupleSerializer<Tuple5<String, String, byte[], Integer, byte[]>> listStateSerializer =
                new TupleSerializer<>(
                        (Class<Tuple5<String, String, byte[], Integer, byte[]>>)
                                (Class<?>) Tuple5.class,
                        new TypeSerializer[]{
                                StringSerializer.INSTANCE, // 表名
                                StringSerializer.INSTANCE, // 键名
                                BytePrimitiveArraySerializer.INSTANCE, // 分区数据（序列化后的 BinaryRow）
                                IntSerializer.INSTANCE, // 桶编号
                                BytePrimitiveArraySerializer.INSTANCE // 状态值
                        });

        // 从 Flink 的 OperatorStateStore 获取 UnionListState
        listState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        "paimon_store_sink_write_state", listStateSerializer));

        // 初始化状态映射表 map，并从 ListState 加载数据
        map = new HashMap<>();
        for (Tuple5<String, String, byte[], Integer, byte[]> tuple : listState.get()) {
            BinaryRow partition = SerializationUtils.deserializeBinaryRow(tuple.f2);
            if (stateValueFilter.filter(tuple.f0, partition, tuple.f3)) {
                map.computeIfAbsent(tuple.f0, k -> new HashMap<>())
                        .computeIfAbsent(tuple.f1, k -> new ArrayList<>())
                        .add(new StateValue(partition, tuple.f3, tuple.f4));
            }
        }
    }

    /**
     * 获取状态值过滤器。
     *
     * @return 过滤器对象
     */
    public StateValueFilter stateValueFilter() {
        return stateValueFilter;
    }

    /**
     * 根据表名和键名获取状态值列表。
     *
     * @param tableName 表名
     * @param key       键名
     * @return 该键下的状态值列表，可能为空
     */
    public @Nullable List<StateValue> get(String tableName, String key) {
        Map<String, List<StateValue>> innerMap = map.get(tableName);
        return innerMap == null ? null : innerMap.get(key);
    }

    /**
     * 在内存状态表中存储新的状态值列表。
     *
     * @param tableName   表名
     * @param key         键名
     * @param stateValues 状态值列表
     */
    public void put(String tableName, String key, List<StateValue> stateValues) {
        map.computeIfAbsent(tableName, k -> new HashMap<>()).put(key, stateValues);
    }

    /**
     * 将内存中的状态数据快照到 Flink 的 ListState。
     *
     * @throws Exception 如果存储状态过程中发生错误
     */
    public void snapshotState() throws Exception {
        List<Tuple5<String, String, byte[], Integer, byte[]>> list = new ArrayList<>();
        for (Map.Entry<String, Map<String, List<StateValue>>> tables : map.entrySet()) {
            for (Map.Entry<String, List<StateValue>> entry : tables.getValue().entrySet()) {
                for (StateValue stateValue : entry.getValue()) {
                    list.add(
                            Tuple5.of(
                                    tables.getKey(),
                                    entry.getKey(),
                                    SerializationUtils.serializeBinaryRow(stateValue.partition()),
                                    stateValue.bucket(),
                                    stateValue.value()));
                }
            }
        }
        listState.update(list);
    }

    /**
     * {@link StoreSinkWrite} 的状态值封装类。
     *
     * <p>所有状态值必须包含分区（partition）和桶（bucket）信息，以便当 Sink 的并行度改变时能够重新分配数据。
     */
    public static class StateValue {

        /** 分区信息（序列化后的 BinaryRow） */
        private final BinaryRow partition;

        /** 数据所在的桶编号 */
        private final int bucket;

        /** 存储的状态值数据 */
        private final byte[] value;

        /**
         * 构造方法，初始化状态值。
         *
         * @param partition 分区数据
         * @param bucket    数据桶编号
         * @param value     具体存储的状态值数据
         */
        public StateValue(BinaryRow partition, int bucket, byte[] value) {
            this.partition = partition;
            this.bucket = bucket;
            this.value = value;
        }

        /**
         * 获取分区信息。
         *
         * @return 分区数据
         */
        public BinaryRow partition() {
            return partition;
        }

        /**
         * 获取桶编号。
         *
         * @return 桶编号
         */
        public int bucket() {
            return bucket;
        }

        /**
         * 获取状态值数据。
         *
         * @return 状态值数据
         */
        public byte[] value() {
            return value;
        }
    }

    /**
     * 用于筛选 {@link StateValue} 的接口。
     *
     * <p>该接口可根据表名、分区和桶编号，决定是否保留该状态值。
     */
    public interface StateValueFilter {

        /**
         * 过滤状态值，决定是否保留该状态值。
         *
         * @param tableName 表名
         * @param partition 分区信息
         * @param bucket    数据桶编号
         * @return 是否保留该状态值，true 表示保留，false 表示丢弃
         */
        boolean filter(String tableName, BinaryRow partition, int bucket);
    }
}
