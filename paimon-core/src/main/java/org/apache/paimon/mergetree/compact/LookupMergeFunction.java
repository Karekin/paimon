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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * 用于查找的合并函数，此包装器仅考虑最新的高级别记录，
 * 因为每次合并都会查询旧的合并记录，所以最新的高级别记录应该是最终的合并值。
 */
public class LookupMergeFunction implements MergeFunction<KeyValue> {

    private final MergeFunction<KeyValue> mergeFunction; // 被包装的合并函数
    private final LinkedList<KeyValue> candidates = new LinkedList<>(); // 候选记录列表，用于存储待合并的记录
    private final InternalRowSerializer keySerializer; // 键的序列化器
    private final InternalRowSerializer valueSerializer; // 值的序列化器

    /**
     * 构造方法，初始化 LookupMergeFunction。
     * @param mergeFunction 被包装的合并函数
     * @param keyType 键的数据类型
     * @param valueType 值的数据类型
     */
    public LookupMergeFunction(
            MergeFunction<KeyValue> mergeFunction, RowType keyType, RowType valueType) {
        this.mergeFunction = mergeFunction; // 初始化被包装的合并函数
        this.keySerializer = new InternalRowSerializer(keyType); // 初始化键的序列化器
        this.valueSerializer = new InternalRowSerializer(valueType); // 初始化值的序列化器
    }

    @Override
    public void reset() {
        candidates.clear(); // 清空候选记录列表
    }

    @Override
    public void add(KeyValue kv) {
        candidates.add(kv.copy(keySerializer, valueSerializer)); // 将 KeyValue 添加到候选记录列表
    }

    @Override
    public KeyValue getResult() {
        // 1. 找到最新的高级别记录
        Iterator<KeyValue> descending = candidates.descendingIterator(); // 获取从后到前的迭代器
        KeyValue highLevel = null; // 用于存储最新的高级别记录
        while (descending.hasNext()) {
            KeyValue kv = descending.next(); // 获取下一个记录
            if (kv.level() > 0) { // 如果记录的层级大于 0（即高级别记录）
                if (highLevel != null) {
                    descending.remove(); // 移除之前的高级别记录
                } else {
                    highLevel = kv; // 设置为最新的高级别记录
                }
            }
        }

        // 2. 使用被包装的合并函数处理所有输入
        mergeFunction.reset(); // 重置被包装的合并函数
        candidates.forEach(mergeFunction::add); // 将所有候选记录添加到被包装的合并函数中
        return mergeFunction.getResult(); // 返回合并结果
    }

    // 提供候选记录列表的访问方法
    LinkedList<KeyValue> candidates() {
        return candidates;
    }

    /**
     * 包装合并函数工厂。
     * @param wrapped 被包装的合并函数工厂
     * @param keyType 键的数据类型
     * @param valueType 值的数据类型
     * @return 包装后的合并函数工厂
     */
    public static MergeFunctionFactory<KeyValue> wrap(
            MergeFunctionFactory<KeyValue> wrapped, RowType keyType, RowType valueType) {
        if (wrapped.create() instanceof FirstRowMergeFunction) {
            // 如果被包装的合并函数已经是 FirstRowMergeFunction，则不需要包装
            return wrapped;
        }
        return new Factory(wrapped, keyType, valueType); // 返回新的工厂实例
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final MergeFunctionFactory<KeyValue> wrapped; // 被包装的合并函数工厂
        private final RowType keyType; // 键的数据类型
        private final RowType rowType; // 值的数据类型

        /**
         * 构造方法，初始化 Factory。
         * @param wrapped 被包装的合并函数工厂
         * @param keyType 键的数据类型
         * @param rowType 值的数据类型
         */
        private Factory(
                MergeFunctionFactory<KeyValue> wrapped, RowType keyType, RowType rowType) {
            this.wrapped = wrapped;
            this.keyType = keyType;
            this.rowType = rowType;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            // 根据投影调整值的数据类型
            RowType valueType =
                    projection == null ? rowType : Projection.of(projection).project(rowType);
            return new LookupMergeFunction(
                    wrapped.create(projection), keyType, valueType); // 创建新的合并函数实例
        }

        @Override
        public AdjustedProjection adjustProjection(@Nullable int[][] projection) {
            return wrapped.adjustProjection(projection); // 调用被包装的工厂的 adjustProjection 方法
        }
    }
}
