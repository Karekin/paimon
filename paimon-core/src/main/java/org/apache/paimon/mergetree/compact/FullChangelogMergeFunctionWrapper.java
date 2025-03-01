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
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Preconditions;

/**
 * 包装合并函数，以在全量整理期间生成变更日志。
 *
 * <p>此包装器只能在 {@link SortMergeReader} 中使用，因为：
 *
 * <ul>
 *   <li>此包装器不复制 {@link KeyValue}。由于 {@link KeyValue} 会被读取器重复使用，这可能导致其他读取器出现问题。
 *   <li>在 {@link SortMergeReader} 中，具有相同键的 {@link KeyValue} 来自不同的内部读取器，因此与对象重复使用相关的 问题不存在。
 * </ul>
 */
public class FullChangelogMergeFunctionWrapper implements MergeFunctionWrapper<ChangelogResult> {

    private final MergeFunction<KeyValue> mergeFunction; // 被包装的合并函数
    private final int maxLevel; // 最大层级
    private final RecordEqualiser valueEqualiser; // 值比较器，用于比较两个值是否相等
    private final boolean changelogRowDeduplicate; // 是否对变更行进行去重

    // 仅全量整理会将文件写入最大层级，参见 UniversalCompaction 类
    private KeyValue topLevelKv; // 顶层的 KeyValue
    private KeyValue initialKv; // 初始的 KeyValue
    private boolean isInitialized; // 是否已初始化

    private final ChangelogResult reusedResult = new ChangelogResult(); // 重用的变更结果对象
    private final KeyValue reusedBefore = new KeyValue(); // 重用的更新前的 KeyValue
    private final KeyValue reusedAfter = new KeyValue(); // 重用的更新后的 KeyValue

    public FullChangelogMergeFunctionWrapper(
            MergeFunction<KeyValue> mergeFunction,
            int maxLevel,
            RecordEqualiser valueEqualiser,
            boolean changelogRowDeduplicate) {
        this.mergeFunction = mergeFunction;
        this.maxLevel = maxLevel;
        this.valueEqualiser = valueEqualiser;
        this.changelogRowDeduplicate = changelogRowDeduplicate;
    }

    /**
     * 重置合并函数和相关状态。
     */
    @Override
    public void reset() {
        mergeFunction.reset(); // 重置被包装的合并函数

        topLevelKv = null; // 重置顶层的 KeyValue
        initialKv = null; // 重置初始的 KeyValue
        isInitialized = false; // 标记未初始化
    }

    /**
     * 添加 KeyValue 到合并函数。
     */
    @Override
    public void add(KeyValue kv) {
        if (maxLevel == kv.level()) {
            // 如果 KeyValue 的层级等于最大层级，且顶层的 KeyValue 已存在，抛出异常
            Preconditions.checkState(
                    topLevelKv == null, "Top level key-value already exists! This is unexpected.");
            topLevelKv = kv; // 设置顶层的 KeyValue
        }

        if (initialKv == null) {
            initialKv = kv; // 设置初始的 KeyValue
        } else {
            if (!isInitialized) {
                merge(initialKv); // 合并初始的 KeyValue
                isInitialized = true; // 标记已初始化
            }
            merge(kv); // 合并当前的 KeyValue
        }
    }

    /**
     * 合并 KeyValue。
     */
    private void merge(KeyValue kv) {
        mergeFunction.add(kv); // 调用被包装的合并函数的 add 方法
    }

    /**
     * 获取合并后的结果。
     */
    @Override
    public ChangelogResult getResult() {
        reusedResult.reset(); // 重置变更结果对象

        if (isInitialized) {
            // 如果已初始化，获取合并后的 KeyValue
            KeyValue merged = mergeFunction.getResult();
            if (topLevelKv == null) {
                // 如果顶层的 KeyValue 为空，且合并后的 KeyValue 是添加操作
                if (merged.isAdd()) {
                    reusedResult.addChangelog(
                            replace(reusedAfter, RowKind.INSERT, merged)); // 添加插入操作
                }
            } else {
                // 如果顶层的 KeyValue 不为空
                if (!merged.isAdd()) {
                    // 如果合并后的 KeyValue 不是添加操作，添加删除操作
                    reusedResult.addChangelog(
                            replace(reusedBefore, RowKind.DELETE, topLevelKv));
                } else if (!changelogRowDeduplicate
                        || !valueEqualiser.equals(topLevelKv.value(), merged.value())) {
                    // 如果未启用去重，或合并后的值与顶层的值不同
                    reusedResult.addChangelog(
                                    replace(reusedBefore, RowKind.UPDATE_BEFORE, topLevelKv)) // 添加更新前的操作
                            .addChangelog(
                                    replace(reusedAfter, RowKind.UPDATE_AFTER, merged)); // 添加更新后的操作
                }
            }
            // 返回变更结果对象，并设置合并后的结果
            return reusedResult.setResultIfNotRetract(merged);
        } else {
            // 如果未初始化
            if (topLevelKv == null && initialKv.isAdd()) {
                // 如果顶层的 KeyValue 为空，且初始的 KeyValue 是添加操作
                reusedResult.addChangelog(
                        replace(reusedAfter, RowKind.INSERT, initialKv)); // 添加插入操作
            }
            // 如果顶层的 KeyValue 不为空，但只有一个 KeyValue，或初始的 KeyValue 不是添加操作
            return reusedResult.setResultIfNotRetract(initialKv);
        }
    }

    /**
     * 替换 KeyValue 的内容。
     */
    private KeyValue replace(KeyValue reused, RowKind valueKind, KeyValue from) {
        return reused.replace( // 使用重用的 KeyValue 对象，替换内容
                from.key(), // 键
                from.sequenceNumber(), // 序列号
                valueKind, // 值类型
                from.value() // 值
        );
    }
}
