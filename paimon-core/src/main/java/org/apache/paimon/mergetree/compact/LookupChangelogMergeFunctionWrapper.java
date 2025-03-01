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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.LookupLevels.PositionedKeyValue;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 包装合并函数，用于在涉及层级 0 文件的压缩过程中生成变更日志。
 *
 * <p>变更日志记录是在层级 0 文件参与压缩的过程中生成的，如果在压缩过程中满足以下条件：
 *
 * <ul>
 *   <li>没有层级 0 的记录，不生成变更日志。
 *   <li>有层级 0 的记录，以及层级 x（x > 0）的记录，层级 x 的记录应在变更日志中排在层级 0 之前。
 *   <li>有层级 0 的记录，但没有层级 x 的记录，则需要查询上层的历史值作为变更日志的之前记录（BEFORE）。
 * </ul>
 */
public class LookupChangelogMergeFunctionWrapper<T>
        implements MergeFunctionWrapper<ChangelogResult> {

    private final LookupMergeFunction mergeFunction;
    private final MergeFunction<KeyValue> mergeFunction2;
    private final Function<InternalRow, T> lookup;

    private final ChangelogResult reusedResult = new ChangelogResult();
    private final KeyValue reusedBefore = new KeyValue();
    private final KeyValue reusedAfter = new KeyValue();
    private final RecordEqualiser valueEqualiser;
    private final boolean changelogRowDeduplicate;
    private final LookupStrategy lookupStrategy;
    private final @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer;
    private final Comparator<KeyValue> comparator;

    /**
     * 构造函数，初始化 LookupChangelogMergeFunctionWrapper。
     * @param mergeFunctionFactory 合并函数工厂
     * @param lookup 查询函数
     * @param valueEqualiser 值比较器
     * @param changelogRowDeduplicate 是否去重变更日志
     * @param lookupStrategy 查询策略
     * @param deletionVectorsMaintainer 删除向量维护器
     * @param userDefinedSeqComparator 用户自定义比较器
     */
    public LookupChangelogMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory,
            Function<InternalRow, T> lookup,
            RecordEqualiser valueEqualiser,
            boolean changelogRowDeduplicate,
            LookupStrategy lookupStrategy,
            @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer,
            @Nullable UserDefinedSeqComparator userDefinedSeqComparator) {
        // 创建合并函数
        MergeFunction<KeyValue> mergeFunction = mergeFunctionFactory.create();

        // 确保合并函数是 LookupMergeFunction 类型
        checkArgument(
                mergeFunction instanceof LookupMergeFunction,
                "合并函数应是 LookupMergeFunction 类型，但实际是 %s，这是一个错误。",
                mergeFunction.getClass().getName());

        // 如果使用删除向量策略，确保删除向量维护器不为空
        if (lookupStrategy.deletionVector) {
            checkArgument(
                    deletionVectorsMaintainer != null,
                    "deletionVectorsMaintainer 不应为空，这是一个错误。");
        }

        this.mergeFunction = (LookupMergeFunction) mergeFunction;
        this.mergeFunction2 = mergeFunctionFactory.create();
        this.lookup = lookup;
        this.valueEqualiser = valueEqualiser;
        this.changelogRowDeduplicate = changelogRowDeduplicate;
        this.lookupStrategy = lookupStrategy;
        this.deletionVectorsMaintainer = deletionVectorsMaintainer;
        this.comparator = createSequenceComparator(userDefinedSeqComparator);
    }

    /** 重置合并函数。 */
    @Override
    public void reset() {
        mergeFunction.reset();
    }

    /** 添加键值对到合并函数中。 */
    @Override
    public void add(KeyValue kv) {
        mergeFunction.add(kv);
    }

    /** 获取合并结果和变更日志。 */
    @Override
    public ChangelogResult getResult() {
        // 1. 计算最高层级记录和候选记录中是否包含层级 0 记录
        LinkedList<KeyValue> candidates = mergeFunction.candidates();
        Iterator<KeyValue> descending = candidates.descendingIterator();
        KeyValue highLevel = null;
        boolean containLevel0 = false;

        // 遍历候选记录，提取最高层级记录并移除
        while (descending.hasNext()) {
            KeyValue kv = descending.next();
            if (kv.level() > 0) { // 层级大于 0 的记录
                descending.remove();
                if (highLevel == null) {
                    highLevel = kv;
                }
            } else {
                containLevel0 = true; // 存在层级 0 的记录
            }
        }

        // 2. 如果最高层级记录不存在，则查询历史值
        if (highLevel == null) {
            InternalRow lookupKey = candidates.get(0).key();
            T lookupResult = lookup.apply(lookupKey); // 查询历史值
            if (lookupResult != null) {
                if (lookupStrategy.deletionVector) { // 使用删除向量策略
                    PositionedKeyValue positionedKeyValue = (PositionedKeyValue) lookupResult;
                    highLevel = positionedKeyValue.keyValue();
                    deletionVectorsMaintainer.notifyNewDeletion(
                            positionedKeyValue.fileName(), positionedKeyValue.rowPosition());
                } else {
                    highLevel = (KeyValue) lookupResult;
                }
            }
        }

        // 3. 计算合并结果
        KeyValue result = calculateResult(candidates, highLevel);

        // 4. 生成变更日志（当存在层级 0 的记录时）
        reusedResult.reset();
        if (containLevel0 && lookupStrategy.produceChangelog) {
            setChangelog(highLevel, result);
        }

        return reusedResult.setResult(result); // 设置合并结果并返回
    }

    /**
     * 计算合并结果。
     * @param candidates 候选记录
     * @param highLevel 最高层级记录
     * @return 合并结果
     */
    private KeyValue calculateResult(List<KeyValue> candidates, @Nullable KeyValue highLevel) {
        mergeFunction2.reset();
        for (KeyValue candidate : candidates) {
            if (highLevel != null && comparator.compare(highLevel, candidate) < 0) {
                // 如果最高层级记录的顺序号大于当前候选记录
                mergeFunction2.add(highLevel);
                mergeFunction2.add(candidate);
                highLevel = null;
            } else {
                mergeFunction2.add(candidate);
            }
        }
        if (highLevel != null) {
            mergeFunction2.add(highLevel);
        }
        return mergeFunction2.getResult();
    }

    /**
     * 设置变更日志。
     * @param before 变更前的记录
     * @param after 变更后的记录
     */
    private void setChangelog(@Nullable KeyValue before, KeyValue after) {
        if (before == null || !before.isAdd()) { // 如果没有前记录或前记录不是新增
            if (after.isAdd()) { // 变更后是新增
                reusedResult.addChangelog(replaceAfter(RowKind.INSERT, after)); // 添加插入日志
            }
        } else {
            if (!after.isAdd()) { // 如果变更后不是新增
                reusedResult.addChangelog(replaceBefore(RowKind.DELETE, before)); // 添加删除日志
            } else if (!changelogRowDeduplicate || !valueEqualiser.equals(before.value(), after.value())) {
                // 如果不去重或前后值不同
                reusedResult
                        .addChangelog(replaceBefore(RowKind.UPDATE_BEFORE, before)) // 添加更新前日志
                        .addChangelog(replaceAfter(RowKind.UPDATE_AFTER, after)); // 添加更新后日志
            }
        }
    }

    /** 替换前记录。 */
    private KeyValue replaceBefore(RowKind valueKind, KeyValue from) {
        return replace(reusedBefore, valueKind, from);
    }

    /** 替换后记录。 */
    private KeyValue replaceAfter(RowKind valueKind, KeyValue from) {
        return replace(reusedAfter, valueKind, from);
    }

    /**
     * 替换键值对。
     * @param reused 重用的键值对对象
     * @param valueKind 值类型
     * @param from 原始键值对
     * @return 替换后的键值对
     */
    private KeyValue replace(KeyValue reused, RowKind valueKind, KeyValue from) {
        return reused.replace(from.key(), from.sequenceNumber(), valueKind, from.value());
    }

    /**
     * 创建序列比较器。
     * @param userDefinedSeqComparator 用户自定义比较器
     * @return 键值对比较器
     */
    private Comparator<KeyValue> createSequenceComparator(
            @Nullable FieldsComparator userDefinedSeqComparator) {
        if (userDefinedSeqComparator == null) {
            return Comparator.comparingLong(KeyValue::sequenceNumber); // 使用序列号比较
        }

        return (o1, o2) -> {
            int result = userDefinedSeqComparator.compare(o1.value(), o2.value());
            if (result != 0) {
                return result;
            }
            return Long.compare(o1.sequenceNumber(), o2.sequenceNumber());
        };
    }
}
