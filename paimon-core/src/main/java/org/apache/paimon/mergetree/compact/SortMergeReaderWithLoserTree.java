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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * 使用败者树实现的 {@link SortMergeReader}。
 *
 * <p>该类通过败者树（LoserTree）实现多路归并排序，适合高效合并多个排序流。
 *
 * @param <T> 合并结果的类型。
 */
public class SortMergeReaderWithLoserTree<T> implements SortMergeReader<T> {

    // 合并函数包装器，用于处理归并时的合并逻辑
    private final MergeFunctionWrapper<T> mergeFunctionWrapper;

    // 败者树，用于实现多路归并
    private final LoserTree<KeyValue> loserTree;

    /**
     * 构造函数。
     *
     * @param readers 需要归并的多个记录读取器列表。
     * @param userKeyComparator 用户自定义的主键比较器，用于比较 KeyValue 的键。
     * @param userDefinedSeqComparator 用户自定义的序列号比较器（可选），用于比较序列号。
     * @param mergeFunctionWrapper 合并函数包装器，定义了记录的合并逻辑。
     */
    public SortMergeReaderWithLoserTree(
            List<RecordReader<KeyValue>> readers,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper) {
        this.mergeFunctionWrapper = mergeFunctionWrapper;
        this.loserTree =
                new LoserTree<>(
                        readers,
                        (e1, e2) -> userKeyComparator.compare(e2.key(), e1.key()), // 键的比较器，逆序比较
                        createSequenceComparator(userDefinedSeqComparator)); // 创建序列号比较器
    }

    /**
     * 创建序列号比较器。
     *
     * @param userDefinedSeqComparator 用户自定义的序列号比较器（可选）。
     * @return 序列号比较器。
     */
    private Comparator<KeyValue> createSequenceComparator(
            @Nullable FieldsComparator userDefinedSeqComparator) {
        if (userDefinedSeqComparator == null) {
            // 如果没有用户自定义比较器，默认按序列号逆序比较
            return (e1, e2) -> Long.compare(e2.sequenceNumber(), e1.sequenceNumber());
        }

        // 用户自定义比较器：先按值比较，如果相等再按序列号逆序比较
        return (o1, o2) -> {
            int result = userDefinedSeqComparator.compare(o2.value(), o1.value());
            if (result != 0) {
                return result;
            }
            return Long.compare(o2.sequenceNumber(), o1.sequenceNumber());
        };
    }

    /**
     * 读取一个批次的合并结果。
     *
     * <p>与堆排序不同，{@link LoserTree} 仅产生一个批次。
     *
     * @return 批次的记录迭代器，如果没有更多记录，返回 null。
     * @throws IOException 如果读取数据失败。
     */
    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        loserTree.initializeIfNeeded(); // 初始化败者树（如果尚未初始化）
        return loserTree.peekWinner() == null ? null : new SortMergeIterator();
    }

    /**
     * 关闭资源。
     *
     * @throws IOException 如果关闭失败。
     */
    @Override
    public void close() throws IOException {
        loserTree.close();
    }

    /**
     * {@link SortMergeReaderWithLoserTree} 的迭代器类，用于遍历合并结果。
     */
    private class SortMergeIterator implements RecordIterator<T> {

        private boolean released = false; // 标记当前批次是否已释放

        /**
         * 获取下一个合并结果。
         *
         * @return 下一个合并后的记录，如果没有更多记录返回 null。
         * @throws IOException 如果读取数据失败。
         */
        @Nullable
        @Override
        public T next() throws IOException {
            while (true) {
                loserTree.adjustForNextLoop(); // 调整败者树以准备下一轮迭代
                KeyValue winner = loserTree.popWinner(); // 获取当前胜者
                if (winner == null) {
                    return null;
                }
                mergeFunctionWrapper.reset(); // 重置合并函数
                mergeFunctionWrapper.add(winner); // 将当前胜者加入合并函数

                T result = merge(); // 执行合并
                if (result != null) {
                    return result;
                }
            }
        }

        /**
         * 合并当前批次的数据。
         *
         * @return 合并结果。
         */
        private T merge() {
            Preconditions.checkState(
                    !released, "SortMergeIterator#nextImpl is called after release");

            // 将败者树中剩余的记录加入合并函数
            while (loserTree.peekWinner() != null) {
                mergeFunctionWrapper.add(loserTree.popWinner());
            }
            return mergeFunctionWrapper.getResult(); // 返回合并结果
        }

        /**
         * 释放当前批次的资源。
         */
        @Override
        public void releaseBatch() {
            released = true; // 标记为已释放
        }
    }
}

