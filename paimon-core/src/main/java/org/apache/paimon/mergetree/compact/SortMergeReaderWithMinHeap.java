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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 基于最小堆实现的排序合并读取器。
 */
public class SortMergeReaderWithMinHeap<T> implements SortMergeReader<T> {

    // 用于暂存下一批读取器，这些读取器的数据还未被处理到最小堆中
    private final List<RecordReader<KeyValue>> nextBatchReaders;
    // 用户提供的键比较器，用于比较键的大小
    private final Comparator<InternalRow> userKeyComparator;
    // 用于合并键相同的数据的函数包装器
    private final MergeFunctionWrapper<T> mergeFunctionWrapper;

    // 基于最小堆的优先队列，用于管理待处理的数据元素
    private final PriorityQueue<Element> minHeap;
    // 已从优先队列中移除的元素列表，用于后续的处理
    private final List<Element> polled;

    /**
     * 构造方法，初始化基于最小堆的排序合并读取器。
     * @param readers                   待处理的记录读取器列表
     * @param userKeyComparator         用户提供的键比较器
     * @param userDefinedSeqComparator  用户自定义的字段比较器
     * @param mergeFunctionWrapper      合并函数包装器
     */
    public SortMergeReaderWithMinHeap(
            List<RecordReader<KeyValue>> readers,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper) {
        this.nextBatchReaders = new ArrayList<>(readers); // 初始化暂存读取器列表
        this.userKeyComparator = userKeyComparator; // 初始化键比较器
        this.mergeFunctionWrapper = mergeFunctionWrapper; // 初始化合并函数包装器

        // 初始化最小堆，自定义比较器用于确定堆中的顺序
        this.minHeap =
                new PriorityQueue<>(
                        (e1, e2) -> {
                            int result = userKeyComparator.compare(e1.kv.key(), e2.kv.key());
                            if (result != 0) {
                                return result;
                            }
                            if (userDefinedSeqComparator != null) {
                                result =
                                        userDefinedSeqComparator.compare(e1.kv.value(), e2.kv.value());
                                if (result != 0) {
                                    return result;
                                }
                            }
                            // 如果键和字段都相同，则根据序列号比较
                            return Long.compare(e1.kv.sequenceNumber(), e2.kv.sequenceNumber());
                        });
        this.polled = new ArrayList<>(); // 初始化移除元素列表
    }

    /**
     * 读取一批记录。
     * @return                           记录迭代器，或 null 如果没有更多数据
     * @throws IOException               输入输出异常
     */
    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        for (RecordReader<KeyValue> reader : nextBatchReaders) {
            while (true) {
                RecordIterator<KeyValue> iterator = reader.readBatch(); // 从读取器中读取批处理数据
                if (iterator == null) {
                    // 读取器中没有更多批处理数据，关闭读取器并移除
                    reader.close();
                    break;
                }
                KeyValue kv = iterator.next(); // 读取记录
                if (kv == null) {
                    // 迭代器为空，释放资源并继续
                    iterator.releaseBatch();
                } else {
                    // 将记录包装为 Element 并添加到最小堆中
                    minHeap.offer(new Element(kv, iterator, reader));
                    break;
                }
            }
        }
        nextBatchReaders.clear(); // 清空暂存读取器列表

        // 如果最小堆为空，返回 null 表示没有更多数据
        return minHeap.isEmpty() ? null : new SortMergeIterator();
    }

    /**
     * 关闭资源。
     * @throws IOException               输入输出异常
     */
    @Override
    public void close() throws IOException {
        // 关闭暂存读取器列表中的所有读取器
        for (RecordReader<KeyValue> reader : nextBatchReaders) {
            reader.close();
        }
        // 释放最小堆中元素的资源
        for (Element element : minHeap) {
            element.iterator.releaseBatch();
            element.reader.close();
        }
        // 释放已移除元素列表中的资源
        for (Element element : polled) {
            element.iterator.releaseBatch();
            element.reader.close();
        }
    }

    /**
     * 排序合并迭代器，用于逐条读取排序合并后的数据。
     */
    private class SortMergeIterator implements RecordIterator<T> {

        private boolean released = false; // 标记是否已释放批处理

        /**
         * 获取下一条记录。
         * @return                          数据记录，或 null 如果没有更多数据
         * @throws IOException              输入输出异常
         */
        @Override
        public T next() throws IOException {
            while (true) {
                boolean hasMore = nextImpl(); // 尝试获取下一条数据
                if (!hasMore) {
                    return null;
                }
                T result = mergeFunctionWrapper.getResult(); // 获取合并结果
                if (result != null) {
                    return result;
                }
            }
        }

        /**
         * 获取下一条数据的实现。
         * @return                          是否有更多数据
         * @throws IOException              输入输出异常
         */
        private boolean nextImpl() throws IOException {
            Preconditions.checkState(
                    !released,
                    "SortMergeIterator#advanceNext is called after release");
            Preconditions.checkState(
                    nextBatchReaders.isEmpty(),
                    "SortMergeIterator#advanceNext is called even if the last call returns null. "
                            + "This is a bug.");

            // 将已移除的元素重新添加到优先队列中
            for (Element element : polled) {
                if (element.update()) {
                    // 如果仍有数据，重新添加到队列中
                    minHeap.offer(element);
                } else {
                    // 释放资源并标记读取器
                    element.iterator.releaseBatch();
                    nextBatchReaders.add(element.reader);
                }
            }
            polled.clear();

            // 如果仍有待处理的读取器，结束当前批处理
            if (!nextBatchReaders.isEmpty()) {
                return false;
            }

            mergeFunctionWrapper.reset(); // 重置合并函数包装器
            // 获取当前最小堆中的键
            InternalRow key =
                    Preconditions.checkNotNull(minHeap.peek(), "Min heap is empty. This is a bug.")
                            .kv
                            .key();

            // 从最小堆中获取所有具有相同键的元素
            while (!minHeap.isEmpty()) {
                Element element = minHeap.peek();
                if (userKeyComparator.compare(key, element.kv.key()) != 0) {
                    break;
                }
                minHeap.poll(); // 移除元素
                mergeFunctionWrapper.add(element.kv); // 将元素添加到合并函数包装器中
                polled.add(element); // 添加到移除元素列表
            }
            return true; // 返回 true 表示仍有数据
        }

        /**
         * 释放批处理资源。
         */
        @Override
        public void releaseBatch() {
            released = true; // 标记批处理已释放
        }
    }

    /**
     * 数据元素，用于封装键值对、迭代器和读取器。
     */
    private static class Element {
        // 当前键值对
        private KeyValue kv;
        // 记录迭代器，用于读取后续数据
        private final RecordIterator<KeyValue> iterator;
        // 记录读取器
        private final RecordReader<KeyValue> reader;

        /**
         * 构造方法，初始化数据元素。
         * @param kv           当前键值对
         * @param iterator     记录迭代器
         * @param reader       记录读取器
         */
        private Element(
                KeyValue kv,
                RecordIterator<KeyValue> iterator,
                RecordReader<KeyValue> reader) {
            this.kv = kv;
            this.iterator = iterator;
            this.reader = reader;
        }

        /**
         * 更新元素，读取下一个键值对。
         * @return              是否成功更新
         * @throws IOException  输入输出异常
         */
        private boolean update() throws IOException {
            // 调用此方法时，元素不应仍处于优先队列中
            KeyValue nextKv = iterator.next();
            if (nextKv == null) {
                return false; // 没有更多数据
            }
            kv = nextKv; // 更新键值对
            return true;
        }
    }
}