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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * 这个类实现了将若干数据文件（DataFileMeta）划分为最少数量的 {@link SortedRun} 的算法。
 * <p>
 * 一句话概括：它通过比较文件的键区间（minKey 和 maxKey），将有重叠区间的文件放到同一个区段（section）里，
 * 并在区段内进一步划分为若干 {@link SortedRun}。
 * <p>
 * 在读取或合并时，我们可以对每个区段内的 {@link SortedRun} 进行有序合并（merge sort）。
 */
public class IntervalPartition {

    // 待处理的数据文件列表
    private final List<DataFileMeta> files;
    // 用于比较 InternalRow 的比较器，主要比较文件的 minKey 和 maxKey
    private final Comparator<InternalRow> keyComparator;

    /**
     * 构造函数：
     * 1. 将传入的文件列表进行拷贝。
     * 2. 按照 (minKey, maxKey) 的顺序对文件进行排序，确保后续处理时文件按键值顺序排列。
     *
     * @param inputFiles     待处理的数据文件列表
     * @param keyComparator  比较器，用于比较文件的 minKey 和 maxKey
     */
    public IntervalPartition(List<DataFileMeta> inputFiles, Comparator<InternalRow> keyComparator) {
        this.files = new ArrayList<>(inputFiles);
        // 按照 minKey 进行排序，如果 minKey 相同，则比较 maxKey
        this.files.sort(
                (o1, o2) -> {
                    int leftResult = keyComparator.compare(o1.minKey(), o2.minKey());
                    return leftResult == 0
                            ? keyComparator.compare(o1.maxKey(), o2.maxKey())
                            : leftResult;
                });
        this.keyComparator = keyComparator;
    }

    /**
     * 返回一个二维列表，每个元素代表一个 section（键区间彼此不重叠），
     * 而每个 section 内部又包含若干个 {@link SortedRun}。
     *
     * <p>外层列表的元素（section）之间的键区间不重叠，这使得在处理数据时，可以一次只处理一个 section，
     * 从而减少同时处理的 {@link SortedRun} 数量。
     *
     * <p>内层列表（每个 section）中的 {@link SortedRun} 则表示该 section 内部需要合并（或排序）的一组文件。
     *
     * <p>通常使用方式：
     * <pre>{@code
     * for (List<SortedRun> section : algorithm.partition()) {
     *     // 对每个section内部的SortedRun进行合并排序
     * }
     * }</pre>
     *
     * @return 二维列表，每个元素为一个包含若干 SortedRun 的section
     */
    public List<List<SortedRun>> partition() {
        // 存放最终结果：多个 section，每个 section 又对应多个 SortedRun
        List<List<SortedRun>> result = new ArrayList<>();
        // 临时存放当前section的文件集合
        List<DataFileMeta> section = new ArrayList<>();
        // 用来记录当前section的右边界（即最大 maxKey）
        BinaryRow bound = null;

        // 遍历所有文件，按照之前排好序的顺序
        for (DataFileMeta meta : files) {
            // 如果当前section不空，且meta的最小值比当前bound还要大，
            // 说明这个文件与当前section没有区间重叠，可以结束当前section，开始一个新的section  TODO 为什么总是false？不走ConcatRecordReader的批读逻辑
            if (!section.isEmpty() && keyComparator.compare(meta.minKey(), bound) > 0) {
                result.add(partition(section));
                section.clear();
                bound = null;
            }
            // 否则，将当前文件加入到section里
            section.add(meta);
            // 更新section的最大边界
            if (bound == null || keyComparator.compare(meta.maxKey(), bound) > 0) {
                bound = meta.maxKey();
            }
        }
        // 最后如果section里还有文件没有处理完，则将其作为最后一个section
        if (!section.isEmpty()) {
            result.add(partition(section));
        }

        return result;
    }

    /**
     * 将同一个section内的文件列表再细分为若干 {@link SortedRun}。
     * <p>
     * 具体做法是使用一个小顶堆（按每个run的最后一个文件的maxKey来排序），
     * 尝试把文件放进能容纳（即 key 区间无重叠）的 run，如果没有合适的 run，就创建新的 run。
     *
     * @param metas 当前section内的文件列表
     * @return 该section被进一步划分后的若干SortedRun
     */
    private List<SortedRun> partition(List<DataFileMeta> metas) {
        // 优先队列(小顶堆)，比较依据是每个列表最后一个文件的 maxKey 较小者优先
        PriorityQueue<List<DataFileMeta>> queue =
                new PriorityQueue<>(
                        (o1, o2) ->
                                // 按照各自最后一个文件的maxKey进行比较
                                keyComparator.compare(
                                        o1.get(o1.size() - 1).maxKey(),
                                        o2.get(o2.size() - 1).maxKey()));

        // 初始化：先把metas的第一个文件单独放到一个run里
        List<DataFileMeta> firstRun = new ArrayList<>();
        firstRun.add(metas.get(0));
        queue.add(firstRun);

        // 从第二个文件开始，依次尝试放到合适的run中
        for (int i = 1; i < metas.size(); i++) {
            DataFileMeta meta = metas.get(i);
            // 取出堆顶（maxKey最小的run）
            List<DataFileMeta> top = queue.poll();
            // 如果当前文件的minKey比这个run最后文件的maxKey还要大，表示可以安全地拼接到同一个run里
            if (keyComparator.compare(meta.minKey(), top.get(top.size() - 1).maxKey()) > 0) {
                top.add(meta);
            } else {
                // 否则，需要创建一个新的run
                List<DataFileMeta> newRun = new ArrayList<>();
                newRun.add(meta);
                queue.add(newRun);
            }
            // 将更新后的run重新放回堆中
            queue.add(top);
        }

        // 将最终堆中所有的run转换为SortedRun返回
        // 这里顺序无关，因为不同run之间的排序并不重要
        return queue.stream().map(SortedRun::fromSorted).collect(Collectors.toList());
    }
}

