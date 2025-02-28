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

package org.apache.paimon.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;

import static java.util.Comparator.comparingLong;

/**
 * 包含装箱实现的类。
 * <p>装箱问题是指将不同大小的物品装入固定容量的箱子，尽量减少箱子的数量。
 * 这个类通过不同的算法实现了装箱功能。
 */
public class BinPacking {
    private BinPacking() {}

    /**
     * 按有序方式对输入物品进行装箱。
     * 物品按顺序被装入箱子，当箱子中的重量超过目标重量时，将箱子打包并创建新箱子。
     *
     * @param items       待装箱的物品
     * @param weightFunc  获取物品重量的函数
     * @param targetWeight 箱子的目标重量
     * @return 装箱后的物品列表
     */
    public static <T> List<List<T>> packForOrdered(
            Iterable<T> items, Function<T, Long> weightFunc, long targetWeight) {
        List<List<T>> packed = new ArrayList<>();

        List<T> binItems = new ArrayList<>();
        long binWeight = 0L;

        for (T item : items) {
            long weight = weightFunc.apply(item);
            // 当箱子的总重量加上当前物品的重量超过目标重量且箱子中有物品时，将箱子打包
            if (binWeight + weight > targetWeight && binItems.size() > 0) {
                packed.add(binItems);
                binItems = new ArrayList<>();
                binWeight = 0;
            }

            binWeight += weight;
            binItems.add(item);
        }

        if (binItems.size() > 0) {
            packed.add(binItems);
        }
        return packed;
    }

    /**
     * 固定箱数量的装箱实现。
     * 根据指定的箱数量，将物品均匀地分配到每个箱中。
     *
     * @param items       待装箱的物品
     * @param weightFunc  获取物品重量的函数
     * @param binNumber   指定的箱数量
     * @return 装箱后的物品列表
     */
    public static <T> List<List<T>> packForFixedBinNumber(
            Iterable<T> items, Function<T, Long> weightFunc, int binNumber) {
        // 1. 首先对物品按重量进行排序
        List<T> sorted = new ArrayList<>();
        items.forEach(sorted::add);
        sorted.sort(comparingLong(weightFunc::apply));

        // 2. 进行装箱
        PriorityQueue<FixedNumberBin<T>> bins = new PriorityQueue<>();
        for (T item : sorted) {
            long weight = weightFunc.apply(item);
            FixedNumberBin<T> bin;
            if (bins.size() < binNumber) {
                // 如果箱子数量未达到指定数量，创建新箱子
                bin = new FixedNumberBin<>();
            } else {
                // 从优先队列中取出当前最轻的箱子
                bin = bins.poll();
            }
            bin.add(item, weight);
            bins.add(bin);
        }

        // 3. 输出装箱结果
        List<List<T>> packed = new ArrayList<>();
        bins.forEach(bin -> packed.add(bin.items));
        return packed;
    }

    /**
     * 表示具有固定数量的箱子。
     */
    private static class FixedNumberBin<T> implements Comparable<FixedNumberBin<T>> {
        private final List<T> items = new ArrayList<>();
        private long binWeight = 0L;

        void add(T item, long weight) {
            this.binWeight += weight;
            items.add(item);
        }

        @Override
        public int compareTo(FixedNumberBin<T> other) {
            return Long.compare(binWeight, other.binWeight);
        }
    }
}
