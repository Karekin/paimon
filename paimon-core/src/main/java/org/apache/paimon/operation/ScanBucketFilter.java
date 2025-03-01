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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;
import static org.apache.paimon.predicate.PredicateBuilder.splitOr;

/**
 * 扫描时的桶过滤器，用于跳过不需要的文件。
 */
public abstract class ScanBucketFilter {

    public static final int MAX_VALUES = 1000; // 最大值数目

    private final RowType bucketKeyType; // 桶键类型

    private ScanBucketSelector selector; // 桶选择器

    /**
     * 构造函数，初始化桶过滤器。
     * @param bucketKeyType 桶键类型
     */
    public ScanBucketFilter(RowType bucketKeyType) {
        this.bucketKeyType = bucketKeyType;
    }

    /**
     * 将谓词下推到扫描中。
     * @param predicate 谓词
     */
    public abstract void pushdown(Predicate predicate);

    /**
     * 设置桶键过滤器。
     * @param predicate 谓词
     */
    public void setBucketKeyFilter(Predicate predicate) {
        this.selector = create(predicate, bucketKeyType).orElse(null);
    }

    /**
     * 判断是否选择指定的桶。
     * @param bucket 桶号
     * @param numBucket 总桶数
     * @return 是否选择
     */
    public boolean select(int bucket, int numBucket) {
        return selector == null || selector.select(bucket, numBucket);
    }

    /**
     * 根据谓词和桶键类型创建扫描桶选择器。
     * @param bucketPredicate 谓词
     * @param bucketKeyType 桶键类型
     * @return 扫描桶选择器的可选值
     */
    @VisibleForTesting
    static Optional<ScanBucketSelector> create(Predicate bucketPredicate, RowType bucketKeyType) {
        @SuppressWarnings("unchecked")
        List<Object>[] bucketValues = new List[bucketKeyType.getFieldCount()];

        nextAnd:
        for (Predicate andPredicate : splitAnd(bucketPredicate)) {
            Integer reference = null;
            List<Object> values = new ArrayList<>();
            for (Predicate orPredicate : splitOr(andPredicate)) {
                if (orPredicate instanceof LeafPredicate) {
                    LeafPredicate leaf = (LeafPredicate) orPredicate;
                    if (reference == null || reference == leaf.index()) {
                        reference = leaf.index();
                        if (leaf.function().equals(Equal.INSTANCE)
                                || leaf.function().equals(In.INSTANCE)) {
                            values.addAll(
                                    leaf.literals().stream()
                                            .filter(Objects::nonNull)
                                            .collect(Collectors.toList()));
                            continue;
                        }
                    }
                }

                // 跳出循环，继续下一个谓词
                continue nextAnd;
            }
            if (reference != null) {
                if (bucketValues[reference] != null) {
                    // And 中重复的相等条件
                    return Optional.empty();
                }

                bucketValues[reference] = values;
            }
        }

        int rowCount = 1;
        for (List<Object> values : bucketValues) {
            if (values == null) {
                return Optional.empty();
            }

            rowCount *= values.size();
            if (rowCount > MAX_VALUES) {
                return Optional.empty();
            }
        }

        InternalRowSerializer serializer = new InternalRowSerializer(bucketKeyType);
        List<Integer> hashCodes = new ArrayList<>();
        assembleRows(
                bucketValues,
                columns -> hashCodes.add(hash(columns, serializer)),
                new ArrayList<>(),
                0);

        return Optional.of(new ScanBucketSelector(hashCodes.stream().mapToInt(i -> i).toArray()));
    }

    private static int hash(List<Object> columns, InternalRowSerializer serializer) {
        BinaryRow binaryRow = serializer.toBinaryRow(GenericRow.of(columns.toArray()));
        return KeyAndBucketExtractor.bucketKeyHashCode(binaryRow);
    }

    private static void assembleRows(
            List<Object>[] rowValues,
            Consumer<List<Object>> consumer,
            List<Object> stack,
            int columnIndex) {
        List<Object> columnValues = rowValues[columnIndex];
        for (Object value : columnValues) {
            stack.add(value);
            if (columnIndex == rowValues.length - 1) {
                // 最后一列，处理行
                consumer.accept(stack);
            } else {
                assembleRows(rowValues, consumer, stack, columnIndex + 1);
            }
            stack.remove(stack.size() - 1);
        }
    }

    /**
     * 选择桶的选择器。
     */
    @ThreadSafe
    public static class ScanBucketSelector {

        private final int[] hashCodes; // 哈希码数组

        private final Map<Integer, Set<Integer>> buckets = new ConcurrentHashMap<>();

        public ScanBucketSelector(int[] hashCodes) {
            this.hashCodes = hashCodes;
        }

        /**
         * 判断是否选择指定的桶。
         * @param bucket 桶号
         * @param numBucket 总桶数
         * @return 是否选择
         */
        @VisibleForTesting
        boolean select(int bucket, int numBucket) {
            return buckets.computeIfAbsent(numBucket, k -> createBucketSet(numBucket))
                    .contains(bucket);
        }

        @VisibleForTesting
        int[] hashCodes() {
            return hashCodes;
        }

        @VisibleForTesting
        Set<Integer> createBucketSet(int numBucket) {
            ImmutableSet.Builder<Integer> builder = new ImmutableSet.Builder<>();
            for (int hash : hashCodes) {
                builder.add(KeyAndBucketExtractor.bucket(hash, numBucket));
            }
            return builder.build();
        }
    }
}
