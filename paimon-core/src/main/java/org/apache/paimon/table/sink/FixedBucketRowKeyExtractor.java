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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;

/**
 * {@link InternalRow} 的 {@link KeyAndBucketExtractor} 实现，
 * 主要用于根据表的分桶策略提取键值并计算数据所属的桶编号。
 */
public class FixedBucketRowKeyExtractor extends RowKeyExtractor {

    // 分桶的总数，即数据可分布的桶的数量
    private final int numBuckets;

    // 标记分桶键（bucket key）是否与去除部分字段的主键（trimmed primary key）相同
    private final boolean sameBucketKeyAndTrimmedPrimaryKey;

    // 用于从记录中投影出分桶键的投影操作
    private final Projection bucketKeyProjection;

    // 复用的二进制行对象，存储计算出的分桶键，以减少重复计算
    private BinaryRow reuseBucketKey;

    // 复用的分桶编号，存储计算出的桶索引
    private Integer reuseBucket;

    /**
     * 构造函数，基于表模式（schema）初始化分桶信息。
     *
     * @param schema 表模式，包含表的结构信息、主键信息、分桶信息等
     */
    public FixedBucketRowKeyExtractor(TableSchema schema) {
        super(schema);
        // 通过表模式配置获取分桶数量
        numBuckets = new CoreOptions(schema.options()).bucket();
        // 判断分桶键是否与去除部分字段的主键相同
        sameBucketKeyAndTrimmedPrimaryKey = schema.bucketKeys().equals(schema.trimmedPrimaryKeys());
        // 通过 CodeGen 工具生成分桶键投影逻辑
        bucketKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.bucketKeys()));
    }

    /**
     * 设置当前处理的记录，并清除复用缓存的分桶键和分桶编号。
     *
     * @param record 当前处理的 {@link InternalRow} 记录
     */
    @Override
    public void setRecord(InternalRow record) {
        super.setRecord(record);
        // 清空复用的分桶键，确保新的记录可以正确计算
        this.reuseBucketKey = null;
        // 清空复用的分桶编号，防止使用上一个记录的分桶信息
        this.reuseBucket = null;
    }

    /**
     * 获取当前记录的分桶键（bucket key）。
     *
     * 计算逻辑：
     * 1. 如果分桶键与去除部分字段的主键相同，则直接复用主键。
     * 2. 否则，使用 `bucketKeyProjection` 计算分桶键，并缓存到 `reuseBucketKey` 以避免重复计算。
     *
     * @return 当前记录对应的分桶键
     */
    private BinaryRow bucketKey() {
        if (sameBucketKeyAndTrimmedPrimaryKey) {
            // 直接使用去除部分字段的主键作为分桶键
            return trimmedPrimaryKey();
        }

        if (reuseBucketKey == null) {
            // 通过投影操作计算分桶键，并存入复用变量
            reuseBucketKey = bucketKeyProjection.apply(record);
        }
        return reuseBucketKey;
    }

    /**
     * 计算当前记录应该被分配到的桶编号（bucket index）。
     *
     * 计算逻辑：
     * 1. 先获取当前记录的分桶键。
     * 2. 如果 `reuseBucket` 为空，则计算分桶编号：
     *    - 通过 `bucketKey` 计算哈希值。
     *    - 结合 `numBuckets` 计算最终的分桶索引，并缓存结果。
     * 3. 返回分桶编号。
     *
     * @return 当前记录对应的桶索引
     */
    @Override
    public int bucket() {
        // 获取当前记录的分桶键
        BinaryRow bucketKey = bucketKey();
        if (reuseBucket == null) {
            // 计算哈希值并获取分桶索引
            reuseBucket =
                    KeyAndBucketExtractor.bucket(
                            KeyAndBucketExtractor.bucketKeyHashCode(bucketKey), numBuckets);
        }
        return reuseBucket;
    }
}

