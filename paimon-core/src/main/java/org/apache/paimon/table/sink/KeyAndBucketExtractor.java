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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.RowKind;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 该接口用于从给定的记录中提取：
 * - 分区键（partition key）
 * - 存储桶 ID（bucket ID）
 * - 用于文件存储的主键（trimmedPrimaryKey）
 * - 用于外部日志系统的主键（logPrimaryKey）
 *
 * @param <T> 记录的类型
 */
public interface KeyAndBucketExtractor<T> {

    /**
     * 设置当前待处理的记录，用于后续提取分区键、桶 ID 和主键信息。
     *
     * @param record 当前待处理的记录
     */
    void setRecord(T record);

    /**
     * 获取当前记录对应的分区键（Partition Key）。
     *
     * @return 记录的分区键
     */
    BinaryRow partition();

    /**
     * 获取当前记录的桶 ID（Bucket ID）。
     *
     * @return 桶 ID
     */
    int bucket();

    /**
     * 获取当前记录的主键（用于文件存储）。
     *
     * @return 主键（去除了不必要字段）
     */
    BinaryRow trimmedPrimaryKey();

    /**
     * 获取当前记录的主键（用于外部日志存储）。
     *
     * @return 日志系统主键
     */
    BinaryRow logPrimaryKey();

    /**
     * 计算存储桶键（Bucket Key）的哈希值。
     *
     * @param bucketKey 存储桶键
     * @return 计算得到的哈希值
     */
    static int bucketKeyHashCode(BinaryRow bucketKey) {
        assert bucketKey.getRowKind() == RowKind.INSERT; // 只允许 INSERT 记录
        return bucketKey.hashCode();
    }

    /**
     * 计算桶 ID。
     *
     * @param hashcode    记录的哈希值
     * @param numBuckets  存储桶的数量
     * @return 计算得到的桶 ID
     */
    static int bucket(int hashcode, int numBuckets) {
        checkArgument(numBuckets > 0, "桶的数量必须大于 0: " + numBuckets);
        return Math.abs(hashcode % numBuckets); // 取模运算确保桶 ID 在合法范围内
    }
}

