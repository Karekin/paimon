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

package org.apache.paimon.data.columnar.heap;

import org.apache.paimon.data.columnar.writable.AbstractWritableVector;
import org.apache.paimon.memory.MemorySegment;

import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * 支持可空值的堆向量基类。
 *
 * <p>该类继承自 {@link AbstractWritableVector}，实现了共享的可空结构和字典相关的操作。
 * 它是其他堆向量类（如长整型或浮点型向量）的基础。
 */
public abstract class AbstractHeapVector extends AbstractWritableVector {

    // 判断当前系统是否为小端字节序
    public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    // 通过 sun.misc.Unsafe 提供低级内存操作
    public static final sun.misc.Unsafe UNSAFE = MemorySegment.UNSAFE;

    // 获取数组在内存中的偏移量，用于高效访问
    public static final int BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    public static final int INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
    public static final int LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
    public static final int FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
    public static final int DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);

    /*
     * 如果 hasNulls 为 true，则 isNull 数组包含每个值是否为 null 的信息：
     * - true 表示该值为 null
     * - false 表示该值不为 null
     * 该数组始终被分配，以便后续批次可以重用并支持动态添加 null 值。
     */
    protected boolean[] isNull;

    /** 可重用的用于存储字典 ID 的整数列向量。 */
    protected HeapIntVector dictionaryIds;

    // 列向量的长度（行数）
    private final int len;

    /**
     * 构造函数。
     *
     * @param len 列向量的行数（长度）。
     */
    public AbstractHeapVector(int len) {
        isNull = new boolean[len]; // 初始化可空数组
        this.len = len; // 保存列向量长度
    }

    /**
     * 重置列向量到默认状态。
     * - 将 `isNull` 数组的值设置为 false。
     * - 设置 `noNulls` 为 true，表示没有空值。
     */
    @Override
    public void reset() {
        if (!noNulls) {
            Arrays.fill(isNull, false); // 重置所有值为非 null
        }
        noNulls = true;
    }

    /**
     * 将指定索引的值设置为 null。
     *
     * @param i 要设置为 null 的行索引。
     */
    @Override
    public void setNullAt(int i) {
        isNull[i] = true; // 设置为 null
        noNulls = false; // 标记当前列向量包含空值
    }

    /**
     * 将从指定索引开始的一段连续行设置为 null。
     *
     * @param i 起始行索引。
     * @param count 要设置为 null 的行数。
     */
    @Override
    public void setNulls(int i, int count) {
        for (int j = 0; j < count; j++) {
            isNull[i + j] = true;
        }
        if (count > 0) {
            noNulls = false; // 标记当前列向量包含空值
        }
    }

    /**
     * 将整个列向量填充为 null。
     */
    @Override
    public void fillWithNulls() {
        this.noNulls = false; // 标记包含空值
        Arrays.fill(isNull, true); // 将所有值设置为 null
    }

    /**
     * 检查指定索引的值是否为 null。
     *
     * @param i 要检查的行索引。
     * @return 如果值为 null 返回 true，否则返回 false。
     */
    @Override
    public boolean isNullAt(int i) {
        return !noNulls && isNull[i];
    }

    /**
     * 为字典的 ID 保留一个整数列向量。
     *
     * <p>如果当前字典列向量为空或容量不足，则创建新的列向量。
     * 否则，重置现有的列向量以复用。
     *
     * @param capacity 需要保留的最小容量。
     * @return 一个可写的整数列向量，用于存储字典 ID。
     */
    @Override
    public HeapIntVector reserveDictionaryIds(int capacity) {
        if (dictionaryIds == null) {
            dictionaryIds = new HeapIntVector(capacity); // 创建新的字典列向量
        } else {
            if (capacity > dictionaryIds.vector.length) {
                // 如果容量不足，扩展当前列向量
                int current = dictionaryIds.vector.length;
                while (current < capacity) {
                    current <<= 1; // 按 2 倍扩展容量
                }
                dictionaryIds = new HeapIntVector(current);
            } else {
                dictionaryIds.reset(); // 重置现有列向量
            }
        }
        return dictionaryIds;
    }

    /**
     * 返回用于存储字典 ID 的整数列向量。
     *
     * @return 一个整数列向量，用于存储字典 ID。
     */
    @Override
    public HeapIntVector getDictionaryIds() {
        return dictionaryIds;
    }

    /**
     * 获取列向量的长度（行数）。
     *
     * @return 列向量的长度。
     */
    public int getLen() {
        return this.len;
    }
}

