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

import org.apache.paimon.data.columnar.writable.WritableLongVector;

import java.util.Arrays;

/**
 * 表示一个支持可空值的长整型列向量。
 *
 * <p>该类继承自 {@link AbstractHeapVector} 并实现了 {@link WritableLongVector} 接口，
 * 提供对长整型数据的高效存储和操作。
 */
public class HeapLongVector extends AbstractHeapVector implements WritableLongVector {

    private static final long serialVersionUID = 8534925169458006397L;

    // 存储列向量数据的长整型数组
    public long[] vector;

    /**
     * 构造一个长整型列向量。
     *
     * <p>该构造函数主要用于测试目的，不建议在生产环境中直接使用。
     *
     * @param len 列向量的行数（数组的长度）。
     */
    public HeapLongVector(int len) {
        super(len); // 调用父类构造函数初始化通用字段
        vector = new long[len]; // 初始化长整型数组
    }

    /**
     * 获取指定行索引的长整型值。
     *
     * @param i 要获取值的行索引。
     * @return 指定行索引的长整型值。如果存在字典，则通过字典解码获取值。
     */
    @Override
    public long getLong(int i) {
        if (dictionary == null) {
            return vector[i];
        } else {
            return dictionary.decodeToLong(dictionaryIds.vector[i]);
        }
    }

    /**
     * 设置指定行索引的长整型值。
     *
     * @param i 要设置值的行索引。
     * @param value 要设置的长整型值。
     */
    @Override
    public void setLong(int i, long value) {
        vector[i] = value;
    }

    /**
     * 从二进制数据中批量设置长整型值。
     *
     * @param rowId 起始行索引。
     * @param count 要设置的行数。
     * @param src 源二进制数据。
     * @param srcIndex 二进制数据的起始索引。
     * @throws IndexOutOfBoundsException 如果超出数组边界。
     */
    @Override
    public void setLongsFromBinary(int rowId, int count, byte[] src, int srcIndex) {
        // 检查索引是否越界
        if (rowId + count > vector.length || srcIndex + count * 8L > src.length) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "索引越界，行号：%s，行数：%s，二进制起始索引：%s，二进制长度：%s，长整型数组起始索引：%s，长整型数组长度：%s。",
                            rowId, count, srcIndex, src.length, rowId, vector.length));
        }

        if (LITTLE_ENDIAN) {
            // 小端字节序直接内存拷贝
            UNSAFE.copyMemory(
                    src,
                    BYTE_ARRAY_OFFSET + srcIndex,
                    vector,
                    LONG_ARRAY_OFFSET + rowId * 8L,
                    count * 8L);
        } else {
            // 大端字节序逐一转换
            long srcOffset = srcIndex + BYTE_ARRAY_OFFSET;
            for (int i = 0; i < count; ++i, srcOffset += 8) {
                vector[i + rowId] = Long.reverseBytes(UNSAFE.getLong(src, srcOffset));
            }
        }
    }

    /**
     * 将整个列向量填充为指定的长整型值。
     *
     * @param value 要填充的长整型值。
     */
    @Override
    public void fill(long value) {
        Arrays.fill(vector, value);
    }
}
