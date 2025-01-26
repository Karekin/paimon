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

package org.apache.paimon.data.columnar.writable;

import org.apache.paimon.data.columnar.LongColumnVector;

/**
 * 可写的 {@link LongColumnVector} 接口。
 *
 * <p>该接口扩展了 {@link WritableColumnVector} 和 {@link LongColumnVector}，
 * 提供对长整型列向量的写入操作支持，包括单个值设置、批量设置和填充操作。
 */
public interface WritableLongVector extends WritableColumnVector, LongColumnVector {

    /**
     * 在指定的行索引设置长整型值。
     *
     * @param rowId 要设置值的行索引。
     * @param value 要设置的长整型值。
     */
    void setLong(int rowId, long value);

    /**
     * 从二进制数据中批量设置长整型值。
     *
     * <p>该方法使用 {@code UNSAFE} 工具进行内存复制，能够高效地从二进制数据中提取长整型值并设置到列向量中。
     *
     * @param rowId 起始行索引，从该行开始设置值。
     * @param count 要设置的长整型值的数量（每个长整型值占用 8 字节）。
     * @param src 源二进制数据。
     * @param srcIndex 源二进制数据的起始索引（以字节为单位）。
     * @throws IndexOutOfBoundsException 如果指定的行索引或源索引超出范围。
     */
    void setLongsFromBinary(int rowId, int count, byte[] src, int srcIndex);

    /**
     * 使用指定的长整型值填充整个列向量。
     *
     * @param value 用于填充列向量的长整型值。
     */
    void fill(long value);
}
