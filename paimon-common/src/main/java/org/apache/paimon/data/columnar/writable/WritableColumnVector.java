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

import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.Dictionary;

/**
 * 可写的 {@link ColumnVector} 接口。
 *
 * <p>该接口扩展了 {@link ColumnVector}，支持对列向量的写入操作，包括设置值、设置空值以及使用字典等操作。
 */
public interface WritableColumnVector extends ColumnVector {

    /**
     * 重置列向量到默认状态。
     *
     * <p>该方法通常用于清空当前列向量的内容，以便复用列向量对象。
     */
    void reset();

    /**
     * 在指定行设置为 null。
     *
     * @param rowId 要设置为 null 的行的索引。
     */
    void setNullAt(int rowId);

    /**
     * 将从指定行开始的一段连续行设置为 null。
     *
     * @param rowId 起始行索引（包含）。
     * @param count 要设置为 null 的行数。
     */
    void setNulls(int rowId, int count);

    /**
     * 将整个列向量填充为 null。
     *
     * <p>该方法通常用于初始化或清空整个列向量。
     */
    void fillWithNulls();

    /**
     * 设置字典，该字典应该与字典 ID 一起使用。
     *
     * @param dictionary 要设置的字典对象。
     */
    void setDictionary(Dictionary dictionary);

    /**
     * 检查是否已设置字典。
     *
     * @return 如果有字典返回 true，否则返回 false。
     */
    boolean hasDictionary();

    /**
     * 为字典的 ID 保留一个整数列。
     *
     * <p>返回的 {@link WritableIntVector} 的大小应等于或大于指定的容量。
     * 字典 ID 必须与 {@link #setDictionary} 一致。当前不支持混合字典的操作。
     *
     * @param capacity 要保留的最小容量。
     * @return 可写的整数向量，用于存储字典 ID。
     */
    WritableIntVector reserveDictionaryIds(int capacity);

    /**
     * 获取已保留的字典 ID 列。
     *
     * <p>该方法返回与字典相关联的整数向量。
     *
     * @return 可写的整数向量，包含字典 ID。
     */
    WritableIntVector getDictionaryIds();
}

