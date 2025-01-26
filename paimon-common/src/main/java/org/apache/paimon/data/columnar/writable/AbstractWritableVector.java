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

import java.io.Serializable;

/**
 * 为 {@link ColumnVector} 提供共享结构的抽象基类，包括 NULL 信息和字典功能。
 *
 * <p>注意：如果列向量中存在 null 值，必须将 {@link #noNulls} 设置为 false。
 */
public abstract class AbstractWritableVector implements WritableColumnVector, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 指示整个列向量是否没有 null 值。
     * - 如果整个列向量没有 null 值，则为 true。
     * - 如果存在任何 null 值，则为 false。
     */
    protected boolean noNulls = true;

    /**
     * 该列向量的字典。
     *
     * <p>如果字典不为 null，则在 {@code get()} 方法中会使用字典解码值。
     */
    protected Dictionary dictionary;

    /**
     * 更新列向量的字典。
     *
     * <p>字典用于编码和解码列向量中的值，以节省存储空间并提高操作效率。
     *
     * @param dictionary 要设置的新字典对象。
     */
    @Override
    public void setDictionary(Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    /**
     * 检查列向量是否有字典。
     *
     * @return 如果字典不为 null，则返回 true；否则返回 false。
     */
    @Override
    public boolean hasDictionary() {
        return this.dictionary != null;
    }
}

