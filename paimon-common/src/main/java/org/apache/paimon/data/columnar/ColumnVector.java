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

package org.apache.paimon.data.columnar;

/**
 * 可空列向量接口。
 *
 * <p>此接口表示一个支持空值的列向量，具体数据的访问需要通过其特定的子类实现。
 * 列向量是批量数据处理中的一种常用数据结构，用于高效地存储和操作表格中的列数据。
 */
public interface ColumnVector {

    /**
     * 检查指定位置的数据是否为 null。
     *
     * @param i 要检查的行索引。
     * @return 如果指定位置的数据为 null，则返回 true；否则返回 false。
     */
    boolean isNullAt(int i);
}

