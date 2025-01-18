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

package org.apache.paimon.sort;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;

/** Sort buffer to sort records. */
/**
* @授课老师: 码界探索
* @微信: 252810631
* @版权所有: 请尊重劳动成果
* 用于排序记录的排序缓冲区接口。
*/
public interface SortBuffer {

    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 获取缓冲区中当前存储的记录数量。
    */
    int size();
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 清空缓冲区中的所有记录。
     */
    void clear();
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 获取缓冲区当前占用的内存大小。
     */
    long getOccupancy();

    /** Flush memory, return false if not supported. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将缓冲区中的数据刷新到外部存储（如磁盘），如果不支持刷新操作则返回false。
     */
    boolean flushMemory() throws IOException;

    /** @return false if the buffer is full. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将一个记录写入缓冲区。如果缓冲区已满，则返回false。
     */
    boolean write(InternalRow record) throws IOException;

    /** @return iterator with sorting. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 获取缓冲区中当前存储的记录数量。
     */
    MutableObjectIterator<BinaryRow> sortedIterator() throws IOException;
}
