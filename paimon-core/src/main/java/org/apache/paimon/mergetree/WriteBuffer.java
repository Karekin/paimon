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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;

/**
 * 仅附加写缓冲区，用于存储键值对。当其已满时，会将数据刷新到磁盘并形成数据文件。
 */
public interface WriteBuffer {

    /**
     * 将一条带有序列号和值类型的记录写入缓冲区。
     *
     * @param sequenceNumber 序列号，标识该条目的顺序
     * @param valueKind 值的类型（例如，插入、更新、删除等）
     * @param key 键，类型为 InternalRow
     * @param value 值，类型为 InternalRow
     * @return 如果记录成功写入，则返回true；如果内存表已满，则返回false
     * @throws IOException 如果在写入过程中发生I/O错误
     */
    boolean put(long sequenceNumber, RowKind valueKind, InternalRow key, InternalRow value) throws IOException;

    /**
     * 获取记录的大小
     */
    int size();

    /**
     * 获取内存占用的大小
     * 对此缓冲区中的每个剩余元素执行给定的操作，直到所有元素都*已处理或操作引发异常。
     */
    long memoryOccupancy();

    /**
     * 将内存中的数据刷新到磁盘，如果操作不支持则返回false
     * 对此缓冲区中的每个剩余元素执行给定的操作，直到所有元素都*已处理或操作引发异常。
     */
    boolean flushMemory() throws IOException;

    /**
     * 对此缓冲区中的每个剩余元素执行给定的操作，直到所有元素都已处理或操作引发异常。
     *
     * @param keyComparator 比较键的比较器
     * @param mergeFunction 合并函数
     * @param rawConsumer   用于消费未合并记录的消费者
     * @param mergedConsumer 用于消费合并后的记录的消费者
     * 对此缓冲区中的每个剩余元素执行给定的操作，直到所有元素都*已处理或操作引发异常。
     */
    void forEach(
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            @Nullable KvConsumer rawConsumer,
            KvConsumer mergedConsumer) throws IOException;

    /**
     * 从这个表中移除所有记录。调用此方法后，表将为空。
     * 对此缓冲区中的每个剩余元素执行给定的操作，直到所有元素都*已处理或操作引发异常。
     */
    void clear();

    /**
     * 一个函数式接口，用于接受键值对并可能抛出异常。
     */
    @FunctionalInterface
    interface KvConsumer {
        void accept(KeyValue kv) throws IOException;
    }
}
