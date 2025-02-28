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
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.NormalizedKeyComputer;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.sort.BinaryInMemorySortBuffer;
import org.apache.paimon.sort.SortBuffer;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.MutableObjectIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

/**
 * 一个实现了WriteBuffer接口的类，用于在BinaryInMemorySortBuffer 中存储记录。
 */
public class SortBufferWriteBuffer implements WriteBuffer {

    private final RowType keyType; // 键的类型
    private final RowType valueType; // 值的类型
    private final KeyValueSerializer serializer; // 键值序列化器
    private final SortBuffer buffer; // 排序缓冲区

    /**
     * 构造函数，用于初始化SortBufferWriteBuffer。
     *
     * @param keyType 键的类型
     * @param valueType 值的类型
     * @param userDefinedSeqComparator 用户定义的序列比较器，可能为空
     * @param memoryPool 内存池，用于分配内存段
     * @param spillable 是否允许缓冲区溢出到磁盘
     * @param maxDiskSize 最大磁盘使用大小
     * @param sortMaxFan 排序时的最大扇出数
     * @param compression 排序压缩方式
     * @param ioManager IO管理器，用于处理磁盘IO操作
     */
    public SortBufferWriteBuffer(
            RowType keyType,
            RowType valueType,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MemorySegmentPool memoryPool,
            boolean spillable,
            MemorySize maxDiskSize,
            int sortMaxFan,
            CompressOptions compression,
            IOManager ioManager) {
        this.keyType = keyType;
        this.valueType = valueType;

        // 初始化键值序列化器
        this.serializer = new KeyValueSerializer(keyType, valueType);

        // 初始化排序字段，首先是键的所有字段
        IntStream sortFields = IntStream.range(0, keyType.getFieldCount());

        // 如果有用户定义的序列比较器，则添加其比较字段
        if (userDefinedSeqComparator != null) {
            IntStream udsFields =
                    IntStream.of(userDefinedSeqComparator.compareFields())
                            .map(operand -> operand + keyType.getFieldCount() + 2);
            sortFields = IntStream.concat(sortFields, udsFields);
        }

        // 添加一个序列字段作为排序的最后一个字段
        sortFields = IntStream.concat(sortFields, IntStream.of(keyType.getFieldCount()));
        int[] sortFieldArray = sortFields.toArray();

        // 构造排序所需的字段类型列表
        List<DataType> fieldTypes = new ArrayList<>(keyType.getFieldTypes());
        fieldTypes.add(new BigIntType(false)); // 添加序列字段类型
        fieldTypes.add(new TinyIntType(false)); // 添加值类型的类型
        fieldTypes.addAll(valueType.getFieldTypes());

        // 创建规范化键计算器和记录比较器
        NormalizedKeyComputer normalizedKeyComputer =
                CodeGenUtils.newNormalizedKeyComputer(fieldTypes, sortFieldArray);
        RecordComparator keyComparator =
                CodeGenUtils.newRecordComparator(fieldTypes, sortFieldArray);

        // 检查内存池是否有足够的内存页
        if (memoryPool.freePages() < 3) {
            throw new IllegalArgumentException(
                    "Write buffer requires a minimum of 3 page memory, please increase write buffer memory size.");
        }

        // 创建内部行序列化器
        InternalRowSerializer serializer =
                InternalSerializers.create(KeyValue.schema(keyType, valueType));

        // 创建内存中的排序缓冲区
        BinaryInMemorySortBuffer inMemorySortBuffer =
                BinaryInMemorySortBuffer.createBuffer(
                        normalizedKeyComputer, serializer, keyComparator, memoryPool);

        // 根据是否允许溢出到磁盘和是否提供了IO管理器，决定是创建内存中的排序缓冲区还是结合磁盘的外部排序缓冲区
        this.buffer =
                ioManager != null && spillable
                        ? new BinaryExternalSortBuffer(
                        new BinaryRowSerializer(serializer.getArity()), // 行序列化器
                        keyComparator,
                        memoryPool.pageSize(), // 内存分页大小
                        inMemorySortBuffer,
                        ioManager, // IO管理器
                        sortMaxFan, // 排序扇出数
                        compression, // 压缩方式
                        maxDiskSize) // 最大磁盘使用量
                        : inMemorySortBuffer;
    }

    /**
     * 将给定的键值对写入缓冲区。
     * @param sequenceNumber 序列号，用于标识该条目的顺序。
     * @param valueKind 值的类型（例如，插入、更新、删除等）。
     * @param key 键，类型为InternalRow。
     * @param value 值，类型为InternalRow。
     * @return 如果写入成功，则返回true；否则返回false。
     * @throws IOException 如果在写入过程中发生I/O错误。
     */
    @Override
    public boolean put(long sequenceNumber, RowKind valueKind, InternalRow key, InternalRow value)
            throws IOException {
        // 使用序列化器将键、序列号、值类型和值组合成一个新的行，然后写入缓冲区
        return buffer.write(serializer.toRow(key, sequenceNumber, valueKind, value));
    }

    /**
     * 获取缓冲区中存储的元素数量。
     */
    @Override
    public int size() {
        return buffer.size();
    }

    /**
     * 获取当前对象占用的内存大小。
     */
    @Override
    public long memoryOccupancy() {
        return buffer.getOccupancy();
    }

    /**
     * 将缓冲区中的数据刷新到外部存储（如磁盘）。
     */
    @Override
    public boolean flushMemory() throws IOException {
        return buffer.flushMemory();
    }

    @Override
    public void forEach(
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            @Nullable KvConsumer rawConsumer,
            KvConsumer mergedConsumer)
            throws IOException {
        MergeIterator mergeIterator =
                new MergeIterator(
                        rawConsumer, buffer.sortedIterator(), keyComparator, mergeFunction);
        while (mergeIterator.hasNext()) {
            mergedConsumer.accept(mergeIterator.next());
        }
    }

    @Override
    public void clear() {
        buffer.clear();
    }

    @VisibleForTesting
    SortBuffer buffer() {
        return buffer;
    }

    private class MergeIterator {
        @Nullable private final KvConsumer rawConsumer;
        private final MutableObjectIterator<BinaryRow> kvIter;
        private final Comparator<InternalRow> keyComparator;
        private final ReducerMergeFunctionWrapper mergeFunctionWrapper;

        // 两组变量，用于存储前后数据
        private KeyValueSerializer previous;
        private BinaryRow previousRow;

        private KeyValueSerializer current;
        private BinaryRow currentRow;

        private KeyValue result;
        private boolean advanced;

        public MergeIterator(
                @Nullable KvConsumer rawConsumer,
                MutableObjectIterator<BinaryRow> kvIter,
                Comparator<InternalRow> keyComparator,
                MergeFunction<KeyValue> mergeFunction)
                throws IOException {
            this.rawConsumer = rawConsumer;
            this.kvIter = kvIter;
            this.keyComparator = keyComparator;
            this.mergeFunctionWrapper = new ReducerMergeFunctionWrapper(mergeFunction);

            // 计算总字段数，包括键字段、序列字段和值字段
            int totalFieldCount = keyType.getFieldCount() + 2 + valueType.getFieldCount();
            this.previous = new KeyValueSerializer(keyType, valueType);
            this.previousRow = new BinaryRow(totalFieldCount);
            this.current = new KeyValueSerializer(keyType, valueType);
            this.currentRow = new BinaryRow(totalFieldCount);
            readOnce();
            this.advanced = false;
        }

        public boolean hasNext() throws IOException {
            advanceIfNeeded();
            return previousRow != null;
        }

        public KeyValue next() throws IOException {
            advanceIfNeeded();
            if (previousRow == null) {
                return null;
            }
            advanced = false;
            return result;
        }

        private void advanceIfNeeded() throws IOException {
            if (advanced) {
                return;
            }
            advanced = true;

            // 归并批量处理
            do {
                swapSerializers(); // 交换前后变量
                if (previousRow == null) {
                    return;
                }
                mergeFunctionWrapper.reset(); // 重置合并函数包装器
                mergeFunctionWrapper.add(previous.getReusedKv()); // 添加上一个键值对

                // 继续读取直到键不相同
                while (readOnce()) { // 读取下一个键值对
                    // 比较键值是否相同
                    if (keyComparator.compare(
                            previous.getReusedKv().key(), current.getReusedKv().key())
                            != 0) {
                        break;
                    }
                    mergeFunctionWrapper.add(current.getReusedKv()); // 合并当前键值对
                    swapSerializers(); // 交换前后变量
                }
                result = mergeFunctionWrapper.getResult(); // 获取合并结果
            } while (result == null);
        }

        private boolean readOnce() throws IOException {
            try {
                currentRow = kvIter.next(currentRow); // 获取下一行
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (currentRow != null) {
                current.fromRow(currentRow); // 将行转换为键值对
                if (rawConsumer != null) { // 如果有原始消费者
                    rawConsumer.accept(current.getReusedKv()); // 通知原始消费者
                }
            }
            return currentRow != null;
        }

        private void swapSerializers() {
            KeyValueSerializer tmp = previous;
            BinaryRow tmpRow = previousRow;
            previous = current;
            previousRow = currentRow;
            current = tmp;
            currentRow = tmpRow;
        }
    }
}