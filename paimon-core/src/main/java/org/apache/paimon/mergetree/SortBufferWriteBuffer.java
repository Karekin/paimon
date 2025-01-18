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

/** A {@link WriteBuffer} which stores records in {@link BinaryInMemorySortBuffer}. */
/**
* @授课老师: 码界探索
* @微信: 252810631
* @版权所有: 请尊重劳动成果
* 实现了WriteBuffer接口的类，用于在BinaryInMemorySortBuffer 中存储记录。
*/
public class SortBufferWriteBuffer implements WriteBuffer {

    private final RowType keyType;// 键的类型
    private final RowType valueType;// 值的类型
    private final KeyValueSerializer serializer;// 键值序列化器
    // 排序缓冲区，可以是内存中的，也可以是结合了磁盘的
    private final SortBuffer buffer;

    /**
     * 构造函数，用于初始化SortBufferWriteBuffer。
     *
     * @param keyType            键的类型
     * @param valueType          值的类型
     * @param userDefinedSeqComparator 用户定义的序列比较器，可能为空
     * @param memoryPool         内存池，用于分配内存段
     * @param spillable          是否允许缓冲区溢出到磁盘
     * @param maxDiskSize        最大磁盘使用大小
     * @param sortMaxFan         排序时的最大扇出数
     * @param compression        排序压缩方式
     * @param ioManager          IO管理器，用于处理磁盘IO操作
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

        //初始化排序字段，首先是键的所有字段
        IntStream sortFields = IntStream.range(0, keyType.getFieldCount());

        //// 如果有用户定义的序列比较器，则添加其比较字段
        if (userDefinedSeqComparator != null) {
            IntStream udsFields =
                    IntStream.of(userDefinedSeqComparator.compareFields())
                            .map(operand -> operand + keyType.getFieldCount() + 2);
            sortFields = IntStream.concat(sortFields, udsFields);
        }

        // 添加一个序列字段作为排序的最后一个字段
        sortFields = IntStream.concat(sortFields, IntStream.of(keyType.getFieldCount()));
        // 将排序字段转换为数组
        int[] sortFieldArray = sortFields.toArray();

        // row type
        // 构造排序所需的字段类型列表，包括键的类型、一个BigIntType（可能用于序列字段）和一个TinyIntType（可能用于其他目的），以及值的类型
        List<DataType> fieldTypes = new ArrayList<>(keyType.getFieldTypes());
        fieldTypes.add(new BigIntType(false));
        fieldTypes.add(new TinyIntType(false));
        fieldTypes.addAll(valueType.getFieldTypes());

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
                                new BinaryRowSerializer(serializer.getArity()),
                                keyComparator,
                                memoryPool.pageSize(),
                                inMemorySortBuffer,
                                ioManager,
                                sortMaxFan,
                                compression,
                                maxDiskSize)
                        : inMemorySortBuffer;
    }

    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
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
        // 使用序列化器将键、序列号、值类型和值组合成一个新的行，然后写入缓冲区。
        return buffer.write(serializer.toRow(key, sequenceNumber, valueKind, value));
    }

    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    *  获取缓冲区中存储的元素数量。
    */
    @Override
    public int size() {
        return buffer.size();
    }

    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 获取当前对象占用的内存大小。
    */
    @Override
    public long memoryOccupancy() {
        return buffer.getOccupancy();
    }

    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
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
        // TODO do not use iterator
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

        // previously read kv
        private KeyValueSerializer previous;
        private BinaryRow previousRow;
        // reads the next kv
        private KeyValueSerializer current;
        private BinaryRow currentRow;

        private KeyValue result;
        private boolean advanced;

        private MergeIterator(
                @Nullable KvConsumer rawConsumer,
                MutableObjectIterator<BinaryRow> kvIter,
                Comparator<InternalRow> keyComparator,
                MergeFunction<KeyValue> mergeFunction)
                throws IOException {
            this.rawConsumer = rawConsumer;
            this.kvIter = kvIter;
            this.keyComparator = keyComparator;
            this.mergeFunctionWrapper = new ReducerMergeFunctionWrapper(mergeFunction);

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

            do {
                swapSerializers();
                if (previousRow == null) {
                    return;
                }
                mergeFunctionWrapper.reset();
                mergeFunctionWrapper.add(previous.getReusedKv());

                while (readOnce()) {
                    if (keyComparator.compare(
                                    previous.getReusedKv().key(), current.getReusedKv().key())
                            != 0) {
                        break;
                    }
                    mergeFunctionWrapper.add(current.getReusedKv());
                    swapSerializers();
                }
                result = mergeFunctionWrapper.getResult();
            } while (result == null);
        }

        private boolean readOnce() throws IOException {
            try {
                currentRow = kvIter.next(currentRow);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (currentRow != null) {
                current.fromRow(currentRow);
                if (rawConsumer != null) {
                    rawConsumer.accept(current.getReusedKv());
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
