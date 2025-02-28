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

import org.apache.paimon.codegen.NormalizedKeyComputer;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.AbstractRowDataSerializer;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 二进制行的内存排序缓冲区。
 */
public class BinaryInMemorySortBuffer extends BinaryIndexedSortable implements SortBuffer {

    private static final int MIN_REQUIRED_BUFFERS = 3; // 最小需要的缓冲区数量

    private final AbstractRowDataSerializer<InternalRow> inputSerializer; // 输入数据的序列化器
    private final ArrayList<MemorySegment> recordBufferSegments; // 数据存储的内存段列表
    private final SimpleCollectingOutputView recordCollector; // 数据收集器

    private long currentDataBufferOffset; // 当前数据缓冲区的偏移量
    private long sortIndexBytes; // 排序索引占用的字节数
    private boolean isInitialized; // 是否已初始化

    /**
     * 创建一个以插入方式工作的内存排序缓冲区。
     */
    public static BinaryInMemorySortBuffer createBuffer(
            NormalizedKeyComputer normalizedKeyComputer, // 规范化键计算器
            AbstractRowDataSerializer<InternalRow> serializer, // 数据序列化器
            RecordComparator comparator, // 数据比较器
            MemorySegmentPool memoryPool // 内存池
    ) {
        checkArgument(memoryPool.freePages() >= MIN_REQUIRED_BUFFERS); // 检查内存池是否有足够的页
        ArrayList<MemorySegment> recordBufferSegments = new ArrayList<>(16); // 初始化数据存储的内存段列表
        return new BinaryInMemorySortBuffer(
                normalizedKeyComputer,
                serializer,
                comparator,
                recordBufferSegments,
                new SimpleCollectingOutputView(recordBufferSegments, memoryPool, memoryPool.pageSize()), // 初始化数据收集器
                memoryPool
        );
    }

    private BinaryInMemorySortBuffer(
            NormalizedKeyComputer normalizedKeyComputer,
            AbstractRowDataSerializer<InternalRow> inputSerializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            SimpleCollectingOutputView recordCollector,
            MemorySegmentPool pool
    ) {
        super(
                normalizedKeyComputer,
                new BinaryRowSerializer(inputSerializer.getArity()), // 初始化BinaryRow序列化器
                comparator,
                recordBufferSegments,
                pool
        );
        this.inputSerializer = inputSerializer; // 初始化输入序列化器
        this.recordBufferSegments = recordBufferSegments; // 初始化数据存储的内存段列表
        this.recordCollector = recordCollector; // 初始化数据收集器
        this.isInitialized = true; // 设置初始化状态为true
        this.clear(); // 清空缓冲区
    }

    // -------------------------------------------------------------------------//
    // Memory Segment管理
    // -------------------------------------------------------------------------//

    private void returnToSegmentPool() {
        // 将所有内存段返回到内存池
        this.memorySegmentPool.returnAll(this.sortIndex); // 返回排序索引的内存段
        this.memorySegmentPool.returnAll(this.recordBufferSegments); // 返回数据存储的内存段
        this.sortIndex.clear(); // 清空排序索引的内存段
        this.recordBufferSegments.clear(); // 清空数据存储的内存段
    }

    public int getBufferSegmentCount() {
        return this.recordBufferSegments.size(); // 返回数据存储的内存段数量
    }

    /**
     * 如果所有包含的数据都被丢弃，则尝试初始化排序缓冲区。
     */
    private void tryInitialize() {
        if (!isInitialized) {
            // 初始化排序索引和数据存储的内存段
            this.currentSortIndexSegment = nextMemorySegment(); // 获取下一个排序索引的内存段
            this.sortIndex.add(this.currentSortIndexSegment); // 将内存段添加到排序索引列表
            this.recordCollector.reset(); // 重置数据收集器
            this.isInitialized = true; // 设置初始化状态为true
        }
    }

    @Override
    public void clear() {
        if (this.isInitialized) {
            // 重置所有偏移量和状态
            this.numRecords = 0;
            this.currentSortIndexOffset = 0;
            this.currentDataBufferOffset = 0;
            this.sortIndexBytes = 0;

            returnToSegmentPool(); // 将内存段返回到内存池
            this.currentSortIndexSegment = null; // 重置当前排序索引的内存段
            this.isInitialized = false; // 设置初始化状态为false
        }
    }

    @Override
    public long getOccupancy() {
        return this.currentDataBufferOffset + this.sortIndexBytes; // 返回内存缓冲区的总占用大小
    }

    @Override
    public boolean flushMemory() {
        return false; // 外部溢出逻辑未实现，返回false
    }

    boolean isEmpty() {
        return this.numRecords == 0; // 返回缓冲区是否为空
    }

    /**
     * 将给定的记录写入此排序缓冲区。写入的记录将被追加并占据最后一个逻辑位置。
     */
    @Override
    public boolean write(InternalRow record) throws IOException {
        tryInitialize(); // 尝试初始化缓冲区

        if (!checkNextIndexOffset()) { // 检查是否需要新的内存段
            return false; // 无法继续写入
        }

        int skip;
        try {
            skip = this.inputSerializer.serializeToPages(record, this.recordCollector); // 序列化记录到内存段
        } catch (EOFException e) {
            return false; // 内存段已满，无法写入
        }

        final long newOffset = this.recordCollector.getCurrentOffset(); // 获取当前数据缓冲区的偏移量
        long currOffset = currentDataBufferOffset + skip; // 计算当前数据缓冲区中记录的实际偏移量
        writeIndexAndNormalizedKey(record, currOffset); // 将记录的偏移量和规范化键写入排序索引
        this.sortIndexBytes += this.indexEntrySize; // 更新排序索引的总字节大小
        this.currentDataBufferOffset = newOffset; // 更新当前数据缓冲区的偏移量
        return true; // 写入成功
    }

    private BinaryRow getRecordFromBuffer(BinaryRow reuse, long pointer) throws IOException {
        this.recordBuffer.setReadPosition(pointer); // 设置读取位置
        return this.serializer.mapFromPages(reuse, this.recordBuffer); // 从内存段读取记录
    }

    // -------------------------------------------------------------------------//

    /**
     * 获取此缓冲区中所有记录的逻辑顺序迭代器。
     */
    private MutableObjectIterator<BinaryRow> iterator() {
        tryInitialize(); // 尝试初始化缓冲区

        return new MutableObjectIterator<BinaryRow>() {
            private final int size = size(); // 获取总记录数
            private int current = 0; // 当前已读取记录数
            private int currentSegment = 0; // 当前排序索引段索引
            private int currentOffset = 0; // 当前排序索引段的偏移量
            private MemorySegment currentIndexSegment = sortIndex.get(0); // 当前排序索引段

            @Override
            public BinaryRow next(BinaryRow target) {
                if (this.current < this.size) {
                    this.current++; // 已读取记录数加一
                    if (this.currentOffset > lastIndexEntryOffset) {
                        this.currentOffset = 0; // 重置排序索引段的偏移量
                        this.currentIndexSegment = sortIndex.get(++this.currentSegment); // 获取下一个排序索引段
                    }

                    long pointer = this.currentIndexSegment.getLong(this.currentOffset); // 获取记录的偏移量
                    this.currentOffset += indexEntrySize; // 更新排序索引段的偏移量

                    try {
                        return getRecordFromBuffer(target, pointer); // 从缓冲区读取记录
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe); // 将异常包装为运行时异常
                    }
                } else {
                    return null; // 已无记录可读取
                }
            }

            @Override
            public BinaryRow next() {
                throw new RuntimeException("Not support!"); // 不支持直接next方法
            }
        };
    }

    @Override
    public final MutableObjectIterator<BinaryRow> sortedIterator() {
        if (numRecords > 0) {
            new QuickSort().sort(this); // 对缓冲区中的数据进行排序
        }
        return iterator(); // 返回排序后的迭代器
    }
}
