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
 * In memory sort buffer for binary row.
 *
 * <ul>
 *   <li>{@link #clear}: Clean all memory.
 *   <li>{@link #tryInitialize}: initialize memory before write and read in buffer.
 * </ul>
 */
/**
* @授课老师: 码界探索
* @微信: 252810631
* @版权所有: 请尊重劳动成果
* 二进制行的内存排序缓冲区。
*/
public class BinaryInMemorySortBuffer extends BinaryIndexedSortable implements SortBuffer {

    private static final int MIN_REQUIRED_BUFFERS = 3;

    private final AbstractRowDataSerializer<InternalRow> inputSerializer;
    private final ArrayList<MemorySegment> recordBufferSegments;
    private final SimpleCollectingOutputView recordCollector;

    private long currentDataBufferOffset;
    private long sortIndexBytes;
    private boolean isInitialized;

    /** Create a memory sorter in `insert` way. */
    public static BinaryInMemorySortBuffer createBuffer(
            NormalizedKeyComputer normalizedKeyComputer,
            AbstractRowDataSerializer<InternalRow> serializer,
            RecordComparator comparator,
            MemorySegmentPool memoryPool) {
        checkArgument(memoryPool.freePages() >= MIN_REQUIRED_BUFFERS);
        ArrayList<MemorySegment> recordBufferSegments = new ArrayList<>(16);
        return new BinaryInMemorySortBuffer(
                normalizedKeyComputer,
                serializer,
                comparator,
                recordBufferSegments,
                new SimpleCollectingOutputView(
                        recordBufferSegments, memoryPool, memoryPool.pageSize()),
                memoryPool);
    }

    private BinaryInMemorySortBuffer(
            NormalizedKeyComputer normalizedKeyComputer,
            AbstractRowDataSerializer<InternalRow> inputSerializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            SimpleCollectingOutputView recordCollector,
            MemorySegmentPool pool) {
        super(
                normalizedKeyComputer,
                new BinaryRowSerializer(inputSerializer.getArity()),
                comparator,
                recordBufferSegments,
                pool);
        this.inputSerializer = inputSerializer;
        this.recordBufferSegments = recordBufferSegments;
        this.recordCollector = recordCollector;
        // The memory will be initialized in super()
        this.isInitialized = true;
        this.clear();
    }

    // -------------------------------------------------------------------------
    // Memory Segment
    // -------------------------------------------------------------------------

    private void returnToSegmentPool() {
        // return all memory
        this.memorySegmentPool.returnAll(this.sortIndex);
        this.memorySegmentPool.returnAll(this.recordBufferSegments);
        this.sortIndex.clear();
        this.recordBufferSegments.clear();
    }

    public int getBufferSegmentCount() {
        return this.recordBufferSegments.size();
    }

    /** Try to initialize the sort buffer if all contained data is discarded. */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 如果所有包含的数据都被丢弃，则尝试初始化排序缓冲区。
    */
    private void tryInitialize() {
        // 如果当前对象或组件尚未初始化
        if (!isInitialized) {
            // grab first buffer
            // 分配并获取第一个内存段，用作当前的排序索引内存段
            this.currentSortIndexSegment = nextMemorySegment();
            // 将当前获取的排序索引内存段添加到排序索引的内存段列表中
            this.sortIndex.add(this.currentSortIndexSegment);
            // grab second buffer
            // 为记录收集器分配或重置第二个内存段（或数据结构）
            this.recordCollector.reset();
            // 将初始化状态标记为true，表示当前对象或组件已经完成了初始化
            this.isInitialized = true;
        }
    }

    @Override
    public void clear() {
        if (this.isInitialized) {
            // reset all offsets
            this.numRecords = 0;
            this.currentSortIndexOffset = 0;
            this.currentDataBufferOffset = 0;
            this.sortIndexBytes = 0;

            // return all memory
            returnToSegmentPool();
            this.currentSortIndexSegment = null;
            this.isInitialized = false;
        }
    }

    @Override
    public long getOccupancy() {
        return this.currentDataBufferOffset + this.sortIndexBytes;
    }

    @Override
    public boolean flushMemory() {
        return false;
    }

    boolean isEmpty() {
        return this.numRecords == 0;
    }

    /**
     * Writes a given record to this sort buffer. The written record will be appended and take the
     * last logical position.
     *
     * @param record The record to be written.
     * @return True, if the record was successfully written, false, if the sort buffer was full.
     * @throws IOException Thrown, if an error occurred while serializing the record into the
     *     buffers.
     */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    *  将给定的记录写入此排序缓冲区。写入的记录将被追加并占据最后一个逻辑位置。
    */
    @Override
    public boolean write(InternalRow record) throws IOException {
        // 尝试初始化排序缓冲区，如果尚未初始化。
        tryInitialize();

        // check whether we need a new memory segment for the sort index
        // 检查是否需要为排序索引创建一个新的内存段。
        // 如果当前内存段没有足够的空间或者已经达到某个条件（如段大小限制），
        // 则返回false，表示无法继续写入更多记录。
        if (!checkNextIndexOffset()) {
            return false;
        }

        // serialize the record into the data buffers
        // 序列化记录到数据缓冲区中。
        int skip;
        try {
            // 将来record通过BinaryRowSerializer序列化到MemorySegment中写入内存，并返回写入的字节数。
            skip = this.inputSerializer.serializeToPages(record, this.recordCollector);
        } catch (EOFException e) {
            return false;
        }
        // 写完记录后，获取记录收集器当前的总偏移量（即新写入的记录之后的偏移量）。
        final long newOffset = this.recordCollector.getCurrentOffset();
        // 计算当前数据缓冲区中记录的实际偏移量
        long currOffset = currentDataBufferOffset + skip;
        // 将record对应的偏移量记录到Index MemorySegment。
        writeIndexAndNormalizedKey(record, currOffset);
        // 更新排序索引的总字节大小
        this.sortIndexBytes += this.indexEntrySize;
        // 更新当前数据缓冲区的偏移量，为下一个记录做准备。
        this.currentDataBufferOffset = newOffset;
        // 返回true，表示记录已成功写入。
        return true;
    }

    private BinaryRow getRecordFromBuffer(BinaryRow reuse, long pointer) throws IOException {
        this.recordBuffer.setReadPosition(pointer);
        return this.serializer.mapFromPages(reuse, this.recordBuffer);
    }

    // -------------------------------------------------------------------------

    /**
     * Gets an iterator over all records in this buffer in their logical order.
     *
     * @return An iterator returning the records in their logical order.
     */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 获取此缓冲区中所有记录的逻辑顺序迭代器。
    */
    private MutableObjectIterator<BinaryRow> iterator() {
        tryInitialize();

        return new MutableObjectIterator<BinaryRow>() {
            private final int size = size();
            private int current = 0;

            private int currentSegment = 0;
            private int currentOffset = 0;

            private MemorySegment currentIndexSegment = sortIndex.get(0);

            @Override
            public BinaryRow next(BinaryRow target) {
                if (this.current < this.size) {
                    this.current++;
                    if (this.currentOffset > lastIndexEntryOffset) {
                        this.currentOffset = 0;
                        this.currentIndexSegment = sortIndex.get(++this.currentSegment);
                    }

                    long pointer = this.currentIndexSegment.getLong(this.currentOffset);
                    this.currentOffset += indexEntrySize;

                    try {
                        return getRecordFromBuffer(target, pointer);
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                } else {
                    return null;
                }
            }

            @Override
            public BinaryRow next() {
                throw new RuntimeException("Not support!");
            }
        };
    }

    @Override
    public final MutableObjectIterator<BinaryRow> sortedIterator() {
        if (numRecords > 0) {
            new QuickSort().sort(this);
        }
        return iterator();
    }
}
