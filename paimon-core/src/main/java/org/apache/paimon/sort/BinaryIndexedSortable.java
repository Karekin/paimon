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
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;

import java.io.IOException;
import java.util.ArrayList;

/**
 * An abstract sortable, provide basic compare and swap. Support writing of index and normalizedKey.
 */
/**
* @授课老师: 码界探索
* @微信: 252810631
* @版权所有: 请尊重劳动成果
* 抽象的可排序类，提供基本的比较和交换功能。支持索引和规范化键的写入。
*/
public abstract class BinaryIndexedSortable implements IndexedSortable {
    // 常量，表示偏移量的长度，通常为8字节（用于存储索引或偏移量）。
    public static final int OFFSET_LEN = 8;

    // put/compare/swap normalized key
    private final NormalizedKeyComputer normalizedKeyComputer;
    // 用于序列化和反序列化BinaryRow的序列化器。
    protected final BinaryRowSerializer serializer;

    // if normalized key not fully determines, need compare record.
    private final RecordComparator comparator;

    protected final RandomAccessInputView recordBuffer;
    private final RandomAccessInputView recordBufferForComparison;

    // segments
    // 当前用于存储排序索引的内存段。
    protected MemorySegment currentSortIndexSegment;
    // 内存段池，用于分配和回收内存段。
    protected final MemorySegmentPool memorySegmentPool;
    protected final ArrayList<MemorySegment> sortIndex;

    // normalized key attributes
    private final int numKeyBytes;
    protected final int indexEntrySize;
    private final int indexEntriesPerSegment;
    protected final int lastIndexEntryOffset;
    private final boolean normalizedKeyFullyDetermines;
    private final boolean useNormKeyUninverted;

    // for serialized comparison
    protected final BinaryRowSerializer serializer1;
    private final BinaryRowSerializer serializer2;
    protected final BinaryRow row1;
    private final BinaryRow row2;

    // runtime variables
    protected int currentSortIndexOffset;
    protected int numRecords;

    public BinaryIndexedSortable(
            NormalizedKeyComputer normalizedKeyComputer,
            BinaryRowSerializer serializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            MemorySegmentPool memorySegmentPool) {
        if (normalizedKeyComputer == null || serializer == null) {
            throw new NullPointerException();
        }
        this.normalizedKeyComputer = normalizedKeyComputer;
        this.serializer = serializer;
        this.comparator = comparator;
        this.memorySegmentPool = memorySegmentPool;
        this.useNormKeyUninverted = !normalizedKeyComputer.invertKey();

        this.numKeyBytes = normalizedKeyComputer.getNumKeyBytes();

        int segmentSize = memorySegmentPool.pageSize();
        this.recordBuffer = new RandomAccessInputView(recordBufferSegments, segmentSize);
        this.recordBufferForComparison =
                new RandomAccessInputView(recordBufferSegments, segmentSize);

        this.normalizedKeyFullyDetermines = normalizedKeyComputer.isKeyFullyDetermines();

        // compute the index entry size and limits
        this.indexEntrySize = numKeyBytes + OFFSET_LEN;
        this.indexEntriesPerSegment = segmentSize / this.indexEntrySize;
        this.lastIndexEntryOffset = (this.indexEntriesPerSegment - 1) * this.indexEntrySize;

        this.serializer1 = serializer.duplicate();
        this.serializer2 = serializer.duplicate();
        this.row1 = this.serializer1.createInstance();
        this.row2 = this.serializer2.createInstance();

        // set to initial state
        this.sortIndex = new ArrayList<>(16);
        this.currentSortIndexSegment = nextMemorySegment();
        sortIndex.add(currentSortIndexSegment);
    }

    protected MemorySegment nextMemorySegment() {
        return this.memorySegmentPool.nextSegment();
    }

    /** check if we need request next index memory. */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 检查是否需要请求下一个索引内存。
    */
    protected boolean checkNextIndexOffset() {
        // 如果当前的排序索引偏移量大于最后一个索引条目的偏移量
        if (this.currentSortIndexOffset > this.lastIndexEntryOffset) {
            // 请求下一个内存段
            MemorySegment returnSegment = nextMemorySegment();
            // 如果成功获取到了新的内存段
            if (returnSegment != null) {
                // 更新当前的排序索引内存段为新的内存段
                this.currentSortIndexSegment = returnSegment;
                // 将新的内存段添加到排序索引的内存段列表中
                this.sortIndex.add(this.currentSortIndexSegment);
                // 重置当前的排序索引偏移量为0，因为新的开始点在新内存段
                this.currentSortIndexOffset = 0;
            } else {
                // 如果没有获取到新的内存段，返回false表示无法继续
                return false;
            }
        }
        return true;
    }

    /** Write of index and normalizedKey. */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    *  写入索引和
    */
    protected void writeIndexAndNormalizedKey(InternalRow record, long currOffset) {
        // add the pointer and the normalized key
        //在currentSortIndexSegment记录这条数据在OutputView的位置（Long8个字节）
        this.currentSortIndexSegment.putLong(this.currentSortIndexOffset, currOffset);
        // 如果字节数不为0（即normalized）
        if (this.numKeyBytes != 0) {
            //将数据对应的Key写入到urrentSortIndexSegment
            normalizedKeyComputer.putKey(
                    record, this.currentSortIndexSegment, this.currentSortIndexOffset + OFFSET_LEN);
        }
        // 更新当前索引偏移量，为下一个索引条目腾出空间
        this.currentSortIndexOffset += this.indexEntrySize;
        // 增加记录计数器，表示已经处理了多少条记录
        this.numRecords++;
    }

    @Override
    public int compare(int i, int j) {
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        return compare(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    @Override
    public int compare(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortIndex.get(segmentNumberI);
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ);

        int val =
                normalizedKeyComputer.compareKey(
                        segI, segmentOffsetI + OFFSET_LEN, segJ, segmentOffsetJ + OFFSET_LEN);

        if (val != 0 || this.normalizedKeyFullyDetermines) {
            return this.useNormKeyUninverted ? val : -val;
        }

        final long pointerI = segI.getLong(segmentOffsetI);
        final long pointerJ = segJ.getLong(segmentOffsetJ);

        return compareRecords(pointerI, pointerJ);
    }

    private int compareRecords(long pointer1, long pointer2) {
        this.recordBuffer.setReadPosition(pointer1);
        this.recordBufferForComparison.setReadPosition(pointer2);

        try {
            return this.comparator.compare(
                    serializer1.mapFromPages(row1, recordBuffer),
                    serializer2.mapFromPages(row2, recordBufferForComparison));
        } catch (IOException ioex) {
            throw new RuntimeException("Error comparing two records.", ioex);
        }
    }

    @Override
    public void swap(int i, int j) {
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        swap(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    @Override
    public void swap(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortIndex.get(segmentNumberI);
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ);

        // swap offset
        long index = segI.getLong(segmentOffsetI);
        segI.putLong(segmentOffsetI, segJ.getLong(segmentOffsetJ));
        segJ.putLong(segmentOffsetJ, index);

        // swap key
        normalizedKeyComputer.swapKey(
                segI, segmentOffsetI + OFFSET_LEN, segJ, segmentOffsetJ + OFFSET_LEN);
    }

    @Override
    public int size() {
        return this.numRecords;
    }

    @Override
    public int recordSize() {
        return indexEntrySize;
    }

    @Override
    public int recordsPerSegment() {
        return indexEntriesPerSegment;
    }

    /** Spill: Write all records to a {@link AbstractPagedOutputView}. */
    public void writeToOutput(AbstractPagedOutputView output) throws IOException {
        final int numRecords = this.numRecords;
        int currentMemSeg = 0;
        int currentRecord = 0;

        while (currentRecord < numRecords) {
            final MemorySegment currentIndexSegment = this.sortIndex.get(currentMemSeg++);

            // go through all records in the memory segment
            for (int offset = 0;
                    currentRecord < numRecords && offset <= this.lastIndexEntryOffset;
                    currentRecord++, offset += this.indexEntrySize) {
                final long pointer = currentIndexSegment.getLong(offset);
                this.recordBuffer.setReadPosition(pointer);
                this.serializer.copyFromPagesToView(this.recordBuffer, output);
            }
        }
    }
}
