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
 * 抽象的可排序类，提供基本的比较和交换功能。支持索引和规范化键的写入。
 */
public abstract class BinaryIndexedSortable implements IndexedSortable {
    // 定义偏移量存储的长度，通常为8字节（long类型）
    public static final int OFFSET_LEN = 8;

    // 用于计算规范化键的组件
    private final NormalizedKeyComputer normalizedKeyComputer;
    // 序列化和反序列化BinaryRow对象的工具
    protected final BinaryRowSerializer serializer;
    // 用于比较数据记录的比较器
    private final RecordComparator comparator;

    // 记录数据的随机访问输入视图
    protected final RandomAccessInputView recordBuffer;
    // 用于比较记录的随机访问输入视图
    private final RandomAccessInputView recordBufferForComparison;

    // 当前用于存储排序索引的内存段
    protected MemorySegment currentSortIndexSegment;
    // 内存段池，用于动态分配和回收内存
    protected final MemorySegmentPool memorySegmentPool;
    // 存储排序索引的内存段列表
    protected final ArrayList<MemorySegment> sortIndex;

    // 规范化键的字节数
    private final int numKeyBytes;
    // 每个索引条目占用的总字节数
    protected final int indexEntrySize;
    // 每个内存段中能容纳的索引条目数
    private final int indexEntriesPerSegment;
    // 最后一个索引条目在内存段中的偏移量
    protected final int lastIndexEntryOffset;
    // 标记规范化键是否完全决定排序顺序
    private final boolean normalizedKeyFullyDetermines;
    // 标记是否未反转规范化键
    private final boolean useNormKeyUninverted;

    // 用于序列化比较的工具
    protected final BinaryRowSerializer serializer1;
    private final BinaryRowSerializer serializer2;
    // 用于存储比较过程中的行数据
    protected final BinaryRow row1;
    private final BinaryRow row2;

    // 当前排序索引的偏移量
    protected int currentSortIndexOffset;
    // 已处理的记录数
    protected int numRecords;

    // 构造方法，初始化各项成员变量
    public BinaryIndexedSortable(
            NormalizedKeyComputer normalizedKeyComputer,
            BinaryRowSerializer serializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            MemorySegmentPool memorySegmentPool) {
        if (normalizedKeyComputer == null || serializer == null) {
            throw new NullPointerException(); // 输入参数不能为空
        }
        this.normalizedKeyComputer = normalizedKeyComputer;
        this.serializer = serializer;
        this.comparator = comparator;
        this.memorySegmentPool = memorySegmentPool;

        this.useNormKeyUninverted = !normalizedKeyComputer.invertKey(); // 判断规范化键是否未反转

        this.numKeyBytes = normalizedKeyComputer.getNumKeyBytes(); // 获取规范化键的字节数

        int segmentSize = memorySegmentPool.pageSize(); // 获取内存段大小
        this.recordBuffer = new RandomAccessInputView(recordBufferSegments, segmentSize); // 初始化记录输入视图
        this.recordBufferForComparison = new RandomAccessInputView(recordBufferSegments, segmentSize); // 初始化比较用记录输入视图

        this.normalizedKeyFullyDetermines = normalizedKeyComputer.isKeyFullyDetermines(); // 判断规范化键是否完全决定排序顺序

        // 计算索引条目大小和限制
        this.indexEntrySize = numKeyBytes + OFFSET_LEN; // 每个索引条目包括规范化键和偏移量
        this.indexEntriesPerSegment = segmentSize / this.indexEntrySize; // 每个内存段可存储的索引条目数
        this.lastIndexEntryOffset = (this.indexEntriesPerSegment - 1) * this.indexEntrySize; // 最后一个索引条目的偏移量

        // 初始化序列化工具和行数据对象
        this.serializer1 = serializer.duplicate();
        this.serializer2 = serializer.duplicate();
        this.row1 = this.serializer1.createInstance();
        this.row2 = this.serializer2.createInstance();

        // 初始化状态
        this.sortIndex = new ArrayList<>(16); // 初始化排序索引内存段列表
        this.currentSortIndexSegment = nextMemorySegment(); // 获取第一个内存段
        sortIndex.add(currentSortIndexSegment); // 添加到排序索引列表
    }

    // 从内存段池中获取下一个内存段
    protected MemorySegment nextMemorySegment() {
        return this.memorySegmentPool.nextSegment();
    }

    /**
     * 检查是否需要请求下一个索引内存。
     */
    protected boolean checkNextIndexOffset() {
        // 如果当前的排序索引偏移量超过了最后一个索引条目的偏移量
        if (this.currentSortIndexOffset > this.lastIndexEntryOffset) {
            // 请求下一个内存段
            MemorySegment returnSegment = nextMemorySegment();
            // 如果获取到新的内存段
            if (returnSegment != null) {
                this.currentSortIndexSegment = returnSegment; // 更新当前的内存段
                this.sortIndex.add(this.currentSortIndexSegment); // 添加到排序索引列表
                this.currentSortIndexOffset = 0; // 重置当前偏移量为0
            } else {
                return false; // 无法获取新内存段，返回失败
            }
        }
        return true; // 检查通过，无需请求新内存段
    }

    /**
     * 写入索引和规范化键。
     */
    protected void writeIndexAndNormalizedKey(InternalRow record, long currOffset) {
        // 将记录的偏移量写入当前内存段的指定位置
        this.currentSortIndexSegment.putLong(this.currentSortIndexOffset, currOffset);
        // 写入规范化键
        if (this.numKeyBytes != 0) {
            normalizedKeyComputer.putKey(
                    record,
                    this.currentSortIndexSegment,
                    this.currentSortIndexOffset + OFFSET_LEN
            );
        }
        // 更新当前的偏移量，为下一个记录腾出空间
        this.currentSortIndexOffset += this.indexEntrySize;
        // 增加记录数
        this.numRecords++;
    }

    @Override
    public int compare(int i, int j) {
        // 根据索引i和j的逻辑位置，计算对应的内存段和偏移量
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        return compare(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    @Override
    public int compare(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        // 获取对应的内存段
        final MemorySegment segI = this.sortIndex.get(segmentNumberI);
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ);

        // 比较两个索引条目的规范化键
        int val = normalizedKeyComputer.compareKey(
                segI,
                segmentOffsetI + OFFSET_LEN, // 跳过偏移量部分
                segJ,
                segmentOffsetJ + OFFSET_LEN
        );

        // 如果规范化键可以完全决定排序顺序，直接返回比较结果
        if (val != 0 || this.normalizedKeyFullyDetermines) {
            return this.useNormKeyUninverted ? val : -val; // 根据是否反转键值，调整比较结果
        }

        // 否则，使用记录比较器比较原始数据
        final long pointerI = segI.getLong(segmentOffsetI); // 获取记录i的偏移量
        final long pointerJ = segJ.getLong(segmentOffsetJ); // 获取记录j的偏移量

        return compareRecords(pointerI, pointerJ);
    }

    private int compareRecords(long pointer1, long pointer2) {
        // 设置输入视图的读取位置为对应偏移量
        this.recordBuffer.setReadPosition(pointer1);
        this.recordBufferForComparison.setReadPosition(pointer2);

        try {
            // 比较两个记录，并返回结果
            return this.comparator.compare(
                    serializer1.mapFromPages(row1, recordBuffer), // 映射记录1
                    serializer2.mapFromPages(row2, recordBufferForComparison) // 映射记录2
            );
        } catch (IOException ioex) {
            throw new RuntimeException("Error comparing two records.", ioex); // 比较失败时抛出异常
        }
    }

    @Override
    public void swap(int i, int j) {
        // 根据索引i和j的逻辑位置，计算对应的内存段和偏移量
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        swap(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    @Override
    public void swap(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        // 获取对应的内存段
        final MemorySegment segI = this.sortIndex.get(segmentNumberI);
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ);

        // 交换偏移量
        long index = segI.getLong(segmentOffsetI);
        segI.putLong(segmentOffsetI, segJ.getLong(segmentOffsetJ));
        segJ.putLong(segmentOffsetJ, index);

        // 交换规范化键
        normalizedKeyComputer.swapKey(
                segI, segmentOffsetI + OFFSET_LEN, segJ, segmentOffsetJ + OFFSET_LEN
        );
    }

    @Override
    public int size() {
        return this.numRecords; // 返回记录总数
    }

    @Override
    public int recordSize() {
        return this.indexEntrySize; // 返回每个记录条目的大小
    }

    @Override
    public int recordsPerSegment() {
        return this.indexEntriesPerSegment; // 返回每个内存段可存储的记录条目数
    }

    /**
     * 将所有记录写入到输出视图中。
     */
    public void writeToOutput(AbstractPagedOutputView output) throws IOException {
        final int numRecords = this.numRecords;
        int currentMemSeg = 0; // 当前处理的内存段索引
        int currentRecord = 0; // 当前处理的记录索引

        // 遍历所有内存段，将记录写入输出视图
        while (currentRecord < numRecords) {
            final MemorySegment currentIndexSegment = this.sortIndex.get(currentMemSeg++); // 获取当前内存段

            // 遍历内存段中的所有记录条目
            for (int offset = 0;
                 currentRecord < numRecords && offset <= this.lastIndexEntryOffset;
                 currentRecord++, offset += this.indexEntrySize) {
                final long pointer = currentIndexSegment.getLong(offset); // 获取记录的偏移量
                this.recordBuffer.setReadPosition(pointer); // 设置输入视图的读取位置
                this.serializer.copyFromPagesToView(this.recordBuffer, output); // 将记录写入输出视图
            }
        }
    }
}
