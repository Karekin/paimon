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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.memory.MemorySegmentWritable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * BinaryRow的序列化器。
 */
public class BinaryRowSerializer extends AbstractRowDataSerializer<BinaryRow> {

    private static final long serialVersionUID = 1L;
    public static final int LENGTH_SIZE_IN_BYTES = 4; // 记录长度的大小（4字节）

    private final int numFields; // 字段数量
    private final int fixedLengthPartSize; // 二进制行的固定长度部分大小（字节数）

    public BinaryRowSerializer(int numFields) {
        this.numFields = numFields; // 初始化字段数量
        // 计算二进制行的固定长度部分大小（字节数）
        this.fixedLengthPartSize = BinaryRow.calculateFixPartSizeInBytes(numFields);
    }

    @Override
    public BinaryRowSerializer duplicate() {
        return new BinaryRowSerializer(numFields); // 创建序列化器的副本
    }

    public BinaryRow createInstance() {
        return new BinaryRow(numFields); // 创建BinaryRow实例
    }

    @Override
    public BinaryRow copy(BinaryRow from) {
        return from.copy(); // 拷贝BinaryRow对象
    }

    @Override
    public void serialize(BinaryRow record, DataOutputView target) throws IOException {
        // 写入记录的长度
        target.writeInt(record.getSizeInBytes());
        // 判断目标输出是否支持内存段写入
        if (target instanceof MemorySegmentWritable) {
            // 直接写入内存段
            serializeWithoutLength(record, (MemorySegmentWritable) target);
        } else {
            // 使用MemorySegmentUtils将记录内容复制到输出流
            MemorySegmentUtils.copyToView(
                    record.getSegments(), record.getOffset(), record.getSizeInBytes(), target);
        }
    }

    /**
     * 从输入流中反序列化BinaryRow对象。
     */
    @Override
    public BinaryRow deserialize(DataInputView source) throws IOException {
        BinaryRow row = new BinaryRow(numFields); // 创建BinaryRow实例
        int length = source.readInt(); // 读取记录长度
        byte[] bytes = new byte[length]; // 创建字节数组
        source.readFully(bytes); // 读取数据
        row.pointTo(MemorySegment.wrap(bytes), 0, length); // 指向数据
        return row; // 返回反序列化后的对象
    }

    /**
     * 使用可复用的BinaryRow对象进行反序列化。
     */
    public BinaryRow deserialize(BinaryRow reuse, DataInputView source) throws IOException {
        // 检查可复用对象的内存段是否为空或符合要求
        MemorySegment[] segments = reuse.getSegments();
        checkArgument(
                segments == null || (segments.length == 1 && reuse.getOffset() == 0),
                "可复用的BinaryRow应没有内存段或只有一个内存段且偏移量为0。");

        int length = source.readInt(); // 读取记录长度
        // 如果复用对象的内存段大小不足，重新分配内存段
        if (segments == null || segments[0].size() < length) {
            segments = new MemorySegment[] {MemorySegment.wrap(new byte[length])};
        }
        // 读取数据到内存段
        source.readFully(segments[0].getArray(), 0, length);
        reuse.pointTo(segments, 0, length); // 更新复用对象的指向
        return reuse; // 返回反序列化后的对象
    }

    @Override
    public int getArity() {
        return numFields; // 返回字段数量
    }

    @Override
    public BinaryRow toBinaryRow(BinaryRow rowData) throws IOException {
        return rowData; // 返回直接二进制行对象
    }

    // ============================ Page相关操作 ===================================

    /**
     * 将二进制行记录序列化到分页输出视图中。
     */
    @Override
    public int serializeToPages(BinaryRow record, AbstractPagedOutputView headerLessView)
            throws IOException {
        // 检查是否因为空间不足而跳过当前段
        int skip = checkSkipWriteForFixLengthPart(headerLessView);
        // 写入二进制行记录的总字节大小
        headerLessView.writeInt(record.getSizeInBytes());
        // 序列化二进制行记录的内容
        serializeWithoutLength(record, headerLessView);
        return skip; // 返回跳过的字节数
    }

    /**
     * 将二进制行记录序列化到分页输出视图中，不包含长度信息。
     */
    private static void serializeWithoutLength(BinaryRow record, MemorySegmentWritable writable)
            throws IOException {
        if (record.getSegments().length == 1) {
            // 如果记录只占用一个内存段，直接写入
            writable.write(record.getSegments()[0], record.getOffset(), record.getSizeInBytes());
        } else {
            // 调用慢速方法处理多个内存段的情况
            serializeWithoutLengthSlow(record, writable);
        }
    }

    /**
     * 处理多个内存段的二进制行记录序列化。
     */
    public static void serializeWithoutLengthSlow(BinaryRow record, MemorySegmentWritable out)
            throws IOException {
        int remainSize = record.getSizeInBytes(); // 剩余写入的大小
        int posInSegOfRecord = record.getOffset(); // 当前内存段中的偏移量
        int segmentSize = record.getSegments()[0].size(); // 内存段大小
        for (MemorySegment segOfRecord : record.getSegments()) {
            // 计算当前内存段中可写入的最大数据量
            int nWrite = Math.min(segmentSize - posInSegOfRecord, remainSize);
            assert nWrite > 0;
            out.write(segOfRecord, posInSegOfRecord, nWrite); // 写入数据

            // 准备写入下一个内存段
            posInSegOfRecord = 0;
            remainSize -= nWrite;
            if (remainSize == 0) {
                break; // 已完成写入
            }
        }
        checkArgument(remainSize == 0);
    }

    /**
     * 从分页输入视图中反序列化二进制行记录。
     */
    @Override
    public BinaryRow deserializeFromPages(AbstractPagedInputView headerLessView)
            throws IOException {
        return deserializeFromPages(new BinaryRow(getArity()), headerLessView);
    }

    /**
     * 使用可复用的BinaryRow对象从分页输入视图中反序列化二进制行记录。
     */
    @Override
    public BinaryRow deserializeFromPages(BinaryRow reuse, AbstractPagedInputView headerLessView)
            throws IOException {
        checkSkipReadForFixLengthPart(headerLessView); // 检查是否跳过读取固定部分
        return deserialize(reuse, headerLessView); // 反序列化
    }

    /**
     * 将分页输入视图中的二进制行记录映射到可复用的BinaryRow对象。
     */
    @Override
    public BinaryRow mapFromPages(BinaryRow reuse, AbstractPagedInputView headerLessView)
            throws IOException {
        checkSkipReadForFixLengthPart(headerLessView); // 检查是否跳过读取固定部分
        pointTo(headerLessView.readInt(), reuse, headerLessView); // 指向数据
        return reuse; // 返回可复用对象
    }

    /**
     * 从分页输入视图中跳过当前记录。
     */
    @Override
    public void skipRecordFromPages(AbstractPagedInputView headerLessView) throws IOException {
        checkSkipReadForFixLengthPart(headerLessView); // 检查是否跳过读取固定部分
        headerLessView.skipBytes(headerLessView.readInt()); // 跳过指定字节数
    }

    /**
     * 从分页输入视图中复制二进制行记录到输出视图。
     */
    public void copyFromPagesToView(AbstractPagedInputView source, DataOutputView target)
            throws IOException {
        checkSkipReadForFixLengthPart(source); // 检查是否跳过读取固定部分
        int length = source.readInt(); // 读取记录长度
        target.writeInt(length); // 写入记录长度
        target.write(source, length); // 复制数据
    }

    /**
     * 指向分页输入视图中的二进制行记录。
     */
    public void pointTo(int length, BinaryRow reuse, AbstractPagedInputView headerLessView)
            throws IOException {
        if (length < 0) {
            // 异常处理：记录长度无效
            throw new IOException(
                    String.format(
                            "在源位置InSegment[%d]和LimitInSegment[%d]处读取到意外的字节数",
                            headerLessView.getCurrentPositionInSegment(),
                            headerLessView.getCurrentSegmentLimit()));
        }

        // 检查当前内存段的剩余空间是否足够
        int remainInSegment =
                headerLessView.getCurrentSegmentLimit()
                        - headerLessView.getCurrentPositionInSegment();
        MemorySegment currSeg = headerLessView.getCurrentSegment();
        int currPosInSeg = headerLessView.getCurrentPositionInSegment();
        if (remainInSegment >= length) {
            // 如果当前段的空间足够，则直接指向
            reuse.pointTo(currSeg, currPosInSeg, length);
            headerLessView.skipBytesToRead(length); // 跳过已读取的字节数
        } else {
            // 处理跨多个内存段的情况
            pointToMultiSegments(
                    reuse, headerLessView, length, length - remainInSegment, currSeg, currPosInSeg);
        }
    }

    /**
     * 指向跨多个内存段的二进制行记录。
     */
    private void pointToMultiSegments(
            BinaryRow reuse,
            AbstractPagedInputView source,
            int sizeInBytes,
            int remainLength,
            MemorySegment currSeg,
            int currPosInSeg)
            throws IOException {

        int segmentSize = currSeg.size(); // 内存段大小
        // 计算需要多少个完整的内存段
        int div = remainLength / segmentSize;
        int remainder = remainLength - segmentSize * div; // 剩余字节
        int varSegSize = remainder == 0 ? div : div + 1; // 可变段大小

        MemorySegment[] segments = new MemorySegment[varSegSize + 1]; // 分配内存段数组
        segments[0] = currSeg; // 当前内存段
        for (int i = 1; i <= varSegSize; i++) {
            source.advance(); // 切换到下一个内存段
            segments[i] = source.getCurrentSegment(); // 获取下一个内存段
        }

        // 计算最后一个内存段中剩余的字节数
        int remainLenInLastSeg = remainder == 0 ? segmentSize : remainder;
        source.skipBytesToRead(remainLenInLastSeg); // 跳过已读取的字节数
        reuse.pointTo(segments, currPosInSeg, sizeInBytes); // 更新可复用对象的指向
    }

    /**
     * 检查在写入二进制行固定部分时是否需要跳过字节。
     */
    private int checkSkipWriteForFixLengthPart(AbstractPagedOutputView out) throws IOException {
        // 计算当前段中剩余的可用字节数
        int available = out.getSegmentSize() - out.getCurrentPositionInSegment();
        // 如果剩余空间不足以写入固定部分，则切换到下一个段
        if (available < getSerializedRowFixedPartLength()) {
            out.advance();
            return available; // 返回跳过的字节数
        }
        return 0; // 无需跳过
    }

    /**
     * 检查在读取二进制行固定部分时是否需要跳过字节。
     */
    public void checkSkipReadForFixLengthPart(AbstractPagedInputView source) throws IOException {
        int available = source.getCurrentSegmentLimit() - source.getCurrentPositionInSegment();
        if (available < getSerializedRowFixedPartLength()) {
            source.advance(); // 切换到下一个段
        }
    }

    /**
     * 返回序列化一行所需的固定部分长度（字节数）。
     */
    public int getSerializedRowFixedPartLength() {
        return getFixedLengthPartSize() + LENGTH_SIZE_IN_BYTES;
    }

    public int getFixedLengthPartSize() {
        return fixedLengthPartSize; // 返回固定长度部分大小
    }

    @Override
    public boolean equals(Object obj) {
        // 判断对象是否为同一类型的序列化器，并且字段数量相同
        return obj instanceof BinaryRowSerializer
                && numFields == ((BinaryRowSerializer) obj).numFields;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(numFields); // 根据字段数量生成哈希码
    }
}
