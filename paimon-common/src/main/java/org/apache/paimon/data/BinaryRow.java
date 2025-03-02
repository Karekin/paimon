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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.nio.ByteOrder;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 一种 {@link InternalRow} 的实现，它使用 {@link MemorySegment} 而不是 Object 作为底层支持。
 *      它可以显著减少 Java 对象的序列化/反序列化。
 *
 * <p>一行数据包含两个部分：固定长度部分和可变长度部分。
 *
 * <p>固定长度部分包含 1 个字节的头部、用于跟踪 null 值的位集（null bit set）以及字段值。null 位集用于跟踪 null 值，
 *      并且对齐到 8 字节的边界。`字段值`存储固定长度的原始类型和可变长度值（如果这些值可以存储在 8 字节内）。
 *      如果可变长度值无法存储在 8 字节内，则存储其长度和可变长度部分的偏移量。
 *
 * <p>固定长度部分一定会落在一个 MemorySegment 中，这将加速字段的读写操作。
 *      在写入阶段，如果目标内存段的空间小于固定长度部分的大小，我们将跳过该空间。
 *      因此，单个 Row 中的字段数量不能超过单个 MemorySegment 的容量。
 *      如果字段数量过多，我们建议用户设置更大的 MemorySegment 页面大小。
 *
 * <p>可变长度部分可能会跨越多个 MemorySegment。
 *
 * @since 0.4.0
 */
@Public
public final class BinaryRow extends BinarySection implements InternalRow, DataSetters {

    private static final long serialVersionUID = 1L;

    /**
     * 表示当前系统使用的字节序是否为小端（Little-Endian）。如果 nativeOrder() 返回的
     * ByteOrder 是小端，则 LITTLE_ENDIAN 为 true，否则为 false。
     */
    public static final boolean LITTLE_ENDIAN =
            (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

    /**
     * 定义一个掩码，用于判断第一个字节是否为零。如果系统是小端字节序，则掩码的最低位字节为 FF，
     * 其他位设置为全 1；如果系统是大端字节序，则掩码的最高位字节为 FF，其他位设置为全 1。
     */
    private static final long FIRST_BYTE_ZERO = LITTLE_ENDIAN ? ~0xFFL : ~(0xFFL << 56L);

    /**
     * 头部大小（Header size）以位为单位，固定为 8 位，即 1 字节。
     */
    public static final int HEADER_SIZE_IN_BITS = 8;

    /**
     * 空 Row 的实例，用于表示没有字段的 Row。
     */
    public static final BinaryRow EMPTY_ROW = new BinaryRow(0);

    /**
     * 初始化 EMPTY_ROW，并将它绑定到一个固定大小的内存块中。该内存块是通过 MemorySegment 包装的，
     * 大小为 EMPTY_ROW 的固定部分大小。
     */
    static {
        int size = EMPTY_ROW.getFixedLengthPartSize();
        byte[] bytes = new byte[size];
        EMPTY_ROW.pointTo(MemorySegment.wrap(bytes), 0, size);
    }

    /**
     * 计算存储 null 位集所需的字节数。对于给定的字段数（arity），计算的公式是：
     * ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8。其中，每 64 个字段需要 8 个字节的位集，
     * 并且需要对齐到 8 字节边界。
     *
     * @param arity 字段数目
     * @return 存储 null 位集所需的字节数
     */
    public static int calculateBitSetWidthInBytes(int arity) {
        return ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8;
    }

    /**
     * 计算固定部分所需的字节总数，包括 null 位集和字段值的大小。固定部分的大小等于 null
     * 位集的大小加上每个字段 8 字节（假设每个字段占用 8 字节）。
     *
     * @param arity 字段数目
     * @return 固定部分的大小（字节）
     */
    public static int calculateFixPartSizeInBytes(int arity) {
        return calculateBitSetWidthInBytes(arity) + 8 * arity;
    }

    /**
     * 字段数（Arity），表示当前 Row 中包含的字段数目。
     */
    private final int arity;

    /**
     * null 位集所需的字节数，根据字段数计算得出。
     */
    private final int nullBitsSizeInBytes;

    /**
     * 构造一个 BinaryRow，指定字段数目。
     *
     * @param arity 字段数目，需大于等于 0
     */
    public BinaryRow(int arity) {
        checkArgument(arity >= 0);
        this.arity = arity;
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
    }

    /**
     * 根据字段位置计算字段在固定部分中的偏移量。
     *
     * @param pos 字段位置（索引）
     * @return 字段的偏移量（相对于 Row 起始位置）
     */
    private int getFieldOffset(int pos) {
        return offset + nullBitsSizeInBytes + pos * 8;
    }

    /**
     * 断言语法，检查字段位置是否有效。
     *
     * @param index 字段位置（索引）
     */
    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < arity : "index (" + index + ") should < " + arity;
    }

    /**
     * 获取固定长度部分的大小（字节）。
     *
     * @return 固定部分的大小（字节）
     */
    public int getFixedLengthPartSize() {
        return nullBitsSizeInBytes + 8 * arity;
    }

    /**
     * 获取字段数目。
     *
     * @return 字段数目
     */
    @Override
    public int getFieldCount() {
        return arity;
    }

    /**
     * 获取 Row 的类型（RowKind）。
     *
     * @return Row 类型
     */
    @Override
    public RowKind getRowKind() {
        byte kindValue = segments[0].get(offset);
        return RowKind.fromByteValue(kindValue);
    }

    /**
     * 设置 Row 的类型（RowKind）。
     *
     * @param kind Row 类型
     */
    @Override
    public void setRowKind(RowKind kind) {
        segments[0].put(offset, kind.toByteValue());
    }

    /**
     * 设置 Row 的总大小（字节）。
     *
     * @param sizeInBytes 总大小（字节）
     */
    public void setTotalSize(int sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    /**
     * 检查指定字段位置的值是否为 null。
     *
     * @param pos 字段位置（索引）
     * @return 值是否为 null
     */
    @Override
    public boolean isNullAt(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.bitGet(segments[0], offset, pos + HEADER_SIZE_IN_BITS);
    }

    /**
     * 设置指定字段位置的值为非 null（清除 null 标记）。
     *
     * @param i 字段位置（索引）
     */
    private void setNotNullAt(int i) {
        assertIndexIsValid(i);
        MemorySegmentUtils.bitUnSet(segments[0], offset, i + HEADER_SIZE_IN_BITS);
    }

    /**
     * 设置指定字段位置的值为 null。
     *
     * @param i 字段位置（索引）
     */
    @Override
    public void setNullAt(int i) {
        assertIndexIsValid(i);
        MemorySegmentUtils.bitSet(segments[0], offset, i + HEADER_SIZE_IN_BITS);
        // 将固定部分的字段数据设置为 0，以确保在 equals 和 hash 操作时数据一致
        segments[0].putLong(getFieldOffset(i), 0);
    }

    /**
     * 设置指定字段位置的值为 int 类型。
     *
     * @param pos 字段位置（索引）
     * @param value 值
     */
    @Override
    public void setInt(int pos, int value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putInt(getFieldOffset(pos), value);
    }

    /**
     * 设置指定字段位置的值为 long 类型。
     *
     * @param pos 字段位置（索引）
     * @param value 值
     */
    @Override
    public void setLong(int pos, long value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putLong(getFieldOffset(pos), value);
    }

    /**
     * 设置指定字段位置的值为 double 类型。
     *
     * @param pos 字段位置（索引）
     * @param value 值
     */
    @Override
    public void setDouble(int pos, double value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putDouble(getFieldOffset(pos), value);
    }

    /**
     * 设置指定字段位置的值为 Decimal 类型。如果精度允许，使用紧凑格式存储；否则，使用变长存储。
     *
     * @param pos 字段位置（索引）
     * @param value 值
     * @param precision 精度
     */
    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            // 紧凑格式，直接存储为 long 类型
            setLong(pos, value.toUnscaledLong());
        } else {
            // 变长存储，需要存储长度和偏移量
            int fieldOffset = getFieldOffset(pos);
            int cursor = (int) (segments[0].getLong(fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            // 清零变长部分的内存
            MemorySegmentUtils.setLong(segments, offset + cursor, 0L);
            MemorySegmentUtils.setLong(segments, offset + cursor + 8, 0L);

            if (value == null) {
                setNullAt(pos);
                // 保留偏移量，以便后续更新
                segments[0].putLong(fieldOffset, ((long) cursor) << 32);
            } else {
                byte[] bytes = value.toUnscaledBytes();
                assert bytes.length <= 16;

                // 将数据写入变长部分
                MemorySegmentUtils.copyFromBytes(segments, offset + cursor, bytes, 0, bytes.length);
                // 更新固定部分的数据（存储偏移量和长度）
                setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
            }
        }
    }

    /**
     * 设置指定字段位置的值为 Timestamp 类型。如果精度允许，使用紧凑格式存储；否则，拆分为两部分存储。
     *
     * @param pos 字段位置（索引）
     * @param value 值
     * @param precision 精度
     */
    @Override
    public void setTimestamp(int pos, Timestamp value, int precision) {
        assertIndexIsValid(pos);

        if (Timestamp.isCompact(precision)) {
            // 紧凑格式，直接存储为 long 类型（时间戳毫秒部分）
            setLong(pos, value.getMillisecond());
        } else {
            // 拆分为两部分存储：变长部分存储毫秒部分，固定部分存储纳秒部分
            int fieldOffset = getFieldOffset(pos);
            int cursor = (int) (segments[0].getLong(fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            if (value == null) {
                setNullAt(pos);
                // 清零变长部分的内存
                MemorySegmentUtils.setLong(segments, offset + cursor, 0L);
                // 保留偏移量
                segments[0].putLong(fieldOffset, ((long) cursor) << 32);
            } else {
                // 将毫秒部分写入变长部分，纳秒部分写入固定部分
                MemorySegmentUtils.setLong(segments, offset + cursor, value.getMillisecond());
                setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
            }
        }
    }

    /**
     * 设置指定字段位置的值为 boolean 类型。
     *
     * @param pos 字段位置（索引）
     * @param value 值
     */
    @Override
    public void setBoolean(int pos, boolean value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putBoolean(getFieldOffset(pos), value);
    }

    /**
     * 设置指定字段位置的值为 short 类型。
     *
     * @param pos 字段位置（索引）
     * @param value 值
     */
    @Override
    public void setShort(int pos, short value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putShort(getFieldOffset(pos), value);
    }

    /**
     * 设置指定字段位置的值为 byte 类型。
     *
     * @param pos 字段位置（索引）
     * @param value 值
     */
    @Override
    public void setByte(int pos, byte value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].put(getFieldOffset(pos), value);
    }

    /**
     * 设置指定字段位置的值为 float 类型。
     *
     * @param pos 字段位置（索引）
     * @param value 值
     */
    @Override
    public void setFloat(int pos, float value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        segments[0].putFloat(getFieldOffset(pos), value);
    }

    /**
     * 获取指定字段位置的 boolean 值。
     *
     * @param pos 字段位置（索引）
     * @return boolean 值
     */
    @Override
    public boolean getBoolean(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getBoolean(getFieldOffset(pos));
    }

    /**
     * 获取指定字段位置的 byte 值。
     *
     * @param pos 字段位置（索引）
     * @return byte 值
     */
    @Override
    public byte getByte(int pos) {
        assertIndexIsValid(pos);
        return segments[0].get(getFieldOffset(pos));
    }

    /**
     * 获取指定字段位置的 short 值。
     *
     * @param pos 字段位置（索引）
     * @return short 值
     */
    @Override
    public short getShort(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getShort(getFieldOffset(pos));
    }

    /**
     * 获取指定字段位置的 int 值。
     *
     * @param pos 字段位置（索引）
     * @return int 值
     */
    @Override
    public int getInt(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getInt(getFieldOffset(pos));
    }

    /**
     * 获取指定字段位置的 long 值。
     *
     * @param pos 字段位置（索引）
     * @return long 值
     */
    @Override
    public long getLong(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getLong(getFieldOffset(pos));
    }

    /**
     * 获取指定字段位置的 float 值。
     *
     * @param pos 字段位置（索引）
     * @return float 值
     */
    @Override
    public float getFloat(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getFloat(getFieldOffset(pos));
    }

    /**
     * 获取指定字段位置的 double 值。
     *
     * @param pos 字段位置（索引）
     * @return double 值
     */
    @Override
    public double getDouble(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getDouble(getFieldOffset(pos));
    }

    /**
     * 获取指定字段位置的 BinaryString 值。
     *
     * @param pos 字段位置（索引）
     * @return BinaryString 值
     */
    @Override
    public BinaryString getString(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = segments[0].getLong(fieldOffset);
        return MemorySegmentUtils.readBinaryString(segments, offset, fieldOffset, offsetAndLen);
    }

    /**
     * 获取指定字段位置的 Decimal 值。
     *
     * @param pos 字段位置（索引）
     * @param precision 精度
     * @param scale 小数位数
     * @return Decimal 值
     */
    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            // 紧凑格式
            return Decimal.fromUnscaledLong(
                    segments[0].getLong(getFieldOffset(pos)), precision, scale);
        } else {
            // 变长存储
            int fieldOffset = getFieldOffset(pos);
            final long offsetAndSize = segments[0].getLong(fieldOffset);
            return MemorySegmentUtils.readDecimal(segments, offset, offsetAndSize, precision, scale);
        }
    }

    /**
     * 获取指定字段位置的 Timestamp 值。
     *
     * @param pos 字段位置（索引）
     * @param precision 精度
     * @return Timestamp 值
     */
    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        assertIndexIsValid(pos);

        if (Timestamp.isCompact(precision)) {
            // 紧凑格式
            return Timestamp.fromEpochMillis(segments[0].getLong(getFieldOffset(pos)));
        } else {
            // 变长存储
            int fieldOffset = getFieldOffset(pos);
            final long offsetAndNanoOfMilli = segments[0].getLong(fieldOffset);
            return MemorySegmentUtils.readTimestampData(segments, offset, offsetAndNanoOfMilli);
        }
    }

    /**
     * 获取指定字段位置的二进制数据。
     *
     * @param pos 字段位置（索引）
     * @return 二进制数据
     */
    @Override
    public byte[] getBinary(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = segments[0].getLong(fieldOffset);
        return MemorySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndLen);
    }

    /**
     * 获取指定字段位置的数组数据。
     *
     * @param pos 字段位置（索引）
     * @return 数组数据
     */
    @Override
    public InternalArray getArray(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.readArrayData(segments, offset, getLong(pos));
    }

    /**
     * 获取指定字段位置的映射数据。
     *
     * @param pos 字段位置（索引）
     * @return 映射数据
     */
    @Override
    public InternalMap getMap(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.readMapData(segments, offset, getLong(pos));
    }

    /**
     * 获取指定字段位置的 Row 数据。
     *
     * @param pos 字段位置（索引）
     * @param numFields Row 的字段数目
     * @return Row 数据
     */
    @Override
    public InternalRow getRow(int pos, int numFields) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.readRowData(segments, numFields, offset, getLong(pos));
    }

    /**
     * 检查是否存在任何为空的字段。
     *
     * @return 是否存在空字段
     */
    public boolean anyNull() {
        // 跳过头部字节的判断
        if ((segments[0].getLong(0) & FIRST_BYTE_ZERO) != 0) {
            return true;
        }
        // 遍历 null 位集的其他部分
        for (int i = 8; i < nullBitsSizeInBytes; i += 8) {
            if (segments[0].getLong(i) != 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * 检查指定字段集合是否存在任何为空的字段。
     *
     * @param fields 字段位置集合
     * @return 是否存在空字段
     */
    public boolean anyNull(int[] fields) {
        for (int field : fields) {
            if (isNullAt(field)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 复制当前 Row。
     *
     * @return 复制后的 Row
     */
    public BinaryRow copy() {
        return copy(new BinaryRow(arity));
    }

    /**
     * 使用给定的 Row 进行复制。
     *
     * @param reuse 目标 Row
     * @return 复制后的 Row
     */
    public BinaryRow copy(BinaryRow reuse) {
        return copyInternal(reuse);
    }

    /**
     * 内部复制逻辑，将当前 Row 的数据复制到目标 Row。
     *
     * @param reuse 目标 Row
     * @return 复制后的 Row
     */
    private BinaryRow copyInternal(BinaryRow reuse) {
        byte[] bytes = MemorySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    /**
     * 清空当前 Row 的所有数据，将 segments、offset 和 sizeInBytes 置为初始值。
     */
    public void clear() {
        segments = null;
        offset = 0;
        sizeInBytes = 0;
    }

    /**
     * 比较两个 BinaryRow 或 NestedRow 是否相等。相等的条件是：
     * 1. totalSize 相同；
     * 2. 所有字节的内容相同。
     *
     * @param o 对象
     * @return 是否相等
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // 包括 BinaryRow 和 NestedRow 的比较
        if (!(o instanceof BinaryRow || o instanceof NestedRow)) {
            return false;
        }
        final BinarySection that = (BinarySection) o;
        // 比较总字节数和内容是否一致
        return sizeInBytes == that.sizeInBytes
                && MemorySegmentUtils.equals(
                segments, offset, that.segments, that.offset, sizeInBytes);
    }

    /**
     * 计算当前 Row 的哈希值，基于其内容。哈希值的计算方式是按照内存中的字节数据进行分块计算。
     *
     * @return 哈希值
     */
    @Override
    public int hashCode() {
        return MemorySegmentUtils.hashByWords(segments, offset, sizeInBytes);
    }

    /**
     * 创建一个包含单个整形字段的 Row。
     *
     * @param i 整形字段的值
     * @return 包含单个整形字段的 Row
     */
    public static BinaryRow singleColumn(@Nullable Integer i) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        if (i == null) {
            writer.setNullAt(0);
        } else {
            writer.writeInt(0, i);
        }
        writer.complete();
        return row;
    }

    /**
     * 创建一个包含单个字符串字段的 Row。
     *
     * @param string 字符串字段的值
     * @return 包含单个字符串字段的 Row
     */
    public static BinaryRow singleColumn(@Nullable String string) {
        BinaryString binaryString = string == null ? null : BinaryString.fromString(string);
        return singleColumn(binaryString);
    }

    /**
     * 创建一个包含单个 BinaryString 字段的 Row。
     *
     * @param string BinaryString 字段的值
     * @return 包含单个 BinaryString 字段的 Row
     */
    public static BinaryRow singleColumn(@Nullable BinaryString string) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        if (string == null) {
            writer.setNullAt(0);
        } else {
            writer.writeString(0, string);
        }
        writer.complete();
        return row;
    }
}
