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

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentWritable;

import java.io.IOException;
import java.io.UTFDataFormatException;

/**
 * The base class for all output views that are backed by multiple memory pages. This base class
 * contains all encoding methods to write data to a page and detect page boundary crossing. The
 * concrete sub classes must implement the methods to collect the current page and provide the next
 * memory page once the boundary is crossed.
 *
 * <p>The paging assumes that all memory segments are of the same size.
 */
/**
* @授课老师: 码界探索
* @微信: 252810631
* @版权所有: 请尊重劳动成果
* 由多个内存页支持的所有输出视图的基类。
*/
public abstract class AbstractPagedOutputView implements DataOutputView, MemorySegmentWritable {
    //要写入的当前内存段
    protected MemorySegment currentSegment; // the current memory segment to write to
    //内存段的大小
    protected final int segmentSize; // the size of the memory segments
    //当前内存段的位置
    private int positionInSegment; // the offset in the current segment
    // UTF编码的缓冲区
    private byte[] utfBuffer; // the reusable array for UTF encodings

    // --------------------------------------------------------------------------------------------
    //                                    Constructors
    // --------------------------------------------------------------------------------------------

    /**
     * Creates a new output view that writes initially to the given initial segment. All segments in
     * the view have to be of the given {@code segmentSize}. A header of length {@code headerLength}
     * is left at the beginning of each segment.
     *
     * @param initialSegment The segment that the view starts writing to.
     * @param segmentSize The size of the memory segments.
     */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 创建一个新的输出视图，最初写入给定的初始段。视图中的所有段都必须具有给定的
    * @param initialSegment  视图开始写入的段。
    * @param segmentSize  内存段的大小。
    */
    protected AbstractPagedOutputView(MemorySegment initialSegment, int segmentSize) {
        if (initialSegment == null) {
            throw new NullPointerException("Initial Segment may not be null");
        }
        this.segmentSize = segmentSize;
        this.currentSegment = initialSegment;
        this.positionInSegment = 0;
    }

    // --------------------------------------------------------------------------------------------
    //                                  Page Management
    // --------------------------------------------------------------------------------------------

    /**
     * This method must return a segment. If no more segments are available, it must throw an {@link
     * java.io.EOFException}.
     *
     * @param current The current memory segment
     * @param positionInCurrent The position in the segment, one after the last valid byte.
     * @return The next memory segment.
     * @throws IOException
     */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 此方法必须返回一个段。如果没有更多可用的段，则必须抛出一个EOFException
    */
    protected abstract MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
            throws IOException;

    /**
     * Gets the segment that the view currently writes to.
     *
     * @return The segment the view currently writes to.
     */
    public MemorySegment getCurrentSegment() {
        return this.currentSegment;
    }

    /**
     * Gets the current write position (the position where the next bytes will be written) in the
     * current memory segment.
     *
     * @return The current write offset in the current memory segment.
     */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 获取当前内存段中的当前写入位置（将写入下一个字节的位置）。
    */
    public int getCurrentPositionInSegment() {
        return this.positionInSegment;
    }

    /**
     * Gets the size of the segments used by this view.
     *
     * @return The memory segment size.
     */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 获取此视图使用的段的大小。
    */
    public int getSegmentSize() {
        return this.segmentSize;
    }

    /**
     * Moves the output view to the next page. This method invokes internally the {@link
     * #nextSegment(MemorySegment, int)} method to give the current memory segment to the concrete
     * subclass' implementation and obtain the next segment to write to. Writing will continue
     * inside the new segment after the header.
     *
     * @throws IOException Thrown, if the current segment could not be processed or a new segment
     *     could not be obtained.
     */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 将输出视图移动到下一页。此方法在内部调用方法，将当前内存段提供给具体子类的实现，并获取下一个要写入的内存段。写入将在标头后的新内存段内继续。
    */
    public void advance() throws IOException {
        this.currentSegment = nextSegment(this.currentSegment, this.positionInSegment);
        this.positionInSegment = 0;
    }

    /**
     * Sets the internal state to the given memory segment and the given position within the
     * segment.
     * 将内部状态设置为给定的内存段和段内的给定位置。
     * @param seg The memory segment to write the next bytes to.
     * @param position The position to start writing the next bytes to.
     */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 将内部状态设置为给定的内存段和段内的给定位置。
    */
    protected void seekOutput(MemorySegment seg, int position) {
        this.currentSegment = seg;
        this.positionInSegment = position;
    }

    /**
     * Clears the internal state. Any successive write calls will fail until either {@link
     * #advance()} or {@link #seekOutput(MemorySegment, int)} is called.
     *
     * @see #advance()
     * @see #seekOutput(MemorySegment, int)
     */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 清除内部状态。任何连续的写调用都将失败，直到调用
     * #advance() #seekOutput(MemorySegment, int)
    */
    protected void clear() {
        this.currentSegment = null;
        this.positionInSegment = 0;
    }

    // --------------------------------------------------------------------------------------------
    //                               Data Output Specific methods
    // --------------------------------------------------------------------------------------------

    @Override
    public void write(int b) throws IOException {
        writeByte(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int remaining = this.segmentSize - this.positionInSegment;
        if (remaining >= len) {
            this.currentSegment.put(this.positionInSegment, b, off, len);
            this.positionInSegment += len;
        } else {
            if (remaining == 0) {
                advance();
                remaining = this.segmentSize - this.positionInSegment;
            }
            while (true) {
                int toPut = Math.min(remaining, len);
                this.currentSegment.put(this.positionInSegment, b, off, toPut);
                off += toPut;
                len -= toPut;

                if (len > 0) {
                    this.positionInSegment = this.segmentSize;
                    advance();
                    remaining = this.segmentSize - this.positionInSegment;
                } else {
                    this.positionInSegment += toPut;
                    break;
                }
            }
        }
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        writeByte(v ? 1 : 0);
    }

    @Override
    public void writeByte(int v) throws IOException {
        if (this.positionInSegment < this.segmentSize) {
            this.currentSegment.put(this.positionInSegment++, (byte) v);
        } else {
            advance();
            writeByte(v);
        }
    }

    @Override
    public void writeShort(int v) throws IOException {
        if (this.positionInSegment < this.segmentSize - 1) {
            this.currentSegment.putShortBigEndian(this.positionInSegment, (short) v);
            this.positionInSegment += 2;
        } else if (this.positionInSegment == this.segmentSize) {
            advance();
            writeShort(v);
        } else {
            writeByte(v >> 8);
            writeByte(v);
        }
    }

    @Override
    public void writeChar(int v) throws IOException {
        if (this.positionInSegment < this.segmentSize - 1) {
            this.currentSegment.putCharBigEndian(this.positionInSegment, (char) v);
            this.positionInSegment += 2;
        } else if (this.positionInSegment == this.segmentSize) {
            advance();
            writeChar(v);
        } else {
            writeByte(v >> 8);
            writeByte(v);
        }
    }

    @Override
    public void writeInt(int v) throws IOException {
        if (this.positionInSegment < this.segmentSize - 3) {
            this.currentSegment.putIntBigEndian(this.positionInSegment, v);
            this.positionInSegment += 4;
        } else if (this.positionInSegment == this.segmentSize) {
            advance();
            writeInt(v);
        } else {
            writeByte(v >> 24);
            writeByte(v >> 16);
            writeByte(v >> 8);
            writeByte(v);
        }
    }

    @Override
    public void writeLong(long v) throws IOException {
        if (this.positionInSegment < this.segmentSize - 7) {
            this.currentSegment.putLongBigEndian(this.positionInSegment, v);
            this.positionInSegment += 8;
        } else if (this.positionInSegment == this.segmentSize) {
            advance();
            writeLong(v);
        } else {
            writeByte((int) (v >> 56));
            writeByte((int) (v >> 48));
            writeByte((int) (v >> 40));
            writeByte((int) (v >> 32));
            writeByte((int) (v >> 24));
            writeByte((int) (v >> 16));
            writeByte((int) (v >> 8));
            writeByte((int) v);
        }
    }

    @Override
    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToRawIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToRawLongBits(v));
    }

    @Override
    public void writeBytes(String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            writeByte(s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            writeChar(s.charAt(i));
        }
    }

    @Override
    public void writeUTF(String str) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        int c, count = 0;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535) {
            throw new UTFDataFormatException("encoded string too long: " + utflen + " memory");
        }

        if (this.utfBuffer == null || this.utfBuffer.length < utflen + 2) {
            this.utfBuffer = new byte[utflen + 2];
        }
        final byte[] bytearr = this.utfBuffer;

        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
        bytearr[count++] = (byte) (utflen & 0xFF);

        int i;
        for (i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            }
            bytearr[count++] = (byte) c;
        }

        for (; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytearr[count++] = (byte) c;

            } else if (c > 0x07FF) {
                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                bytearr[count++] = (byte) (0x80 | (c & 0x3F));
            } else {
                bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                bytearr[count++] = (byte) (0x80 | (c & 0x3F));
            }
        }

        write(bytearr, 0, utflen + 2);
    }

    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
        while (numBytes > 0) {
            final int remaining = this.segmentSize - this.positionInSegment;
            if (numBytes <= remaining) {
                this.positionInSegment += numBytes;
                return;
            }
            this.positionInSegment = this.segmentSize;
            advance();
            numBytes -= remaining;
        }
    }

    @Override
    public void write(DataInputView source, int numBytes) throws IOException {
        while (numBytes > 0) {
            final int remaining = this.segmentSize - this.positionInSegment;
            if (numBytes <= remaining) {
                this.currentSegment.put(source, this.positionInSegment, numBytes);
                this.positionInSegment += numBytes;
                return;
            }

            if (remaining > 0) {
                this.currentSegment.put(source, this.positionInSegment, remaining);
                this.positionInSegment = this.segmentSize;
                numBytes -= remaining;
            }

            advance();
        }
    }

    @Override
    public void write(MemorySegment segment, int off, int len) throws IOException {
        int remaining = this.segmentSize - this.positionInSegment;
        //当前MemorySegment的能容纳要写的内容
        if (remaining >= len) {
            segment.copyTo(off, currentSegment, positionInSegment, len);
            this.positionInSegment += len;
        } else {
            //容量不够申请
            if (remaining == 0) {
                advance();
                remaining = this.segmentSize - this.positionInSegment;
            }

            while (true) {
                int toPut = Math.min(remaining, len);
                //将BinaryRow数据写入到MemorySegment
                segment.copyTo(off, currentSegment, positionInSegment, toPut);
                off += toPut;
                len -= toPut;

                if (len > 0) {
                    this.positionInSegment = this.segmentSize;
                    advance();
                    remaining = this.segmentSize - this.positionInSegment;
                } else {
                    this.positionInSegment += toPut;
                    break;
                }
            }
        }
    }
}
