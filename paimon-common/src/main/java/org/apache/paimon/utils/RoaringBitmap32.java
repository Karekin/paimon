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

package org.apache.paimon.utils;

import org.apache.paimon.annotation.VisibleForTesting;

import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;

/** 一个压缩的用于处理 32 位整数的位图。 */
public class RoaringBitmap32 {

    public static final int MAX_VALUE = Integer.MAX_VALUE; // 支持的最大整数值

    private final RoaringBitmap roaringBitmap; // 底层的 RoaringBitmap 对象

    public RoaringBitmap32() {
        this.roaringBitmap = new RoaringBitmap(); // 初始化 RoaringBitmap 对象
    }

    private RoaringBitmap32(RoaringBitmap roaringBitmap) {
        this.roaringBitmap = roaringBitmap; // 使用已有的 RoaringBitmap 对象初始化
    }

    public void add(int x) {
        roaringBitmap.add(x); // 将指定整数值添加到位图中
    }

    public void or(RoaringBitmap32 other) {
        roaringBitmap.or(other.roaringBitmap); // 将两个位图进行按位或操作
    }

    public boolean checkedAdd(int x) {
        return roaringBitmap.checkedAdd(x); // 尝试添加指定整数值，并返回是否成功
    }

    public boolean contains(int x) {
        return roaringBitmap.contains(x); // 检查指定整数值是否存在于位图中
    }

    public boolean isEmpty() {
        return roaringBitmap.isEmpty(); // 判断位图是否为空
    }

    public long getCardinality() {
        return roaringBitmap.getLongCardinality(); // 获取位图中不同整数值的数量
    }

    public void serialize(DataOutput out) throws IOException {
        roaringBitmap.runOptimize(); // 优化位图数据结构
        roaringBitmap.serialize(out); // 将位图序列化为数据输出流
    }

    public void deserialize(DataInput in) throws IOException {
        roaringBitmap.deserialize(in); // 从数据输入流中反序列化位图
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoaringBitmap32 that = (RoaringBitmap32) o;
        return Objects.equals(this.roaringBitmap, that.roaringBitmap); // 比较两个位图是否相等
    }

    public void clear() {
        roaringBitmap.clear(); // 清空位图
    }

    public byte[] serialize() {
        roaringBitmap.runOptimize(); // 优化位图数据结构
        ByteBuffer buffer = ByteBuffer.allocate(roaringBitmap.serializedSizeInBytes()); // 分配缓冲区
        roaringBitmap.serialize(buffer); // 序列化位图到缓冲区
        return buffer.array(); // 返回字节数组
    }

    public void deserialize(byte[] rbmBytes) throws IOException {
        roaringBitmap.deserialize(ByteBuffer.wrap(rbmBytes)); // 从字节数组中反序列化位图
    }

    public void flip(final long rangeStart, final long rangeEnd) {
        roaringBitmap.flip(rangeStart, rangeEnd); // 翻转指定范围内的位
    }

    public Iterator<Integer> iterator() {
        return roaringBitmap.iterator(); // 返回位图的迭代器
    }

    @Override
    public String toString() {
        return roaringBitmap.toString(); // 返回位图的字符串表示
    }

    @VisibleForTesting
    public static RoaringBitmap32 bitmapOf(int... dat) {
        RoaringBitmap32 roaringBitmap32 = new RoaringBitmap32(); // 创建新的位图
        for (int ele : dat) {
            roaringBitmap32.add(ele); // 添加整数值到位图
        }
        return roaringBitmap32; // 返回位图
    }

    public static RoaringBitmap32 and(final RoaringBitmap32 x1, final RoaringBitmap32 x2) {
        return new RoaringBitmap32(RoaringBitmap.and(x1.roaringBitmap, x2.roaringBitmap)); // 计算两个位图的按位与
    }

    public static RoaringBitmap32 or(final RoaringBitmap32 x1, final RoaringBitmap32 x2) {
        return new RoaringBitmap32(RoaringBitmap.or(x1.roaringBitmap, x2.roaringBitmap)); // 计算两个位图的按位或
    }

    public static RoaringBitmap32 or(Iterator<RoaringBitmap32> iterator) {
        return new RoaringBitmap32(
                RoaringBitmap.or(
                        new Iterator<RoaringBitmap>() {
                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }

                            @Override
                            public RoaringBitmap next() {
                                return iterator.next().roaringBitmap;
                            }
                        })); // 计算多个位图的按位或
    }
}
