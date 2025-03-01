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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.utils.RoaringBitmap32;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * 基于 RoaringBitmap32 的删除向量，仅支持行数不超过 RoaringBitmap32.MAX_VALUE 的文件。
 */
public class BitmapDeletionVector implements DeletionVector {

    public static final int MAGIC_NUMBER = 1581511376; // 魔术数字，用于标识序列化数据的格式

    private final RoaringBitmap32 roaringBitmap; // 核心数据结构，用于存储删除行的位置

    public BitmapDeletionVector() {
        this.roaringBitmap = new RoaringBitmap32(); // 初始化 RoaringBitmap32 对象
    }

    private BitmapDeletionVector(RoaringBitmap32 roaringBitmap) {
        this.roaringBitmap = roaringBitmap; // 使用已有的 RoaringBitmap32 对象初始化
    }

    @Override
    public void delete(long position) {
        checkPosition(position); // 检查行位置是否超出支持范围
        roaringBitmap.add((int) position); // 将指定行位置标记为已删除
    }

    @Override
    public void merge(DeletionVector deletionVector) {
        if (deletionVector instanceof BitmapDeletionVector) {
            roaringBitmap.or(((BitmapDeletionVector) deletionVector).roaringBitmap); // 合并两个删除向量
        } else {
            throw new RuntimeException("只能合并相同类型的删除向量。");
        }
    }

    @Override
    public boolean checkedDelete(long position) {
        checkPosition(position); // 检查行位置是否超出支持范围
        return roaringBitmap.checkedAdd((int) position); // 检查并标记指定行位置为已删除
    }

    @Override
    public boolean isDeleted(long position) {
        checkPosition(position); // 检查行位置是否超出支持范围
        return roaringBitmap.contains((int) position); // 检查指定行位置是否已被删除
    }

    @Override
    public boolean isEmpty() {
        return roaringBitmap.isEmpty(); // 判断删除向量是否为空
    }

    @Override
    public long getCardinality() {
        return roaringBitmap.getCardinality(); // 获取删除向量中不同整数的数量
    }

    @Override
    public byte[] serializeToBytes() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(MAGIC_NUMBER); // 写入魔术数字
            roaringBitmap.serialize(dos); // 序列化 RoaringBitmap32 数据
            return bos.toByteArray(); // 返回字节数组
        } catch (Exception e) {
            throw new RuntimeException("无法序列化删除向量", e);
        }
    }

    public static DeletionVector deserializeFromDataInput(DataInput bis) throws IOException {
        RoaringBitmap32 roaringBitmap = new RoaringBitmap32();
        roaringBitmap.deserialize(bis); // 反序列化 RoaringBitmap32 数据
        return new BitmapDeletionVector(roaringBitmap); // 返回新的 BitmapDeletionVector 对象
    }

    private void checkPosition(long position) {
        if (position > RoaringBitmap32.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "文件行数过多，RoaringBitmap32 仅支持行数不超过 2147483647 的文件。");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BitmapDeletionVector that = (BitmapDeletionVector) o;
        return Objects.equals(this.roaringBitmap, that.roaringBitmap); // 比较两个删除向量是否相等
    }
}