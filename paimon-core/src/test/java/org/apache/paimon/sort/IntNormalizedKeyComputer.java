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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.SortUtil;

/** Example for int {@link NormalizedKeyComputer}. */
public class IntNormalizedKeyComputer implements NormalizedKeyComputer {

    public static final IntNormalizedKeyComputer INSTANCE = new IntNormalizedKeyComputer();

    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 将数据保存到内存段中 MemorySegment
    */
    @Override
    public void putKey(InternalRow record, MemorySegment target, int offset) {
        // write first null byte.
        // 如果记录的第一个字段是null，则使用SortUtil工具类在目标内存段的指定偏移量处写入一个最小化的规范化键（5表示标记null或者特定的最小键）。
        if (record.isNullAt(0)) {
            SortUtil.minNormalizedKey(target, offset, 5);
        } else {
            // 如果记录的第一个字段不是null，则在目标内存段的指定偏移量处写入一个字节1作为非null标识。
            target.put(offset, (byte) 1);
            // 紧接着，将记录的第一个字段的规范化键写入到目标内存段中，从偏移量+1的位置开始，占用4个字节。
            SortUtil.putIntNormalizedKey(record.getInt(0), target, offset + 1, 4);
        }

        // revert 4 bytes to compare easier.
        target.putInt(offset, Integer.reverseBytes(target.getInt(offset)));
    }

    @Override
    public int compareKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ) {
        int int1 = segI.getInt(offsetI);
        int int2 = segJ.getInt(offsetJ);
        if (int1 != int2) {
            return (int1 < int2) ^ (int1 < 0) ^ (int2 < 0) ? -1 : 1;
        }

        byte byte1 = segI.get(offsetI + 4);
        byte byte2 = segJ.get(offsetJ + 4);
        if (byte1 != byte2) {
            return (byte1 < byte2) ^ (byte1 < 0) ^ (byte2 < 0) ? -1 : 1;
        }
        return 0;
    }

    @Override
    public void swapKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ) {

        int temp0 = segI.getInt(offsetI);
        segI.putInt(offsetI, segJ.getInt(offsetJ));
        segJ.putInt(offsetJ, temp0);

        byte temp1 = segI.get(offsetI + 4);
        segI.put(offsetI + 4, segJ.get(offsetJ + 4));
        segJ.put(offsetJ + 4, temp1);
    }

    @Override
    public int getNumKeyBytes() {
        return 5;
    }

    @Override
    public boolean isKeyFullyDetermines() {
        return true;
    }

    @Override
    public boolean invertKey() {
        return false;
    }
}
