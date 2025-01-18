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

package org.apache.paimon.flink.memory;

import org.apache.paimon.memory.AbstractMemorySegmentPool;
import org.apache.paimon.memory.MemorySegment;

/**
 * Flink memory segment pool allocates segment from flink managed memory for paimon writer buffer.
 */
/**
* @授课老师: 码界探索
* @微信: 252810631
* @版权所有: 请尊重劳动成果
* Flink内存段池从Flink管理的内存中为paimon writer缓冲区分配段。
*/
public class FlinkMemorySegmentPool extends AbstractMemorySegmentPool {
    private final MemorySegmentAllocator allocator;

    public FlinkMemorySegmentPool(long maxMemory, int pageSize, MemorySegmentAllocator allocator) {
        super(maxMemory, pageSize);
        this.allocator = allocator;
    }

    @Override
    protected MemorySegment allocateMemory() {
        return allocator.allocate();
    }
}
