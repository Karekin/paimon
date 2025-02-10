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
 * Flink 内存段池（MemorySegmentPool），用于从 Flink 管理的内存中为 Paimon Writer 缓冲区分配内存段。
 *
 * 该类继承自 AbstractMemorySegmentPool，利用 Flink 提供的内存管理机制来动态分配和管理内存段。
 */
public class FlinkMemorySegmentPool extends AbstractMemorySegmentPool {

    // 内存分配器，从 Flink 内存管理器中分配内存段
    private final MemorySegmentAllocator allocator;

    /**
     * 构造函数，初始化 FlinkMemorySegmentPool。
     *
     * @param maxMemory 内存池的最大可用内存大小（字节）
     * @param pageSize  每个内存页的大小（字节）
     * @param allocator Flink 内存分配器，用于从 Flink 内存管理器中分配内存段
     */
    public FlinkMemorySegmentPool(long maxMemory, int pageSize, MemorySegmentAllocator allocator) {
        super(maxMemory, pageSize);
        this.allocator = allocator;
    }

    /**
     * 从 Flink 内存管理器分配一个新的内存段。
     *
     * @return 分配的内存段
     */
    @Override
    protected MemorySegment allocateMemory() {
        return allocator.allocate();
    }
}

