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

package org.apache.paimon.memory;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.options.MemorySize;

import java.util.List;

/**
 * 内存段池（MemorySegmentPool），用于在内存中存储多个页面（pages）。
 *
 * <p>该接口提供了一组方法，用于管理内存页的分配、释放和回收，确保在受限的内存环境下高效使用内存资源。
 *
 * @since 0.4.0
 */
@Public
public interface MemorySegmentPool extends MemorySegmentSource {

    /** 默认的内存页大小，32KB。 */
    int DEFAULT_PAGE_SIZE = 32 * 1024;

    /**
     * 获取当前池中的页面大小（字节）。
     *
     * @return 每个页面的大小，以字节为单位。
     */
    int pageSize();

    /**
     * 将所有内存页归还到该池中。
     *
     * <p>当不再需要使用这些内存页时，应调用该方法归还，以便其他组件可以重复利用这些资源。
     *
     * @param memory 需要归还的内存页列表。
     */
    void returnAll(List<MemorySegment> memory);

    /**
     * 获取当前池中的可用空闲页数。
     *
     * @return 空闲页面的数量。
     */
    int freePages();

    /**
     * 创建一个基于堆（Heap）的 MemorySegmentPool。
     *
     * <p>该方法用于分配一块指定大小的堆内存，并将其划分为固定大小的页面进行管理。
     *
     * @param maxMemory  最大可分配的内存总量。
     * @param pageSize   每个页面的大小。
     * @return  一个新的堆内存段池（HeapMemorySegmentPool）。
     */
    static MemorySegmentPool createHeapPool(MemorySize maxMemory, MemorySize pageSize) {
        return new HeapMemorySegmentPool(maxMemory.getBytes(), (int) pageSize.getBytes());
    }
}

