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

import org.apache.paimon.memory.MemorySegment;

import org.apache.flink.runtime.memory.MemoryManager;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 从 Flink 内存管理器为 Paimon 分配内存段（MemorySegment）。
 */
public class MemorySegmentAllocator {

    // 任务拥有者对象
    private final Object owner;

    // Flink 内存管理器
    private final MemoryManager memoryManager;

    // 存储已分配的内存段
    private final List<org.apache.flink.core.memory.MemorySegment> allocatedSegments;

    // 临时存储分配的内存段，用于分配时的中转
    private final List<org.apache.flink.core.memory.MemorySegment> segments;

    // 反射获取 Flink MemorySegment 内部的 offHeapBuffer 字段
    private final Field offHeapBufferField;

    /**
     * 构造函数，初始化分配器，并获取 MemorySegment 的 offHeapBuffer 字段。
     *
     * @param owner 任务拥有者对象
     * @param memoryManager Flink 内存管理器
     */
    public MemorySegmentAllocator(Object owner, MemoryManager memoryManager) {
        this.owner = owner;
        this.memoryManager = memoryManager;
        this.allocatedSegments = new ArrayList<>();
        this.segments = new ArrayList<>(1);

        try {
            // 通过反射获取 Flink MemorySegment 的 offHeapBuffer 字段
            this.offHeapBufferField =
                    org.apache.flink.core.memory.MemorySegment.class.getDeclaredField("offHeapBuffer");
            this.offHeapBufferField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 为内存池分配一个新的内存段。
     *
     * @return 分配的内存段
     */
    public MemorySegment allocate() {
        segments.clear();
        try {
            // 从 Flink 内存管理器分配一个页面
            memoryManager.allocatePages(owner, segments, 1);
            org.apache.flink.core.memory.MemorySegment segment = segments.remove(0);

            // 确保分配的内存段不为空
            checkNotNull(segment, "Allocate null segment from memory manager for Paimon.");

            // 确保分配的是堆外内存（off-heap）
            checkArgument(segment.isOffHeap(), "Segment is not off heap from memory manager.");

            // 将分配的内存段加入已分配列表
            allocatedSegments.add(segment);

            // 通过反射获取内存段的 ByteBuffer，并将其包装为 Paimon 的 MemorySegment
            // TODO: 在 Flink 相关 issue 解决后（https://issues.apache.org/jira/browse/FLINK-32213），
            // 直接使用 MemorySegment 提供的方法替代反射。
            return MemorySegment.wrapOffHeapMemory((ByteBuffer) offHeapBufferField.get(segment));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 释放所有已分配的内存段，如果任务已关闭，则调用此方法进行释放。
     */
    public void release() {
        memoryManager.release(allocatedSegments);
    }
}

