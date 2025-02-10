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

/**
 * 提供内存管理相关方法的接口。
 *
 * <p>实现该接口的类可以管理自己的内存池（MemorySegmentPool），
 * 并提供查询内存占用情况以及释放内存的方法。
 */
public interface MemoryOwner {

    /**
     * 为该内存所有者（MemoryOwner）设置 {@link MemorySegmentPool}。
     *
     * <p>内存池用于分配和管理内存页（MemorySegment）。
     *
     * @param memoryPool 需要设置的内存池
     */
    void setMemoryPool(MemorySegmentPool memoryPool);

    /**
     * 获取当前内存所有者的内存占用大小。
     *
     * <p>该方法返回当前分配给该 MemoryOwner 的内存字节数。
     *
     * @return 内存占用大小（字节）
     */
    long memoryOccupancy();

    /**
     * 刷新（flush）并释放该内存所有者的内存。
     *
     * <p>该方法通常用于释放占用的内存资源，以便其他组件可以使用这些内存。
     * 在内存不足时，该方法可能会被调用以回收部分内存。
     *
     * @throws Exception 如果在释放内存时发生错误
     */
    void flushMemory() throws Exception;
}

