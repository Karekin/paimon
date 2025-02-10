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

import org.apache.paimon.annotation.VisibleForTesting;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 一个工厂类，用于根据 {@link MemoryOwner} 创建 {@link MemorySegmentPool}。
 *
 * <p>返回的内存池在没有剩余内存时会尝试抢占（preempt）其他内存使用者的内存。
 */
public class MemoryPoolFactory {

    /** 内部使用的 MemorySegmentPool */
    private final MemorySegmentPool innerPool;

    /** 总页数，即该工厂管理的内存页总数 */
    private final int totalPages;

    /** 维护的内存使用者（MemoryOwner）集合 */
    private Iterable<MemoryOwner> owners;

    /** 总缓冲区大小（以字节为单位） */
    private final long totalBufferSize;

    /** 记录抢占（preempt）内存的次数 */
    private long bufferPreemptCount;

    /**
     * 构造函数，初始化 MemoryPoolFactory。
     *
     * @param innerPool  内部使用的 MemorySegmentPool
     */
    public MemoryPoolFactory(MemorySegmentPool innerPool) {
        this.innerPool = innerPool;
        this.totalPages = innerPool.freePages();
        this.totalBufferSize = (long) totalPages * innerPool.pageSize();
    }

    /**
     * 添加内存所有者（MemoryOwner）。
     *
     * @param newOwners 新的内存所有者
     * @return 返回当前 MemoryPoolFactory 以支持链式调用
     */
    public MemoryPoolFactory addOwners(Iterable<MemoryOwner> newOwners) {
        if (this.owners == null) {
            this.owners = newOwners;
        } else {
            Iterable<MemoryOwner> currentOwners = this.owners;
            this.owners = () -> Iterators.concat(currentOwners.iterator(), newOwners.iterator());
        }
        return this;
    }

    /**
     * 通知新的内存所有者（MemoryOwner），并为其分配子内存池。
     *
     * @param owner 需要分配内存池的内存所有者
     */
    public void notifyNewOwner(MemoryOwner owner) {
        checkNotNull(owners);
        owner.setMemoryPool(createSubPool(owner));
    }

    /**
     * 获取当前注册的所有内存所有者（仅用于测试）。
     *
     * @return 内存所有者列表
     */
    @VisibleForTesting
    public Iterable<MemoryOwner> memoryOwners() {
        return owners;
    }

    /**
     * 为指定的内存所有者创建一个子内存池。
     *
     * @param owner 目标内存所有者
     * @return 创建的内存池
     */
    MemorySegmentPool createSubPool(MemoryOwner owner) {
        return new OwnerMemoryPool(owner);
    }

    /**
     * 在内存不足时，尝试从其他 MemoryOwner 处抢占内存。
     *
     * @param owner 当前需要内存的 MemoryOwner
     */
    private void preemptMemory(MemoryOwner owner) {
        long maxMemory = 0;
        MemoryOwner max = null;

        // 遍历所有的内存所有者，找到占用最多内存的对象
        for (MemoryOwner other : owners) {
            // 不能抢占自己的内存，避免写入和刷新的竞争导致不一致的状态
            if (other != owner && other.memoryOccupancy() > maxMemory) {
                maxMemory = other.memoryOccupancy();
                max = other;
            }
        }

        if (max != null) {
            try {
                // 让最大占用者释放一些内存
                max.flushMemory();
                ++bufferPreemptCount;
            } catch (Exception e) {
                throw new RuntimeException("内存抢占失败", e);
            }
        }
    }

    /**
     * 获取已经抢占内存的次数。
     *
     * @return 抢占次数
     */
    public long bufferPreemptCount() {
        return bufferPreemptCount;
    }

    /**
     * 获取当前已使用的缓冲区大小（字节）。
     *
     * @return 已使用的内存大小
     */
    public long usedBufferSize() {
        long usedBufferSize = 0L;
        if (owners != null) {
            for (MemoryOwner owner : owners) {
                usedBufferSize += owner.memoryOccupancy();
            }
        }
        return usedBufferSize;
    }

    /**
     * 获取总的缓冲区大小（字节）。
     *
     * @return 总缓冲区大小
     */
    public long totalBufferSize() {
        return totalBufferSize;
    }

    /**
     * 内存池的具体实现类，为某个 MemoryOwner 提供内存管理功能。
     */
    private class OwnerMemoryPool implements MemorySegmentPool {

        /** 关联的内存所有者 */
        private final MemoryOwner owner;

        /** 当前已分配的页数 */
        private int allocatedPages = 0;

        /**
         * 构造函数，初始化 OwnerMemoryPool。
         *
         * @param owner 关联的 MemoryOwner
         */
        public OwnerMemoryPool(MemoryOwner owner) {
            this.owner = owner;
        }

        /**
         * 获取页面大小。
         *
         * @return 内存页大小（字节）
         */
        @Override
        public int pageSize() {
            return innerPool.pageSize();
        }

        /**
         * 归还所有内存页。
         *
         * @param memory 需要归还的内存页列表
         */
        @Override
        public void returnAll(List<MemorySegment> memory) {
            allocatedPages -= memory.size();
            innerPool.returnAll(memory);
        }

        /**
         * 获取当前可用的空闲页数。
         *
         * @return 空闲页数
         */
        @Override
        public int freePages() {
            return totalPages - allocatedPages;
        }

        /**
         * 分配下一个可用的内存页。
         *
         * @return 分配的 MemorySegment，如果内存不足则尝试抢占其他 MemoryOwner 的内存
         */
        @Override
        public MemorySegment nextSegment() {
            MemorySegment segment = innerPool.nextSegment();

            // 如果没有可用的内存页，尝试从其他内存使用者抢占内存
            if (segment == null) {
                preemptMemory(owner);
                segment = innerPool.nextSegment();
            }

            if (segment != null) {
                allocatedPages++;
            }

            return segment;
        }
    }
}

