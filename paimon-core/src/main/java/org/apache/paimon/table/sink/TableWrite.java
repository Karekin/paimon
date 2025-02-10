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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.Table;

/**
 * TableWrite 用于将 {@link InternalRow} 写入 {@link Table}。
 *
 * @since 0.4.0
 */
@Public
public interface TableWrite extends AutoCloseable {

    /**
     * 设置 {@link IOManager}，在 'write-buffer-spillable' 选项为 true 时需要。
     *
     * @param ioManager I/O 管理器
     * @return 当前 TableWrite 实例
     */
    TableWrite withIOManager(IOManager ioManager);

    /**
     * 为当前 TableWrite 设置 {@link MemorySegmentPool} 以进行内存管理。
     *
     * @param memoryPool 内存段池
     * @return 当前 TableWrite 实例
     */
    TableWrite withMemoryPool(MemorySegmentPool memoryPool);

    /**
     * 当记录的仅插入状态发生变化时，会调用此方法。
     *
     * @param insertOnly 如果为 true，所有后续记录都将是 {@link org.apache.paimon.types.RowKind#INSERT}，
     *                   并且不会有两个记录具有相同的主键。
     */
    void withInsertOnly(boolean insertOnly);

    /**
     * 计算指定行 {@code row} 属于哪个分区。
     *
     * @param row 输入行
     * @return 计算出的分区
     */
    BinaryRow getPartition(InternalRow row);

    /**
     * 计算指定行 {@code row} 属于哪个存储桶（bucket）。
     *
     * @param row 输入行
     * @return 计算出的存储桶 ID
     */
    int getBucket(InternalRow row);

    /**
     * 将一行数据写入存储。
     *
     * @param row 输入行
     * @throws Exception 写入失败时抛出异常
     */
    void write(InternalRow row) throws Exception;

    /**
     * 将一行数据写入指定存储桶（bucket）。
     *
     * @param row    输入行
     * @param bucket 存储桶 ID
     * @throws Exception 写入失败时抛出异常
     */
    void write(InternalRow row, int bucket) throws Exception;

    /**
     * 直接批量写入 Bundle 记录，而不是逐行写入。
     *
     * @param partition 目标分区
     * @param bucket    目标存储桶
     * @param bundle    需要写入的 Bundle 记录
     * @throws Exception 写入失败时抛出异常
     */
    void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle) throws Exception;

    /**
     * 对指定分区和存储桶进行 compact 操作。
     *
     * <p>默认情况下，该方法会根据 'num-sorted-run.compaction-trigger' 选项决定是否执行 compact。
     * 如果 fullCompaction 为 true，则强制执行 full compaction，该操作开销较大。
     *
     * <p>注意：在 Java API 中，full compaction 不会自动执行。如果将 'changelog-producer' 设置为
     * 'full-compaction'，请定期调用此方法以生成变更日志（changelog）。
     *
     * @param partition      目标分区
     * @param bucket         目标存储桶
     * @param fullCompaction 是否强制执行 full compaction
     * @throws Exception 发生错误时抛出异常
     */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /**
     * 设置指标（metrics）以测量 compaction 操作的性能。
     *
     * @param registry 指标注册表
     * @return 当前 TableWrite 实例
     */
    TableWrite withMetricRegistry(MetricRegistry registry);
}

