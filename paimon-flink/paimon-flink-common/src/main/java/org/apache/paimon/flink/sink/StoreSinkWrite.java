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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.table.sink.TableWriteImpl;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/** Helper class of {@link PrepareCommitOperator} for different types of paimon sinks. */
/**
* @授课老师: 码界探索
* @微信: 252810631
* @版权所有: 请尊重劳动成果
* {@link PrepareCommitOperator}的帮助类，用于不同类型的paimon接收器
*/
public interface StoreSinkWrite {

    /**
     * This method is called when the insert only status of the records changes.
     *
     * @param insertOnly If true, all the following records would be of {@link
     *     org.apache.paimon.types.RowKind#INSERT}, and no two records would have the same primary
     *     key.
     */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 当记录的仅插入状态发生变化时，会调用此方法。
    */
    void withInsertOnly(boolean insertOnly);

    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 数据写入到paimon表
    */
    @Nullable
    SinkRecord write(InternalRow rowData) throws Exception;
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 数据写入到paimon表中对应的桶
     */
    @Nullable
    SinkRecord write(InternalRow rowData, int bucket) throws Exception;

    SinkRecord toLogRecord(SinkRecord record);
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 对paimon表进行Compact
    */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files);

    List<Committable> prepareCommit(boolean waitCompaction, long checkpointId) throws IOException;

    void snapshotState() throws Exception;

    boolean streamingMode();

    void close() throws Exception;

    /**
     * Replace the internal {@link TableWriteImpl} with the one provided by {@code
     * newWriteProvider}. The state of the old {@link TableWriteImpl} will also be transferred to
     * the new {@link TableWriteImpl} by {@link TableWriteImpl#checkpoint()} and {@link
     * TableWriteImpl#restore(List)}.
     *
     * <p>Currently, this method is only used by CDC sinks because they need to deal with schema
     * changes. {@link TableWriteImpl} with the new schema will be provided by {@code
     * newWriteProvider}.
     */
    void replace(FileStoreTable newTable) throws Exception;

    /** Provider of {@link StoreSinkWrite}. */
    @FunctionalInterface
    interface Provider extends Serializable {

        StoreSinkWrite provide(
                FileStoreTable table,
                String commitUser,
                StoreSinkWriteState state,
                IOManager ioManager,
                @Nullable MemorySegmentPool memoryPool,
                @Nullable MetricGroup metricGroup);
    }

    /** Provider of {@link StoreSinkWrite} that uses given write buffer. */
    @FunctionalInterface
    interface WithWriteBufferProvider extends Serializable {

        StoreSinkWrite provide(
                FileStoreTable table,
                String commitUser,
                StoreSinkWriteState state,
                IOManager ioManager,
                @Nullable MemoryPoolFactory memoryPoolFactory,
                MetricGroup metricGroup);
    }
}
