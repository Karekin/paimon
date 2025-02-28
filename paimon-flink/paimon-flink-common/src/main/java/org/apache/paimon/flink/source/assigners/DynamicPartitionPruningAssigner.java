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

package org.apache.paimon.flink.source.assigners;

import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 通过给定的 {@link DynamicFilteringData} 执行动态分区裁剪的分配器。
 * <p>该分配器用于在数据读取过程中动态过滤掉不需要的分区，以优化数据读取性能。</p>
 */
public class DynamicPartitionPruningAssigner implements SplitAssigner {

    private final SplitAssigner innerAssigner; // 内部分片分配器
    private final Projection partitionRowProjection; // 分区投影
    private final DynamicFilteringData dynamicFilteringData; // 动态过滤数据

    public DynamicPartitionPruningAssigner(
            SplitAssigner innerAssigner,
            Projection partitionRowProjection,
            DynamicFilteringData dynamicFilteringData) {
        this.innerAssigner = innerAssigner; // 初始化内部分片分配器
        this.partitionRowProjection = partitionRowProjection; // 初始化分区投影
        this.dynamicFilteringData = dynamicFilteringData; // 初始化动态过滤数据
    }

    @Override
    public List<FileStoreSourceSplit> getNext(int subtask, @Nullable String hostname) {
        // 从内部分片分配器中获取分片，并根据动态过滤数据进行过滤
        List<FileStoreSourceSplit> sourceSplits = innerAssigner.getNext(subtask, hostname);
        while (!sourceSplits.isEmpty()) {
            List<FileStoreSourceSplit> filtered =
                    sourceSplits.stream().filter(this::filter).collect(Collectors.toList());
            if (!filtered.isEmpty()) {
                return filtered;
            }
            sourceSplits = innerAssigner.getNext(subtask, hostname);
        }

        return Collections.emptyList();
    }

    @Override
    public void addSplit(int suggestedTask, FileStoreSourceSplit splits) {
        // 如果分片通过过滤条件，则将其添加到内部分片分配器中
        if (filter(splits)) {
            innerAssigner.addSplit(suggestedTask, splits);
        }
    }

    @Override
    public void addSplitsBack(int subtask, List<FileStoreSourceSplit> splits) {
        // 将分片添加回内部分片分配器
        innerAssigner.addSplitsBack(subtask, splits);
    }

    @Override
    public Collection<FileStoreSourceSplit> remainingSplits() {
        // 获取剩余分片并进行过滤
        return innerAssigner.remainingSplits().stream()
                .filter(this::filter)
                .collect(Collectors.toList());
    }

    /**
     * 根据动态过滤数据创建动态分区裁剪分配器。
     *
     * @param subtaskId 子任务 ID
     * @param oriAssigner 原始分片分配器
     * @param partitionRowProjection 分区投影
     * @param sourceEvent 源事件
     * @param logger 日志记录器
     * @return 动态分区裁剪分配器
     */
    public static SplitAssigner createDynamicPartitionPruningAssignerIfNeeded(
            int subtaskId,
            SplitAssigner oriAssigner,
            Projection partitionRowProjection,
            SourceEvent sourceEvent,
            Logger logger) {
        DynamicFilteringData dynamicFilteringData = ((DynamicFilteringEvent) sourceEvent).getData();
        logger.info(
                "Received DynamicFilteringEvent: {}, is filtering: {}.",
                subtaskId,
                dynamicFilteringData.isFiltering());
        return dynamicFilteringData.isFiltering()
                ? new DynamicPartitionPruningAssigner(
                oriAssigner, partitionRowProjection, dynamicFilteringData)
                : oriAssigner;
    }

    @Override
    public Optional<Long> getNextSnapshotId(int subtask) {
        // 获取下一个快照 ID
        return innerAssigner.getNextSnapshotId(subtask);
    }

    @Override
    public int numberOfRemainingSplits() {
        // 获取剩余分片数量
        return innerAssigner.numberOfRemainingSplits();
    }

    /**
     * 过滤分片，判断是否符合动态分区过滤条件。
     *
     * @param sourceSplit 分片
     * @return 如果分片符合过滤条件，返回 true；否则返回 false
     */
    private boolean filter(FileStoreSourceSplit sourceSplit) {
        DataSplit dataSplit = (DataSplit) sourceSplit.split();
        BinaryRow partition = dataSplit.partition();
        FlinkRowData projected = new FlinkRowData(partitionRowProjection.apply(partition));
        return dynamicFilteringData.contains(projected);
    }
}
