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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.flink.source.assigners.FIFOSplitAssigner;
import org.apache.paimon.flink.source.assigners.PreAssignSplitAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;

import static org.apache.paimon.flink.FlinkConnectorOptions.SplitAssignMode;
/**
 * 有界 {@link FlinkSource}，用于读取记录。不会监控新的快照。
 * <p>该类实现了一个有界的数据源，用于从文件存储中读取数据，并且不会动态监控新的快照。</p>
 */
public class StaticFileStoreSource extends FlinkSource {

    private static final long serialVersionUID = 3L; // 序列化版本号

    private final int splitBatchSize; // 分片批量大小，用于控制分片的分配
    private final SplitAssignMode splitAssignMode; // 分片分配模式
    @Nullable private final DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo;

    /**
     * 构造函数，初始化静态文件存储源。
     *
     * @param readBuilder 数据读取构建器，用于构建读取逻辑
     * @param limit 读取限制，可选参数
     * @param splitBatchSize 分片批量大小
     * @param splitAssignMode 分片分配模式
     */
    public StaticFileStoreSource(
            ReadBuilder readBuilder,
            @Nullable Long limit,
            int splitBatchSize,
            SplitAssignMode splitAssignMode) {
        this(readBuilder, limit, splitBatchSize, splitAssignMode, null); // 调用另一个构造函数
    }

    /**
     * 构造函数，初始化静态文件存储源。
     *
     * @param readBuilder 数据读取构建器
     * @param limit 读取限制
     * @param splitBatchSize 分片批量大小
     * @param splitAssignMode 分片分配模式
     * @param dynamicPartitionFilteringInfo 动态分区筛选信息（可选）
     */
    public StaticFileStoreSource(
            ReadBuilder readBuilder,
            @Nullable Long limit,
            int splitBatchSize,
            SplitAssignMode splitAssignMode,
            @Nullable DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo) {
        super(readBuilder, limit); // 调用父类构造函数
        this.splitBatchSize = splitBatchSize; // 保存分片批量大小
        this.splitAssignMode = splitAssignMode; // 保存分片分配模式
        this.dynamicPartitionFilteringInfo = dynamicPartitionFilteringInfo; // 保存动态分区筛选信息
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED; // 返回有界数据源
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        // 获取分片，如果检查点为 null，则重新生成分片，否则使用检查点中的分片
        Collection<FileStoreSourceSplit> splits =
                checkpoint == null ? getSplits(context) : checkpoint.splits();
        // 创建分片分配器
        SplitAssigner splitAssigner =
                createSplitAssigner(context, splitBatchSize, splitAssignMode, splits);
        // 创建静态文件存储分片枚举器
        return new StaticFileStoreSplitEnumerator(
                context, null, splitAssigner, dynamicPartitionFilteringInfo);
    }

    private List<FileStoreSourceSplit> getSplits(SplitEnumeratorContext context) {
        FileStoreSourceSplitGenerator splitGenerator = new FileStoreSourceSplitGenerator();
        TableScan scan = readBuilder.newScan(); // 创建表扫描
        // 注册扫描指标
        if (context.metricGroup() != null) {
            ((InnerTableScan) scan)
                    .withMetricsRegistry(new FlinkMetricRegistry(context.metricGroup()));
        }
        return splitGenerator.createSplits(scan.plan()); // 生成分片
    }

    public static SplitAssigner createSplitAssigner(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            int splitBatchSize,
            SplitAssignMode splitAssignMode,
            Collection<FileStoreSourceSplit> splits) {
        switch (splitAssignMode) {
            case FAIR:
                // 公平分配模式，创建公平分配的分片分配器
                return new PreAssignSplitAssigner(splitBatchSize, context, splits);
            case PREEMPTIVE:
                // 抢占式分配模式，创建 FIFO 分片分配器
                return new FIFOSplitAssigner(splits);
            default:
                // 如果分配模式不支持，抛出异常
                throw new UnsupportedOperationException(
                        "Unsupported assign mode " + splitAssignMode);
        }
    }
}
