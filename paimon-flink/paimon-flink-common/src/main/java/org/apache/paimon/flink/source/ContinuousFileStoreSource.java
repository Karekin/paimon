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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.StreamTableScan;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * 用于读取记录的无边界 FlinkSource。它持续监控新的快照。
 * <p>该类继承自 {@link FlinkSource}，实现了一个无界的数据源，用于从文件存储中读取数据，并持续监控新的快照。</p>
 */
public class ContinuousFileStoreSource extends FlinkSource {

    private static final long serialVersionUID = 4L; // 序列化版本号

    protected final Map<String, String> options; // 配置选项
    protected final BucketMode bucketMode; // 桶模式，用于分片存储

    /**
     * 构造函数，初始化文件存储源。
     *
     * @param readBuilder 读取构建器，用于构建数据读取逻辑
     * @param options 配置选项，包含数据源的各种参数
     * @param limit 读取限制，可选参数
     */
    public ContinuousFileStoreSource(
            ReadBuilder readBuilder, Map<String, String> options, @Nullable Long limit) {
        this(readBuilder, options, limit, BucketMode.HASH_FIXED); // 默认使用固定哈希的桶模式
    }

    /**
     * 构造函数，初始化文件存储源。
     *
     * @param readBuilder 读取构建器
     * @param options 配置选项
     * @param limit 读取限制
     * @param bucketMode 桶模式
     */
    public ContinuousFileStoreSource(
            ReadBuilder readBuilder,
            Map<String, String> options,
            @Nullable Long limit,
            BucketMode bucketMode) {
        super(readBuilder, limit); // 调用父类构造函数
        this.options = options; // 保存配置选项
        this.bucketMode = bucketMode; // 保存桶模式
    }

    @Override
    public Boundedness getBoundedness() {
        // 获取配置中的有界 Watermark 值
        Long boundedWatermark = CoreOptions.fromMap(options).scanBoundedWatermark();
        // 如果设置了有界 Watermark，则返回有界数据源，否则返回无界数据源
        return boundedWatermark != null ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        Long nextSnapshotId = null; // 下一个快照ID
        Collection<FileStoreSourceSplit> splits = new ArrayList<>(); // 分片集合
        if (checkpoint != null) {
            nextSnapshotId = checkpoint.currentSnapshotId(); // 从检查点中恢复快照ID
            splits = checkpoint.splits(); // 从检查点中恢复分片
        }

        StreamTableScan scan = readBuilder.newStreamScan(); // 创建流扫描
        if (metricGroup(context) != null) {
            // 如果可用，为扫描添加指标注册表
            ((StreamDataTableScan) scan)
                    .withMetricsRegistry(new FlinkMetricRegistry(context.metricGroup()));
        }

        // 恢复扫描状态
        scan.restore(nextSnapshotId);

        // 构建分片枚举器
        return buildEnumerator(context, splits, nextSnapshotId, scan);
    }

    @Nullable
    private SplitEnumeratorMetricGroup metricGroup(SplitEnumeratorContext<?> context) {
        try {
            return context.metricGroup();
        } catch (NullPointerException ignore) {
            // 忽略某些 Flink 版本的 `metricGroup` 方法引发的 NullPointerException
            return null;
        }
    }

    protected SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> buildEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits,
            @Nullable Long nextSnapshotId,
            StreamTableScan scan) {
        // 从配置中获取选项
        Options options = Options.fromMap(this.options);
        // 创建持续的文件分片枚举器
        return new ContinuousFileSplitEnumerator(
                context,
                splits,
                nextSnapshotId,
                options.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis(),
                scan,
                bucketMode,
                options.get(CoreOptions.SCAN_MAX_SPLITS_PER_TASK),
                options.get(FlinkConnectorOptions.STREAMING_READ_SHUFFLE_BUCKET_WITH_PARTITION));
    }
}
