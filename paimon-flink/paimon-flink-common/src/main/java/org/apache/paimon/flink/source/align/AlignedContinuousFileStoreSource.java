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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.source.ContinuousFileStoreSource;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.PendingSplitsCheckpoint;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;

import static org.apache.paimon.disk.IOManagerImpl.splitPaths;

/**
 * 用于创建 {@link AlignedSourceReader} 和 {@link AlignedContinuousFileSplitEnumerator}。
 * <p>这是一个扩展自 {@link ContinuousFileStoreSource} 的类，用于支持对齐的文件存储源读取器和分片枚举器。</p>
 */
public class AlignedContinuousFileStoreSource extends ContinuousFileStoreSource {

    /**
     * 构造函数，初始化对齐的持续文件存储源。
     *
     * @param readBuilder 数据读取构建器
     * @param options 配置选项
     * @param limit 读取限制
     * @param bucketMode 桶模式
     */
    public AlignedContinuousFileStoreSource(
            ReadBuilder readBuilder,
            Map<String, String> options,
            @Nullable Long limit,
            BucketMode bucketMode) {
        super(readBuilder, options, limit, bucketMode); // 调用父类构造函数
    }

    @Override
    public SourceReader<RowData, FileStoreSourceSplit> createReader(SourceReaderContext context) {
        // 创建 IO 管理器
        IOManager ioManager =
                IOManager.create(
                        splitPaths(
                                context.getConfiguration()
                                        .get(org.apache.flink.configuration.CoreOptions.TMP_DIRS)));

        // 创建文件存储源读取器指标
        FileStoreSourceReaderMetrics sourceReaderMetrics =
                new FileStoreSourceReaderMetrics(context.metricGroup());

        // 创建对齐的源读取器
        return new AlignedSourceReader(
                context,
                readBuilder.newRead(),
                sourceReaderMetrics,
                ioManager,
                limit,
                new FutureCompletingBlockingQueue<>( // 记录队列
                        context.getConfiguration()
                                .getInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY)));
    }

    @Override
    protected SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> buildEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits,
            @Nullable Long nextSnapshotId,
            StreamTableScan scan) {
        // 获取配置选项
        Options options = Options.fromMap(this.options);

        // 创建对齐的持续文件分片枚举器
        return new AlignedContinuousFileSplitEnumerator(
                context,
                splits,
                nextSnapshotId,
                options.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis(), // 发现间隔
                scan,
                bucketMode,
                options.get(FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_TIMEOUT).toMillis(), // 对齐超时
                options.get(CoreOptions.SCAN_MAX_SPLITS_PER_TASK),
                options.get(FlinkConnectorOptions.STREAMING_READ_SHUFFLE_BUCKET_WITH_PARTITION));
    }
}
