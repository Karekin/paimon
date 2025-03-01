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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** 从指定的 {@link Snapshot} 中根据给定的配置读取分割块。 */
public interface SnapshotReader {

    SnapshotManager snapshotManager(); // 获取快照管理器对象

    ConsumerManager consumerManager(); // 获取消费者管理器对象

    SplitGenerator splitGenerator(); // 获取分割块生成器对象

    SnapshotReader withSnapshot(long snapshotId); // 指定要读取的快照 ID

    SnapshotReader withSnapshot(Snapshot snapshot); // 指定要读取的快照对象

    SnapshotReader withFilter(Predicate predicate); // 设置行过滤器，过滤不符合条件的行

    SnapshotReader withPartitionFilter(Map<String, String> partitionSpec); // 设置分区过滤器，过滤不符合条件的分区

    SnapshotReader withPartitionFilter(Predicate predicate); // 设置分区过滤器，过滤不符合条件的分区

    SnapshotReader withPartitionFilter(List<BinaryRow> partitions); // 设置分区过滤器，过滤指定分区外的数据

    SnapshotReader withMode(ScanMode scanMode); // 设置扫描模式，如 ALL、APPEND、CHANGELOG 等

    SnapshotReader withLevelFilter(Filter<Integer> levelFilter); // 设置层级过滤器，过滤不符合条件的层级

    SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter); // 设置清单入口过滤器，过滤不符合条件的清单入口

    SnapshotReader withBucket(int bucket); // 设置要读取的桶号

    SnapshotReader withBucketFilter(Filter<Integer> bucketFilter); // 设置桶过滤器，过滤不符合条件的桶

    SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter); // 设置数据文件名过滤器，过滤不符合条件的文件名

    SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks); // 设置分片，用于并行读取

    SnapshotReader withMetricRegistry(MetricRegistry registry); // 设置度量注册表，用于记录读取指标

    /** 从快照中获取分割块计划。 */
    Plan read();

    /** 从文件变更中获取分割块计划。 */
    Plan readChanges();

    Plan readIncrementalDiff(Snapshot before); // 读取快照变化，获取增量变化

    /** 列出分区。 */
    List<BinaryRow> partitions();

    List<PartitionEntry> partitionEntries(); // 列出分区入口

    /** 扫描的结果计划。 */
    interface Plan extends TableScan.Plan {

        @Nullable
        Long watermark(); // 获取水印时间

        /**
         * 此计划的快照 ID，如果表为空或指定了清单列表，则返回 null。
         */
        @Nullable
        Long snapshotId();

        /** 结果分割块列表。 */
        List<Split> splits();

        default List<DataSplit> dataSplits() { // 获取数据分割块列表
            return (List) splits();
        }
    }
}
