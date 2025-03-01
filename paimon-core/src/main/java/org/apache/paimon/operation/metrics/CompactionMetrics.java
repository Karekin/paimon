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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 * 用于衡量压缩任务的指标类。
 *
 * 该类用于收集和报告压缩任务的性能指标，如压缩时间、L0 文件数等。
 */
public class CompactionMetrics {

    private static final String GROUP_NAME = "compaction"; // 指标组名称
    public static final String MAX_LEVEL0_FILE_COUNT = "maxLevel0FileCount"; // L0 文件数最大值
    public static final String AVG_LEVEL0_FILE_COUNT = "avgLevel0FileCount"; // L0 文件数平均值
    public static final String COMPACTION_THREAD_BUSY = "compactionThreadBusy"; // 压缩线程忙碌百分比
    public static final String AVG_COMPACTION_TIME = "avgCompactionTime"; // 平均压缩时间
    private static final long BUSY_MEASURE_MILLIS = 60_000; // 用于计算线程忙碌的周期（毫秒）
    private static final int COMPACTION_TIME_WINDOW = 100; // 压缩时间窗口大小

    private final MetricGroup metricGroup; // 指标组
    private final Map<PartitionAndBucket, ReporterImpl> reporters; // 报告器集合
    private final Map<Long, CompactTimer> compactTimers; // 压缩计时器集合
    private final Queue<Long> compactionTimes; // 压缩时间队列

    /**
     * 构造函数，初始化 CompactionMetrics 对象。
     *
     * @param registry 指标注册表
     * @param tableName 表名称
     */
    public CompactionMetrics(MetricRegistry registry, String tableName) {
        this.metricGroup = registry.tableMetricGroup(GROUP_NAME, tableName); // 初始化指标组
        this.reporters = new HashMap<>(); // 初始化报告器集合
        this.compactTimers = new ConcurrentHashMap<>(); // 初始化压缩计时器集合
        this.compactionTimes = new ConcurrentLinkedQueue<>(); // 初始化压缩时间队列

        registerGenericCompactionMetrics(); // 注册通用压缩指标
    }

    /**
     * 获取指标组。
     *
     * @return 返回指标组
     */
    @VisibleForTesting
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    /**
     * 注册通用压缩指标。
     */
    private void registerGenericCompactionMetrics() {
        metricGroup.gauge(MAX_LEVEL0_FILE_COUNT, () -> getLevel0FileCountStream().max().orElse(-1)); // 注册 L0 文件数最大值
        metricGroup.gauge(
                AVG_LEVEL0_FILE_COUNT, () -> getLevel0FileCountStream().average().orElse(-1)); // 注册 L0 文件数平均值
        metricGroup.gauge(
                AVG_COMPACTION_TIME, () -> getCompactionTimeStream().average().orElse(0.0)); // 注册平均压缩时间
        metricGroup.gauge(COMPACTION_THREAD_BUSY, () -> getCompactBusyStream().sum()); // 注册压缩线程忙碌百分比
    }

    /**
     * 获取 L0 文件数的流数据。
     *
     * @return 返回 L0 文件数的流
     */
    private LongStream getLevel0FileCountStream() {
        return reporters.values().stream().mapToLong(r -> r.level0FileCount);
    }

    /**
     * 获取压缩线程忙碌流数据。
     *
     * @return 返回压缩线程忙碌的流
     */
    private DoubleStream getCompactBusyStream() {
        return compactTimers.values().stream()
                .mapToDouble(t -> 100.0 * t.calculateLength() / BUSY_MEASURE_MILLIS);
    }

    /**
     * 获取压缩时间流数据。
     *
     * @return 返回压缩时间的流
     */
    private DoubleStream getCompactionTimeStream() {
        return compactionTimes.stream().mapToDouble(Long::doubleValue);
    }

    /**
     * 关闭指标组。
     */
    public void close() {
        metricGroup.close();
    }

    /**
     * 压缩指标报告器接口。
     */
    public interface Reporter {

        CompactTimer getCompactTimer(); // 获取压缩计时器
        void reportLevel0FileCount(long count); // 报告 L0 文件数
        void reportCompactionTime(long time); // 报告压缩时间
        void unregister(); // 注销报告器
    }

    private class ReporterImpl implements Reporter {

        private final PartitionAndBucket key; // 分区和桶的标识
        private long level0FileCount; // L0 文件数

        private ReporterImpl(PartitionAndBucket key) {
            this.key = key; // 初始化分区和桶的标识
            this.level0FileCount = 0; // 初始化 L0 文件数
        }

        @Override
        public CompactTimer getCompactTimer() {
            return compactTimers.computeIfAbsent(
                    Thread.currentThread().getId(),
                    ignore -> new CompactTimer(BUSY_MEASURE_MILLIS));
        }

        @Override
        public void reportCompactionTime(long time) {
            synchronized (compactionTimes) {
                compactionTimes.add(time); // 添加压缩时间到队列
                if (compactionTimes.size() > COMPACTION_TIME_WINDOW) {
                    compactionTimes.poll(); // 保持队列大小
                }
            }
        }

        @Override
        public void reportLevel0FileCount(long count) {
            this.level0FileCount = count; // 设置 L0 文件数
        }

        @Override
        public void unregister() {
            reporters.remove(key); // 从报告器集合中移除
        }
    }

    /**
     * 创建一个新的报告器。
     *
     * @param partition 分区
     * @param bucket 桶
     * @return 返回一个新的报告器
     */
    public Reporter createReporter(BinaryRow partition, int bucket) {
        PartitionAndBucket key = new PartitionAndBucket(partition, bucket); // 创建分区和桶的标识
        ReporterImpl reporter = new ReporterImpl(key); // 创建报告器
        reporters.put(key, reporter); // 添加到报告器集合
        return reporter;
    }

    private static class PartitionAndBucket {

        private final BinaryRow partition; // 分区
        private final int bucket; // 桶

        private PartitionAndBucket(BinaryRow partition, int bucket) {
            this.partition = partition;
            this.bucket = bucket;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket); // 计算哈希码
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PartitionAndBucket)) {
                return false;
            }
            PartitionAndBucket other = (PartitionAndBucket) o;
            return Objects.equals(partition, other.partition) && bucket == other.bucket; // 比较分区和桶
        }
    }
}