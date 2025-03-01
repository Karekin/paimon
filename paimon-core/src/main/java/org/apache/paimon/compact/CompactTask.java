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

package org.apache.paimon.compact;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.MetricUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * 压缩任务抽象类。
 *
 * 该类是一个抽象类，定义了压缩任务的通用逻辑和接口。
 * 它实现了 {@link Callable<CompactResult>} 接口，用于执行压缩任务。
 * 子类需要实现 {@link #doCompact()} 方法以提供具体的压缩逻辑。
 */
public abstract class CompactTask implements Callable<CompactResult> {

    // 日志记录器
    private static final Logger LOG = LoggerFactory.getLogger(CompactTask.class);

    // 指标报告器，用于报告压缩任务的性能指标
    @Nullable private final CompactionMetrics.Reporter metricsReporter;

    /**
     * 构造函数，初始化 CompactTask 对象。
     *
     * @param metricsReporter 指标报告器
     */
    public CompactTask(@Nullable CompactionMetrics.Reporter metricsReporter) {
        this.metricsReporter = metricsReporter;
    }

    @Override
    public CompactResult call() throws Exception {
        MetricUtils.safeCall(this::startTimer, LOG); // 启动压缩任务计时器
        try {
            long startMillis = System.currentTimeMillis(); // 记录起始时间
            CompactResult result = doCompact(); // 执行实际的压缩逻辑

            // 报告压缩时间指标
            if (metricsReporter != null) {
                metricsReporter.reportCompactionTime(System.currentTimeMillis() - startMillis);
            }

            // 记录调试日志
            if (LOG.isDebugEnabled()) {
                logMetric(startMillis, result.before(), result.after());
            }

            return result; // 返回压缩结果
        } finally {
            MetricUtils.safeCall(this::stopTimer, LOG); // 停止压缩任务计时器
        }
    }

    /**
     * 启动压缩任务计时器。
     */
    private void startTimer() {
        if (metricsReporter != null) {
            metricsReporter.getCompactTimer().start();
        }
    }

    /**
     * 停止压缩任务计时器。
     */
    private void stopTimer() {
        if (metricsReporter != null) {
            metricsReporter.getCompactTimer().finish();
        }
    }

    /**
     * 记录压缩任务的指标信息。
     *
     * @param startMillis 起始时间（毫秒）
     * @param compactBefore 压缩前的文件元数据
     * @param compactAfter 压缩后的文件元数据
     * @return 返回格式化的日志字符串
     */
    protected String logMetric(
            long startMillis,
            List<DataFileMeta> compactBefore,
            List<DataFileMeta> compactAfter) {
        return String.format(
                "压缩任务完成：耗时：%dms，输入文件数：%d，输出文件数：%d，输入文件总大小：%d，输出文件总大小：%d",
                System.currentTimeMillis() - startMillis,
                compactBefore.size(),
                compactAfter.size(),
                collectRewriteSize(compactBefore),
                collectRewriteSize(compactAfter));
    }

    /**
     * 执行具体的压缩逻辑。
     *
     * @return 返回压缩结果
     * @throws Exception 执行压缩时可能抛出的异常
     */
    protected abstract CompactResult doCompact() throws Exception;

    /**
     * 计算指定文件列表的总大小。
     *
     * @param files 文件元数据列表
     * @return 返回文件的总大小
     */
    private long collectRewriteSize(List<DataFileMeta> files) {
        return files.stream().mapToLong(DataFileMeta::fileSize).sum();
    }
}
