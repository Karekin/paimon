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

import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.append.UnawareAppendTableCompactionCoordinator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ExecutorUtils;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.types.Either;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.ThreadUtils.newDaemonThreadFactory;

/**
 * 一个{@link OneInputStreamOperator}，用于接受提交消息并向下游操作符发送追加压缩任务的坐标信息。
 */
public class AppendBypassCoordinateOperator<CommitT>
        extends AbstractStreamOperator<Either<CommitT, UnawareAppendCompactionTask>>
        implements OneInputStreamOperator<CommitT, Either<CommitT, UnawareAppendCompactionTask>>,
        ProcessingTimeCallback {

    private static final long MAX_PENDING_TASKS = 5000; // 最大待处理任务数
    private static final long EMIT_PER_BATCH = 100; // 每批发出的任务数

    private final FileStoreTable table; // 文件存储表对象
    private final MailboxExecutorImpl mailbox; // 邮箱执行器

    private transient ScheduledExecutorService executorService; // 定时任务执行器
    private transient LinkedBlockingQueue<UnawareAppendCompactionTask> compactTasks; // 压缩任务队列

    public AppendBypassCoordinateOperator(
            FileStoreTable table,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailbox) {
        this.table = table; // 初始化文件存储表
        this.processingTimeService = processingTimeService; // 初始化时间服务
        this.mailbox = (MailboxExecutorImpl) mailbox; // 初始化邮箱执行器
        this.chainingStrategy = ChainingStrategy.HEAD; // 设置链接策略
    }

    @Override
    public void open() throws Exception {
        super.open();
        // 确保压缩协调器的并行度为 1
        checkArgument(
                getRuntimeContext().getNumberOfParallelSubtasks() == 1,
                "Compaction Coordinator parallelism in paimon MUST be one.");
        long intervalMs = table.coreOptions().continuousDiscoveryInterval().toMillis(); // 获取时间间隔
        this.compactTasks = new LinkedBlockingQueue<>(); // 初始化压缩任务队列
        UnawareAppendTableCompactionCoordinator coordinator =
                new UnawareAppendTableCompactionCoordinator(table, true, null); // 初始化协调器
        this.executorService =
                Executors.newSingleThreadScheduledExecutor(
                        newDaemonThreadFactory("Compaction Coordinator")); // 创建定时任务执行器
        this.executorService.scheduleWithFixedDelay(
                () -> asyncPlan(coordinator), 0, intervalMs, TimeUnit.MILLISECONDS); // 定期执行任务
        this.getProcessingTimeService().scheduleWithFixedDelay(this, 0, intervalMs); // 定期处理时间回调
    }

    // 异步生成压缩任务
    private void asyncPlan(UnawareAppendTableCompactionCoordinator coordinator) {
        while (compactTasks.size() < MAX_PENDING_TASKS) {
            List<UnawareAppendCompactionTask> tasks = coordinator.run(); // 运行协调器以获取任务
            compactTasks.addAll(tasks); // 将任务添加到任务队列
            if (tasks.isEmpty()) {
                break; // 如果没有更多任务，退出循环
            }
        }
    }

    // 处理时间回调
    @Override
    public void onProcessingTime(long time) {
        while (mailbox.isIdle()) { // 当邮箱空闲时
            for (int i = 0; i < EMIT_PER_BATCH; i++) {
                UnawareAppendCompactionTask task = compactTasks.poll(); // 从任务队列获取任务
                if (task == null) {
                    return; // 如果没有任务，退出循环
                }
                output.collect(new StreamRecord<>(Either.Right(task))); // 发送压缩任务
            }
        }
    }

    // 处理输入元素
    @Override
    public void processElement(StreamRecord<CommitT> record) throws Exception {
        output.collect(new StreamRecord<>(Either.Left(record.getValue()))); // 发送提交消息
    }

    @Override
    public void close() throws Exception {
        // 优雅地关闭线程池
        ExecutorUtils.gracefulShutdown(1, TimeUnit.MINUTES, executorService);
        super.close();
    }
}
