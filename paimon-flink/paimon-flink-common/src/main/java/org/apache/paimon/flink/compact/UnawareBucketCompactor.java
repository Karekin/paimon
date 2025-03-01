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

package org.apache.paimon.flink.compact;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 无意识桶表的合并器，用于执行 {@link UnawareAppendCompactionTask}。
 * 这个类负责管理对文件存储表的合并操作，处理相关的任务和资源。
 */
public class UnawareBucketCompactor {

    // 表示文件存储表，是数据存储的主要对象
    private final FileStoreTable table;

    // 表示提交数据的用户，用于标识操作的来源
    private final String commitUser;

    // 负责写入操作的文件存储写入器，用于后续的合并操作
    private final transient AppendOnlyFileStoreWrite write;

    // 保存任务结果的队列，用于暂存合并任务执行后的结果
    protected final transient Queue<Future<CommitMessage>> result;

    // 提供线程池的供应商，用于动态提供线程池，避免直接创建线程池
    private final transient Supplier<ExecutorService> compactExecutorsupplier;

    /**
     * 构造函数，初始化合并器的基本组件。
     * 这包括文件存储表、用户信息、写入器、任务结果队列和线程池供应商。
     *
     * @param table 文件存储表，用于存储和管理数据
     * @param commitUser 提交数据的用户
     * @param lazyCompactExecutor 提供线程池的供应商
     */
    public UnawareBucketCompactor(
            FileStoreTable table,
            String commitUser,
            Supplier<ExecutorService> lazyCompactExecutor
    ) {
        // 初始化文件存储表
        this.table = table;

        // 设置提交数据的用户信息
        this.commitUser = commitUser;

        // 初始化写入器，用于后续的写入操作
        this.write = (AppendOnlyFileStoreWrite) table.store().newWrite(commitUser);

        // 初始化任务结果队列
        this.result = new LinkedList<>();

        // 设置线程池供应商
        this.compactExecutorsupplier = lazyCompactExecutor;
    }

    /**
     * 处理单个合并任务并将其提交到执行线程池。
     * 任务结果会被添加到任务结果队列中。
     *
     * @param task 要处理的合并任务
     * @throws Exception 如果任务执行过程中发生异常
     */
    public void processElement(UnawareAppendCompactionTask task) throws Exception {
        // 从线程池供应商中获取线程池，并提交任务
        result.add(compactExecutorsupplier.get().submit(() -> {
            // 调用任务的 doCompact 方法，执行合并操作并返回结果
            return task.doCompact(table, write);
        }));
    }

    /**
     * 关闭合并器，并释放相关资源。
     * 最终调用 shutdown 方法来完成资源释放。
     *
     * @throws Exception 如果资源释放过程中发生异常
     */
    public void close() throws Exception {
        // 调用 shutdown 方法，释放资源
        shutdown();
    }

    /**
     * 用于测试的目的，关闭线程池并清理资源。
     * 此方法会处理任务结果队列中的所有已完成任务。
     * 如果任务队列中的任务尚未完成，将停止后续的处理。
     *
     * @throws Exception 如果资源释放过程中发生异常
     */
    @VisibleForTesting
    void shutdown() throws Exception {
        // 创建一个列表，用于存储最终的提交消息
        List<CommitMessage> messages = new ArrayList<>();

        // 遍历任务结果队列
        for (Future<CommitMessage> resultFuture : result) {
            // 如果任务尚未完成，停止后续处理
            if (!resultFuture.isDone()) {
                break;
            }
            try {
                // 获取任务结果并添加到列表中
                messages.add(resultFuture.get());
            } catch (Exception exception) {
                // 捕获异常，但这里认为异常已经处理过了
            }
        }

        // 如果没有任务结果，直接返回
        if (messages.isEmpty()) {
            return;
        }

        // 创建表提交对象，准备提交数据
        try (TableCommitImpl tableCommit = table.newCommit(commitUser)) {
            // 调用 abort 方法，处理异常或需要中止的任务
            tableCommit.abort(messages);
        }
    }

    /**
     * 准备提交结果，生成可提交的列表。
     * 该方法会清理任务结果队列，并生成一个可提交的列表。
     * 如果 waitCompaction 为 true，会等待所有任务完成；否则，只处理已完成的任务。
     *
     * @param waitCompaction 是否等待所有任务完成
     * @param checkpointId 检查点 ID，用于标识提交点
     * @return 可提交的列表，包含提交消息
     * @throws IOException 如果准备提交过程中发生 I/O 异常
     */
    public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId) throws IOException {
        // 创建一个临时列表，用于存储提交消息
        List<CommitMessage> tempList = new ArrayList<>();

        try {
            // 遍历任务结果队列
            while (!result.isEmpty()) {
                Future<CommitMessage> future = result.peek();

                // 如果任务尚未完成且不需要等待，跳出循环
                if (!future.isDone() && !waitCompaction) {
                    break;
                }

                // 移除队列中的任务结果
                result.poll();

                // 获取任务结果并添加到临时列表
                tempList.add(future.get());
            }

            // 生成可提交的对象列表
            return tempList.stream()
                    .map(s -> new Committable(checkpointId, Committable.Kind.FILE, s))
                    .collect(Collectors.toList());
        } catch (InterruptedException e) {
            // 处理中断异常
            throw new RuntimeException("在等待任务完成时被中断。", e);
        } catch (Exception e) {
            // 捕获其他异常
            throw new RuntimeException("在执行合并操作时遇到错误。", e);
        }
    }

    /**
     * 获取任务结果队列的引用。
     * 可用于外部获取任务结果。
     *
     * @return 任务结果队列
     */
    public Iterable<Future<CommitMessage>> result() {
        return result;
    }
}