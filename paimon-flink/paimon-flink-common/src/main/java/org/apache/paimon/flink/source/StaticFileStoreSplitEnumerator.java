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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.source.assigners.DynamicPartitionPruningAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 为 {@link StaticFileStoreSource} 输入实现的 {@link SplitEnumerator}。
 */
public class StaticFileStoreSplitEnumerator
        implements SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final Logger LOG = LoggerFactory.getLogger(StaticFileStoreSplitEnumerator.class);

    private final SplitEnumeratorContext<FileStoreSourceSplit> context; // 分片枚举器上下文

    @Nullable private final Snapshot snapshot; // 快照

    private SplitAssigner splitAssigner; // 分片分配器

    @Nullable private final DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo; // 动态分区过滤信息

    public StaticFileStoreSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            @Nullable Snapshot snapshot,
            SplitAssigner splitAssigner) {
        this(context, snapshot, splitAssigner, null); // 调用另一个构造函数
    }

    public StaticFileStoreSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            @Nullable Snapshot snapshot,
            SplitAssigner splitAssigner,
            @Nullable DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo) {
        this.context = context; // 初始化上下文
        this.snapshot = snapshot; // 初始化快照
        this.splitAssigner = splitAssigner; // 初始化分片分配器
        this.dynamicPartitionFilteringInfo = dynamicPartitionFilteringInfo; // 初始化动态分区过滤信息
    }

    @Override
    public void start() {
        // 无需启动任何资源
    }

    @Override
    public void handleSplitRequest(int subtask, @Nullable String hostname) {
        if (!context.registeredReaders().containsKey(subtask)) {
            // 如果在发送请求和现在之间读取器失败了，跳过此请求
            return;
        }

        List<FileStoreSourceSplit> assignment = splitAssigner.getNext(subtask, hostname); // 获取分片
        if (assignment.size() > 0) {
            context.assignSplits(
                    new SplitsAssignment<>(Collections.singletonMap(subtask, assignment))); // 分配分片
        } else {
            context.signalNoMoreSplits(subtask); // 通知没有更多分片
        }
    }

    @Override
    public void addSplitsBack(List<FileStoreSourceSplit> backSplits, int subtaskId) {
        splitAssigner.addSplitsBack(subtaskId, backSplits); // 将分片添加回分配器
    }

    @Override
    public void addReader(int subtaskId) {
        // 该源是纯懒加载拉取模式，注册时无需执行任何操作
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) {
        // 创建检查点，包含剩余分片和快照 ID
        return new PendingSplitsCheckpoint(
                splitAssigner.remainingSplits(), snapshot == null ? null : snapshot.id());
    }

    @Override
    public void close() {
        // 无需关闭任何资源
    }

    @Nullable
    public Snapshot snapshot() {
        return snapshot; // 返回快照
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof ReaderConsumeProgressEvent) {
            // 批处理读取不处理消费者进度事件
            // 避免无意义的错误日志
            return;
        }

        if (sourceEvent.getClass().getSimpleName().equals("DynamicFilteringEvent")) {
            // 检查动态分区过滤信息是否已设置
            checkNotNull(
                    dynamicPartitionFilteringInfo,
                    "无法应用动态过滤，因为 dynamicPartitionFilteringInfo 尚未设置。");
            // 根据动态过滤事件更新分片分配器
            this.splitAssigner =
                    DynamicPartitionPruningAssigner.createDynamicPartitionPruningAssignerIfNeeded(
                            subtaskId,
                            splitAssigner,
                            dynamicPartitionFilteringInfo.getPartitionRowProjection(),
                            sourceEvent,
                            LOG);
        } else {
            LOG.error("收到无法识别的事件: {}", sourceEvent); // 记录无法识别的事件
        }
    }

    @VisibleForTesting
    public SplitAssigner getSplitAssigner() {
        return splitAssigner; // 返回分片分配器
    }
}
