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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.TagCreationMode;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SerializableRunnable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.END_INPUT_WATERMARK;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_AUTO_TAG_FOR_SAVEPOINT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_CPU;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_OPERATOR_CHAINING;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_MANAGED_WRITER_BUFFER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.flink.utils.ManagedMemoryUtils.declareManagedMemory;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Paimon的抽象Sink类。
 * 定义了将数据Sink到Flink的抽象类，并提供了一系列方法来管理数据的写入和提交。
 */
public abstract class FlinkSink<T> implements Serializable {

    private static final long serialVersionUID = 1L; // 序列化版本号

    private static final String WRITER_NAME = "Writer"; // 写入操作的名称标识
    private static final String WRITER_WRITE_ONLY_NAME = "Writer(write-only)"; // 写入操作（仅写）的名称标识
    private static final String GLOBAL_COMMITTER_NAME = "Global Committer"; // 全局提交操作的名称标识

    protected final FileStoreTable table; // 存储表对象，用于管理数据的存储
    private final boolean ignorePreviousFiles; // 是否忽略之前的文件

    public FlinkSink(FileStoreTable table, boolean ignorePreviousFiles) {
        this.table = table; // 初始化存储表对象
        this.ignorePreviousFiles = ignorePreviousFiles; // 初始化是否忽略之前的文件
    }

    // 创建写入操作提供者
    private StoreSinkWrite.Provider createWriteProvider(
            CheckpointConfig checkpointConfig, // Flink的检查点配置
            boolean isStreaming, // 是否为流式处理
            boolean hasSinkMaterializer) // 是否有SinkMaterializer
    {
        // 断言：不能使用SinkMaterializer
        SerializableRunnable assertNoSinkMaterializer = () -> {
            // 检查是否使用了SinkMaterializer，如果使用，抛出异常并提示配置
            Preconditions.checkArgument(
                    !hasSinkMaterializer,
                    String.format(
                            "Sink materializer must not be used with Paimon sink. "
                                    + "Please set '%s' to '%s' in Flink's config.",
                            ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE.key(),
                            ExecutionConfigOptions.UpsertMaterialize.NONE.name()));
        };

        Options options = table.coreOptions().toConfiguration(); // 获取存储表的核心配置
        ChangelogProducer changelogProducer = table.coreOptions().changelogProducer(); // 获取变更日志生成器类型
        boolean waitCompaction; // 是否等待Compaction
        CoreOptions coreOptions = table.coreOptions(); // 存储表的核心选项

        // 判断是否为仅写模式
        if (coreOptions.writeOnly()) {
            waitCompaction = false; // 仅写模式下，不等待Compaction
        } else {
            waitCompaction = coreOptions.prepareCommitWaitCompaction(); // 非仅写模式下，是否等待Compaction
            int deltaCommits = -1; // 初始化delta提交次数

            // 检查是否配置了全量Compaction的触发条件
            if (options.contains(FULL_COMPACTION_DELTA_COMMITS)) {
                deltaCommits = options.get(FULL_COMPACTION_DELTA_COMMITS); // 获取配置值
            } else if (options.contains(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL)) {
                // 计算基于时间的触发间隔
                long fullCompactionThresholdMs =
                        options.get(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL).toMillis();
                deltaCommits = (int) (fullCompactionThresholdMs / checkpointConfig.getCheckpointInterval());
            }

            // 根据变更日志生成器类型和delta提交次数，选择不同的写入操作提供者
            if (changelogProducer == ChangelogProducer.FULL_COMPACTION || deltaCommits >= 0) {
                int finalDeltaCommits = Math.max(deltaCommits, 1); // 确保delta提交次数至少为1
                return (table, commitUser, state, ioManager, memoryPool, metricGroup) -> {
                    assertNoSinkMaterializer.run(); // 检查SinkMaterializer的使用
                    // 返回全局全量Compaction写入操作提供者
                    return new GlobalFullCompactionSinkWrite(
                            table,
                            commitUser,
                            state,
                            ioManager,
                            ignorePreviousFiles,
                            waitCompaction,
                            finalDeltaCommits,
                            isStreaming,
                            memoryPool,
                            metricGroup);
                };
            }
        }

        // 根据是否需要Lookup操作和是否等待Compaction，选择不同的写入操作提供者
        if (coreOptions.needLookup() && !coreOptions.prepareCommitWaitCompaction()) {
            return (table, commitUser, state, ioManager, memoryPool, metricGroup) -> {
                assertNoSinkMaterializer.run(); // 检查SinkMaterializer的使用
                // 返回异步Lookup写入操作提供者
                return new AsyncLookupSinkWrite(
                        table,
                        commitUser,
                        state,
                        ioManager,
                        ignorePreviousFiles,
                        waitCompaction,
                        isStreaming,
                        memoryPool,
                        metricGroup);
            };
        }

        // 默认写入操作提供者
        return (table, commitUser, state, ioManager, memoryPool, metricGroup) -> {
            assertNoSinkMaterializer.run(); // 检查SinkMaterializer的使用
            // 返回默认的存储写入操作提供者
            return new StoreSinkWriteImpl(
                    table,
                    commitUser,
                    state,
                    ioManager,
                    ignorePreviousFiles,
                    waitCompaction,
                    isStreaming,
                    memoryPool,
                    metricGroup);
        };
    }

    // 从DataStream创建Sink
    public DataStreamSink<?> sinkFrom(DataStream<T> input) {
        // 创建初始Commit用户
        return sinkFrom(input, createCommitUser(table.coreOptions().toConfiguration()));
    }

    // 从DataStream创建Sink并指定初始Commit用户
    public DataStreamSink<?> sinkFrom(DataStream<T> input, String initialCommitUser) {
        // 执行写入操作，产生可提交的内容
        DataStream<Committable> written =
                doWrite(input, initialCommitUser, input.getParallelism());
        // 提交可提交的内容
        return doCommit(written, initialCommitUser);
    }

    // 检查是否有SinkMaterializer
    private boolean hasSinkMaterializer(DataStream<T> input) {
        Set<Integer> visited = new HashSet<>(); // 记录已访问的Transformation
        Queue<Transformation<?>> queue = new LinkedList<>(); // 广度优先搜索队列
        queue.add(input.getTransformation()); // 添加初始Transformation
        visited.add(input.getTransformation().getId()); // 记录初始Transformation的ID

        // 遍历Transformation图
        while (!queue.isEmpty()) {
            Transformation<?> transformation = queue.poll(); // 获取当前Transformation
            // 检查是否包含SinkMaterializer
            if (transformation.getName().startsWith("SinkMaterializer")) {
                return true; // 包含SinkMaterializer
            }
            // 继续遍历输入的Transformation
            for (Transformation<?> prev : transformation.getInputs()) {
                if (!visited.contains(prev.getId())) {
                    queue.add(prev); // 添加未访问的Transformation到队列
                    visited.add(prev.getId()); // 记录已访问
                }
            }
        }
        return false; // 未找到SinkMaterializer
    }

    // 执行写入操作
    public DataStream<Committable> doWrite(
            DataStream<T> input, String commitUser, @Nullable Integer parallelism) {
        StreamExecutionEnvironment env = input.getExecutionEnvironment(); // 获取Flink执行环境
        boolean isStreaming = isStreaming(input); // 判断是否为流式处理

        boolean writeOnly = table.coreOptions().writeOnly(); // 是否为仅写模式
        SingleOutputStreamOperator<Committable> written =
                input.transform(
                                // 根据是否为仅写模式生成不同的操作名称
                                (writeOnly ? WRITER_WRITE_ONLY_NAME : WRITER_NAME) + " : " + table.name(),
                                new CommittableTypeInfo(), // 类型信息
                                createWriteOperator(
                                        createWriteProvider(
                                                env.getCheckpointConfig(), // 获取检查点配置
                                                isStreaming,
                                                hasSinkMaterializer(input)), // 是否有SinkMaterializer
                                        commitUser))
                        // 设置并行度
                        .setParallelism(parallelism == null ? input.getParallelism() : parallelism);

        boolean writeMCacheEnabled = table.coreOptions().writeManifestCache().getBytes() > 0; // 是否启用Manifest缓存
        boolean hashDynamicMode = table.bucketMode() == BucketMode.HASH_DYNAMIC; // 是否为动态Hash桶模式

        // 如果是非流式处理且启用了Manifest缓存或动态桶模式，需要断言批处理模式的自适应并行度配置
        if (!isStreaming && (writeMCacheEnabled || hashDynamicMode)) {
            assertBatchAdaptiveParallelism(
                    env, written.getParallelism(), writeMCacheEnabled, hashDynamicMode);
        }

        Options options = Options.fromMap(table.options()); // 获取存储表的选项
        // 如果启用了使用托管内存，声明内存管理
        if (options.get(SINK_USE_MANAGED_MEMORY)) {
            declareManagedMemory(
                    written,
                    options.get(SINK_MANAGED_WRITER_BUFFER_MEMORY));
        }
        return written; // 返回写入后的DataStream
    }

    // 执行提交操作
    protected DataStreamSink<?> doCommit(DataStream<Committable> written, String commitUser) {
        StreamExecutionEnvironment env = written.getExecutionEnvironment(); // 获取Flink执行环境
        ReadableConfig conf = env.getConfiguration(); // 获取Flink配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig(); // 获取检查点配置
        boolean streamingCheckpointEnabled =
                isStreaming(written) && checkpointConfig.isCheckpointingEnabled(); // 是否启用了流式检查点

        if (streamingCheckpointEnabled) {
            assertStreamingConfiguration(env); // 断言流式配置
        }

        Options options = Options.fromMap(table.options()); // 获取存储表的选项
        // 创建CommitterOperator
        OneInputStreamOperator<Committable, Committable> committerOperator =
                new CommitterOperator<>(
                        streamingCheckpointEnabled,
                        true,
                        options.get(SINK_COMMITTER_OPERATOR_CHAINING),
                        commitUser,
                        createCommitterFactory(), // CommittableFactory
                        createCommittableStateManager(), // CommittableStateManager
                        options.get(END_INPUT_WATERMARK)); // 结束输入的Watermark

        // 根据配置，可能包装为AutoTagForSavepointCommitterOperator
        if (options.get(SINK_AUTO_TAG_FOR_SAVEPOINT)) {
            committerOperator =
                    new AutoTagForSavepointCommitterOperator<>(
                            (CommitterOperator<Committable, ManifestCommittable>) committerOperator,
                            table::snapshotManager,
                            table::tagManager,
                            () -> table.store().newTagDeletion(),
                            () -> table.store().createTagCallbacks(),
                            table.coreOptions().tagDefaultTimeRetained());
        }

        // 根据配置，可能包装为BatchWriteGeneratorTagOperator
        if (conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.BATCH
                && table.coreOptions().tagCreationMode() == TagCreationMode.BATCH) {
            committerOperator =
                    new BatchWriteGeneratorTagOperator<>(
                            (CommitterOperator<Committable, ManifestCommittable>) committerOperator,
                            table);
        }

        // 转换为全局提交操作
        SingleOutputStreamOperator<?> committed =
                written.transform(
                                GLOBAL_COMMITTER_NAME + " : " + table.name(), // 操作名称
                                new CommittableTypeInfo(), // 类型信息
                                committerOperator)
                        .setParallelism(1) // 设置并行度为1
                        .setMaxParallelism(1); // 设置最大并行度为1

        // 配置全局提交者
        configureGlobalCommitter(
                committed,
                options.get(SINK_COMMITTER_CPU),
                options.get(SINK_COMMITTER_MEMORY));
        // 添加DiscardingSink并返回
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    // 配置全局提交者
    public static void configureGlobalCommitter(
            SingleOutputStreamOperator<?> committed,
            double cpuCores,
            @Nullable MemorySize heapMemory) {
        if (heapMemory == null) {
            return; // 如果没有设置堆内存，直接返回
        }

        // 创建SlotSharingGroup并应用到全局提交者
        SlotSharingGroup slotSharingGroup =
                SlotSharingGroup.newBuilder(committed.getName())
                        .setCpuCores(cpuCores)
                        .setTaskHeapMemory(
                                new org.apache.flink.configuration.MemorySize(
                                        heapMemory.getBytes()))
                        .build();
        committed.slotSharingGroup(slotSharingGroup);
    }

    // 断言流式配置
    public static void assertStreamingConfiguration(StreamExecutionEnvironment env) {
        checkArgument(
                !env.getCheckpointConfig().isUnalignedCheckpointsEnabled(),
                "Paimon sink currently does not support unaligned checkpoints. Please set "
                        + ExecutionCheckpointingOptions.ENABLE_UNALIGNED.key()
                        + " to false.");
        checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "Paimon sink currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key()
                        + " to exactly-once");
    }

    // 断言批处理自适应并行度配置
    public static void assertBatchAdaptiveParallelism(
            StreamExecutionEnvironment env, int sinkParallelism) {
        assertBatchAdaptiveParallelism(
                env, sinkParallelism, "Paimon Sink does not support Flink's Adaptive Parallelism mode. Please manually turn it off or set Paimon `sink.parallelism` manually.");
    }

    // 断言批处理自适应并行度配置（带详细错误信息）
    public static void assertBatchAdaptiveParallelism(
            StreamExecutionEnvironment env,
            int sinkParallelism,
            boolean writeMCacheEnabled,
            boolean hashDynamicMode) {
        List<String> messages = new ArrayList<>(); // 收集错误信息
        if (writeMCacheEnabled) {
            messages.add("Write Manifest Cache");
        }
        if (hashDynamicMode) {
            messages.add("Dynamic Bucket Mode");
        }
        String msg =
                String.format(
                        "Paimon Sink with %s does not support Flink's Adaptive Parallelism mode. "
                                + "Please manually turn it off or set Paimon `sink.parallelism` manually.",
                        messages);
        assertBatchAdaptiveParallelism(env, sinkParallelism, msg);
    }

    // 断言批处理自适应并行度配置（带自定义错误信息）
    public static void assertBatchAdaptiveParallelism(
            StreamExecutionEnvironment env, int sinkParallelism, String exceptionMsg) {
        try {
            checkArgument(
                    sinkParallelism != -1 || !AdaptiveParallelism.isEnabled(env), exceptionMsg);
        } catch (NoClassDefFoundError ignored) {
            // Flink 1.17之前没有自适应并行度支持
        }
    }

    // 抽象方法：创建写入操作操作符
    protected abstract OneInputStreamOperator<T, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, // 写入操作提供者
            String commitUser // Commit用户
    );

    // 抽象方法：创建提交者工厂
    protected abstract Committer.Factory<Committable, ManifestCommittable> createCommitterFactory();

    // 抽象方法：创建可提交状态管理器
    protected abstract CommittableStateManager<ManifestCommittable> createCommittableStateManager();

    // 判断是否为流式处理（基于DataStream）
    public static boolean isStreaming(DataStream<?> input) {
        return isStreaming(input.getExecutionEnvironment());
    }

    // 判断是否为流式处理（基于StreamExecutionEnvironment）
    public static boolean isStreaming(StreamExecutionEnvironment env) {
        return env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                == RuntimeExecutionMode.STREAMING;
    }
}
