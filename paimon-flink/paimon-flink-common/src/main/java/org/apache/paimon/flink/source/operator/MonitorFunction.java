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

package org.apache.paimon.flink.source.operator;

import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;

import static org.apache.paimon.table.BucketMode.BUCKET_UNAWARE;

/**
 * 这是单个（非并行）监控任务，它负责以下功能：
 *
 * <ol>
 *   <li>监听 Paimon 表的快照。
 *   <li>创建与增量文件对应的切片。
 *   <li>将这些切片分配给下游任务进行进一步处理。
 * </ol>
 *
 * <p>需要读取的切片将转发给下游的 {@link ReadOperator}，其并行度可以大于 1。
 *
 * <p>目前，有两个功能依赖于这个监控任务：
 *
 * <ol>
 *   <li>Consumer-id：依赖此功能进行对齐快照消费，并确保每个检查点内都消费快照中的所有数据。
 *   <li>Snapshot-watermark：当没有定义水印时，默认 Paimon 表将传递快照中记录的水印。
 * </ol>
 */
public class MonitorFunction extends RichSourceFunction<Split>
        implements CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MonitorFunction.class);

    // 读取构建器
    private final ReadBuilder readBuilder;

    // 监控间隔
    private final long monitorInterval;

    // 是否需要发出快照水印
    private final boolean emitSnapshotWatermark;

    // 运行状态的标识符
    private volatile boolean isRunning = true;

    // 当前的流式扫描
    private transient StreamTableScan scan;

    // Flink 的 SourceContext，用于发出切片和水印
    private transient SourceContext<Split> ctx;

    // 针对 Flink 的 Checkpoint 状态存储，用于保存 next-snapshot
    private transient ListState<Long> checkpointState;

    // 针对 Flink 的 Checkpoint 状态存储，用于保存每个检查点对应的 next-snapshot
    private transient ListState<Tuple2<Long, Long>> nextSnapshotState;

    // 用于保存未处理的快照任务，键为检查点 ID，值为对应的 next-snapshot
    private transient TreeMap<Long, Long> nextSnapshotPerCheckpoint;

    /**
     * 构造函数，初始化读取构建器、监控间隔和是否发出快照水印。
     *
     * @param readBuilder 读取构建器
     * @param monitorInterval 监控间隔
     * @param emitSnapshotWatermark 是否需要发出快照水印
     */
    public MonitorFunction(
            ReadBuilder readBuilder, long monitorInterval, boolean emitSnapshotWatermark) {
        this.readBuilder = readBuilder;
        this.monitorInterval = monitorInterval;
        this.emitSnapshotWatermark = emitSnapshotWatermark;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 初始化流式扫描
        this.scan = readBuilder.newStreamScan();

        // 获取 Flink 的 ListState，用于保存 next-snapshot
        this.checkpointState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "next-snapshot", LongSerializer.INSTANCE));

        // 获取 Flink 的 ListState，用于保存每个检查点对应的 next-snapshot
        @SuppressWarnings("unchecked")
        final Class<Tuple2<Long, Long>> typedTuple =
                (Class<Tuple2<Long, Long>>) (Class<?>) Tuple2.class;
        this.nextSnapshotState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "next-snapshot-per-checkpoint",
                                        new TupleSerializer<>(
                                                typedTuple,
                                                new TypeSerializer[] {
                                                        LongSerializer.INSTANCE, LongSerializer.INSTANCE
                                                })));

        // 初始化未处理的快照任务
        this.nextSnapshotPerCheckpoint = new TreeMap<>();

        // 如果是从状态恢复
        if (context.isRestored()) {
            LOG.info("正在恢复 {} 的状态。", getClass().getSimpleName());

            // 获取已保存的 next-snapshot 状态
            List<Long> retrievedStates = new ArrayList<>();
            for (Long entry : this.checkpointState.get()) {
                retrievedStates.add(entry);
            }

            // 由于本函数的并行度是 1，所以只能有一个恢复项
            Preconditions.checkArgument(
                    retrievedStates.size() <= 1,
                    "{} 恢复了无效状态。",
                    getClass().getSimpleName());

            // 如果有恢复项，恢复流式扫描到该快照
            if (retrievedStates.size() == 1) {
                this.scan.restore(retrievedStates.get(0));
            }

            // 恢复每个检查点对应的 next-snapshot
            for (Tuple2<Long, Long> tuple2 : nextSnapshotState.get()) {
                nextSnapshotPerCheckpoint.put(tuple2.f0, tuple2.f1);
            }
        } else {
            LOG.info("没有状态需要恢复。");
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        // 清空 CheckpointState
        this.checkpointState.clear();

        // 获取当前的 next-snapshot
        Long nextSnapshot = this.scan.checkpoint();

        if (nextSnapshot != null) {
            this.checkpointState.add(nextSnapshot); // 保存 next-snapshot 到 CheckpointState

            // 将 current checkpoint id 和 next-snapshot 记录到 nextSnapshotPerCheckpoint
            this.nextSnapshotPerCheckpoint.put(ctx.getCheckpointId(), nextSnapshot);
        }

        // 将 nextSnapshotPerCheckpoint 转换为 List<Tuple2<Long, Long>> 并更新到 nextSnapshotState
        List<Tuple2<Long, Long>> nextSnapshots = new ArrayList<>();
        this.nextSnapshotPerCheckpoint.forEach((k, v) -> nextSnapshots.add(new Tuple2<>(k, v)));
        this.nextSnapshotState.update(nextSnapshots);

        if (LOG.isDebugEnabled()) {
            LOG.debug("{} 的 Checkpoint: {}。", getClass().getSimpleName(), nextSnapshot);
        }
    }

    @SuppressWarnings("BusyWait")
    @Override
    public void run(SourceContext<Split> ctx) throws Exception {
        this.ctx = ctx;
        while (isRunning) {
            boolean isEmpty;
            synchronized (ctx.getCheckpointLock()) {
                if (!isRunning) {
                    return;
                }

                try {
                    // 获取最新的切片
                    List<Split> splits = scan.plan().splits();
                    isEmpty = splits.isEmpty();

                    // 发出切片
                    splits.forEach(ctx::collect);

                    // 如果需要发出快照水印，则获取当前快照水印并发出
                    if (emitSnapshotWatermark) {
                        Long watermark = scan.watermark();
                        if (watermark != null) {
                            ctx.emitWatermark(new Watermark(watermark));
                        }
                    }
                } catch (EndOfScanException esf) {
                    LOG.info("捕获 EndOfStreamException，流结束。");
                    return;
                }
            }

            // 如果没有切片，则休眠一段时间
            if (isEmpty) {
                Thread.sleep(monitorInterval);
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // 通知 Paimon 表某个 Checkpoint 完成
        NavigableMap<Long, Long> nextSnapshots =
                nextSnapshotPerCheckpoint.headMap(checkpointId, true);
        OptionalLong max = nextSnapshots.values().stream().mapToLong(Long::longValue).max();
        max.ifPresent(scan::notifyCheckpointComplete);
        nextSnapshots.clear();
    }

    @Override
    public void cancel() {
        // 取消任务时，重置 isRunning 标识符
        // 这是为了避免在 run() 之前调用 cancel()
        if (ctx != null) {
            synchronized (ctx.getCheckpointLock()) {
                isRunning = false;
            }
        } else {
            isRunning = false;
        }
    }

    /**
     * 构建数据流。
     *
     * @param env Flink 执行环境
     * @param name 数据源名称
     * @param typeInfo 类型信息
     * @param readBuilder 读取构建器
     * @param monitorInterval 监控间隔
     * @param emitSnapshotWatermark 是否需要发出快照水印
     * @param shuffleBucketWithPartition 是否按分区和桶进行混合
     * @param bucketMode 数据桶模式
     * @return 数据流
     */
    public static DataStream<RowData> buildSource(
            StreamExecutionEnvironment env,
            String name,
            TypeInformation<RowData> typeInfo,
            ReadBuilder readBuilder,
            long monitorInterval,
            boolean emitSnapshotWatermark,
            boolean shuffleBucketWithPartition,
            BucketMode bucketMode) {
        // 创建 MonitorFunction 的单例数据流
        SingleOutputStreamOperator<Split> singleOutputStreamOperator =
                env.addSource(
                                new MonitorFunction(
                                        readBuilder, monitorInterval, emitSnapshotWatermark),
                                name + "-Monitor",
                                new JavaTypeInfo<>(Split.class))
                        .forceNonParallel();

        // 根据 BucketMode 配置数据分发方式
        DataStream<Split> sourceDataStream;
        if (bucketMode == BUCKET_UNAWARE) {
            sourceDataStream = shuffleUnwareBucket(singleOutputStreamOperator);
        } else {
            sourceDataStream = shuffleNonUnwareBucket(
                    singleOutputStreamOperator, shuffleBucketWithPartition);
        }

        // 将数据流转换为 RowData 类型
        return sourceDataStream.transform(
                name + "-Reader", typeInfo, new ReadOperator(readBuilder));
    }

    /**
     * 配置为未知数据桶时的分发方式（均衡分发）。
     *
     * @param singleOutputStreamOperator 单例数据流
     * @return 数据流
     */
    private static DataStream<Split> shuffleUnwareBucket(
            SingleOutputStreamOperator<Split> singleOutputStreamOperator) {
        return singleOutputStreamOperator.rebalance();
    }

    /**
     * 配置为已知数据桶时的分发方式（按分区和桶进行混合）。
     *
     * @param singleOutputStreamOperator 单例数据流
     * @param shuffleBucketWithPartition 是否按分区和桶进行混合
     * @return 数据流
     */
    private static DataStream<Split> shuffleNonUnwareBucket(
            SingleOutputStreamOperator<Split> singleOutputStreamOperator,
            boolean shuffleBucketWithPartition) {
        return singleOutputStreamOperator.partitionCustom(
                (key, numPartitions) -> {
                    // 根据分区和桶选择目标分区
                    if (shuffleBucketWithPartition) {
                        return ChannelComputer.select(key.f0, key.f1, numPartitions);
                    }
                    return ChannelComputer.select(key.f1, numPartitions);
                },
                split -> {
                    DataSplit dataSplit = (DataSplit) split;
                    return Tuple2.of(dataSplit.partition(), dataSplit.bucket());
                });
    }
}
