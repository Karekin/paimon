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

import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.flink.VersionedSerializerWrapper;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.utils.SerializableSupplier;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;

import java.util.ArrayList;
import java.util.List;

/**
 * 一个 {@link CommittableStateManager} 实现类，用于在状态中存储未提交的 {@link ManifestCommittable}。
 *
 * <p>当作业重启时，这些 {@link ManifestCommittable} 将被恢复并提交，然后将抛出一个有意的故障（Intended Failure），
 * 希望在作业重启后，所有写入器能够基于恢复的快照重新开始写入。
 *
 * <p>适用于提交包含记录的快照。例如，由表存储写入器生成的快照。
 */
public class RestoreAndFailCommittableStateManager<GlobalCommitT>
        implements CommittableStateManager<GlobalCommitT> {

    private static final long serialVersionUID = 1L;

    /** 可提交状态的序列化工厂。 */
    private final SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer;

    /** 作业的全局提交状态。用于过滤掉之前的成功提交。 */
    private ListState<GlobalCommitT> streamingCommitterState;

    public RestoreAndFailCommittableStateManager(
            SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer) {
        this.committableSerializer = committableSerializer;
    }

    @Override
    public void initializeState(
            StateInitializationContext context, Committer<?, GlobalCommitT> committer)
            throws Exception {
        // 初始化状态，从 Flink 的 Operator 状态存储中读取之前的 GlobalCommitT 状态
        streamingCommitterState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(
                                        new ListStateDescriptor<>(
                                                "streaming_committer_raw_states",
                                                BytePrimitiveArraySerializer.INSTANCE)),
                        new VersionedSerializerWrapper<>(committableSerializer.get()));
        // 从状态存储中恢复未提交的状态
        List<GlobalCommitT> restored = new ArrayList<>();
        streamingCommitterState.get().forEach(restored::add);
        // 清空状态存储，避免重复处理
        streamingCommitterState.clear();
        // 恢复并提交状态
        recover(restored, committer);
    }

    private void recover(List<GlobalCommitT> committables, Committer<?, GlobalCommitT> committer)
            throws Exception {
        // 调用 Committer 的 filterAndCommit 方法，过滤并提交恢复的状态
        int numCommitted = committer.filterAndCommit(committables);
        // 如果提交了状态，主动抛出异常以触动作业重启
        if (numCommitted > 0) {
            throw new RuntimeException(
                    "This exception is intentionally thrown "
                            + "after committing the restored checkpoints. "
                            + "By restarting the job we hope that "
                            + "writers can start writing based on these new commits.");
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context, List<GlobalCommitT> committables)
            throws Exception {
        // 在 Flink 的 Checkpoint 机制中保存当前的 GlobalCommitT 状态
        streamingCommitterState.update(committables);
    }
}
