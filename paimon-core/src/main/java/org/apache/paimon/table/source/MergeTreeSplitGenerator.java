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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.utils.BinPacking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;

/** Merge tree implementation of {@link SplitGenerator}. */
public class MergeTreeSplitGenerator implements SplitGenerator {

    // 用于比较记录的比较器，通常用于按主键排序
    private final Comparator<InternalRow> keyComparator;

    // 目标分裂大小，用于控制分裂的大小
    private final long targetSplitSize;

    // 打开文件的成本，用于衡量打开文件的代价
    private final long openFileCost;

    // 是否启用删除向量
    private final boolean deletionVectorsEnabled;

    // 合并引擎类型，用于控制数据文件的合并逻辑
    private final MergeEngine mergeEngine;

    // 构造函数，初始化分裂生成器的参数
    public MergeTreeSplitGenerator(
            Comparator<InternalRow> keyComparator,
            long targetSplitSize,
            long openFileCost,
            boolean deletionVectorsEnabled,
            MergeEngine mergeEngine) {
        this.keyComparator = keyComparator;
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.mergeEngine = mergeEngine;
    }

    // 判断是否始终可以转换为原始数据
    @Override
    public boolean alwaysRawConvertible() {
        // 如果启用了删除向量，或者合并引擎是 FIRST_ROW，则可以转换为原始数据
        return deletionVectorsEnabled || mergeEngine == FIRST_ROW;
    }

    // 为批量扫描生成数据分裂
    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> files) {
        // 判断文件是否都可以不需要删除操作，并且所有文件的级别相同
        boolean rawConvertible =
                files.stream().allMatch(file -> file.level() != 0 && withoutDeleteRow(file));
        boolean oneLevel =
                files.stream().map(DataFileMeta::level).collect(Collectors.toSet()).size() == 1;

        if (rawConvertible && (deletionVectorsEnabled || mergeEngine == FIRST_ROW || oneLevel)) {
            // 计算每个文件的权重
            Function<DataFileMeta, Long> weightFunc =
                    file -> Math.max(file.fileSize(), openFileCost);
            return BinPacking.packForOrdered(files, weightFunc, targetSplitSize).stream()
                    .map(SplitGroup::rawConvertibleGroup)
                    .collect(Collectors.toList());
        }

        /*
         * 生成分裂的逻辑：
         * 1. 将文件按照区间划分为多个分区，确保每个分区的文件区间不重叠
         * 2. 使用有序装箱算法，将分区后的文件组打包成符合目标大小的分裂
         */
        List<List<DataFileMeta>> sections =
                new IntervalPartition(files, keyComparator)
                        .partition().stream().map(this::flatRun).collect(Collectors.toList());

        return packSplits(sections).stream()
                .map(
                        f ->
                                f.size() == 1 && withoutDeleteRow(f.get(0))
                                        ? SplitGroup.rawConvertibleGroup(f)
                                        : SplitGroup.nonRawConvertibleGroup(f))
                .collect(Collectors.toList());
    }

    // 为流式扫描生成数据分裂
    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        // 流式扫描不分裂，直接作为一个数据组返回
        return Collections.singletonList(SplitGroup.rawConvertibleGroup(files));
    }

    // 打包分裂
    private List<List<DataFileMeta>> packSplits(List<List<DataFileMeta>> sections) {
        Function<List<DataFileMeta>, Long> weightFunc =
                file -> Math.max(totalSize(file), openFileCost);
        return BinPacking.packForOrdered(sections, weightFunc, targetSplitSize).stream()
                .map(this::flatFiles)
                .collect(Collectors.toList());
    }

    // 计算文件组的总大小
    private long totalSize(List<DataFileMeta> section) {
        long size = 0L;
        for (DataFileMeta file : section) {
            size += file.fileSize();
        }
        return size;
    }

    // 将分区后的文件展平为一个列表
    private List<DataFileMeta> flatRun(List<SortedRun> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(run -> files.addAll(run.files()));
        return files;
    }

    // 将多个文件组展平为一个文件列表
    private List<DataFileMeta> flatFiles(List<List<DataFileMeta>> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(files::addAll);
        return files;
    }

    // 检查数据文件是否没有删除记录
    private boolean withoutDeleteRow(DataFileMeta dataFileMeta) {
        // 如果没有删除记录，返回 true；否则返回 false
        return dataFileMeta.deleteRowCount().map(count -> count == 0L).orElse(true);
    }
}
