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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.utils.BinPacking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.append.BucketedAppendCompactManager.fileComparator;

/** Append only implementation of {@link SplitGenerator}. */
public class AppendOnlySplitGenerator implements SplitGenerator {

    // 目标分裂大小
    private final long targetSplitSize;
    // 打开文件的成本
    private final long openFileCost;
    // 桶模式，控制数据的桶分配方式
    private final BucketMode bucketMode;

    // 构造函数，初始化目标分裂大小、打开文件成本和桶模式
    public AppendOnlySplitGenerator(
            long targetSplitSize, long openFileCost, BucketMode bucketMode) {
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
        this.bucketMode = bucketMode;
    }

    // 判断是否始终可以转换为原始数据
    @Override
    public boolean alwaysRawConvertible() {
        // 对于追加模式来说，总是可以直接转换为原始数据
        return true;
    }

    // 为批量扫描生成数据分裂
    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> input) {
        // 对输入文件进行排序，排序方式根据桶模式决定
        List<DataFileMeta> files = new ArrayList<>(input);
        files.sort(fileComparator(bucketMode == BucketMode.BUCKET_UNAWARE));
        // 定义文件的权重函数
        Function<DataFileMeta, Long> weightFunc = file -> Math.max(file.fileSize(), openFileCost);
        // 使用有序装箱算法生成分裂
        return BinPacking.packForOrdered(files, weightFunc, targetSplitSize).stream()
                .map(SplitGroup::rawConvertibleGroup)
                .collect(Collectors.toList());
    }

    // 为流式扫描生成数据分裂
    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        // 如果是桶无感知模式，调用批量分裂生成逻辑
        if (bucketMode == BucketMode.BUCKET_UNAWARE) {
            return splitForBatch(files);
        } else {
            // 否则，所有数据作为一个分裂组返回
            return Collections.singletonList(SplitGroup.rawConvertibleGroup(files));
        }
    }
}
