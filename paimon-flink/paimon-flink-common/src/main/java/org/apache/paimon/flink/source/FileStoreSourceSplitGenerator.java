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

import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 文件存储切片生成器，负责计划所有要读取的文件，并将它们拆分为一组 {@link FileStoreSourceSplit}.
 */
public class FileStoreSourceSplitGenerator {

    /**
     * 当前 ID 以可变的字符串形式表示。这比整数值范围覆盖的值更多，所以应该永远不会溢出。
     */
    private final char[] currentId = "0000000000".toCharArray();

    /**
     * 根据表扫描计划创建切片。
     *
     * @param plan 表扫描计划
     * @return 切片列表
     */
    public List<FileStoreSourceSplit> createSplits(TableScan.Plan plan) {
        return plan.splits().stream()
                .map(s -> new FileStoreSourceSplit(getNextId(), s))
                .collect(Collectors.toList());
    }

    /**
     * 根据切片列表创建切片。
     *
     * @param splits 切片列表
     * @return 切片列表
     */
    public List<FileStoreSourceSplit> createSplits(List<Split> splits) {
        return splits.stream()
                .map(s -> new FileStoreSourceSplit(getNextId(), s))
                .collect(Collectors.toList());
    }

    /**
     * 获取下一个唯一的切片 ID。
     *
     * @return 下一个切片 ID
     */
    protected final String getNextId() {
        // 由于我们只是递增数字，因此我们直接递增字符表示，而不是递增整数并将其转换为字符串表示。
        incrementCharArrayByOne(currentId, currentId.length - 1);
        return new String(currentId);
    }

    /**
     * 递增字符数组的值。
     *
     * @param array 字符数组
     * @param pos 当前位置
     */
    private static void incrementCharArrayByOne(char[] array, int pos) {
        if (pos < 0) {
            throw new RuntimeException("生成了太多切片。");
        }

        char c = array[pos];
        c++;

        if (c > '9') {
            c = '0';
            incrementCharArrayByOne(array, pos - 1); // 进位操作
        }
        array[pos] = c;
    }
}
