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

package org.apache.paimon.mergetree;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Preconditions;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@link SortedRun} 表示按键排序的一组文件。这里的文件指的是 {@link DataFileMeta}，
 * 并且这些文件在键空间上都没有重叠（即一个文件的 maxKey 必须小于下一个文件的 minKey）。
 *
 * <p>通常用法：
 * <ul>
 *     <li>将若干个有序(或无序)的 {@link DataFileMeta} 包装成一个 SortedRun；</li>
 *     <li>并可以对该 SortedRun 进行一些合并、校验等操作。</li>
 * </ul>
 */
public class SortedRun {

    /**
     * 表示一个有序文件列表的不可变集合。
     * 有序指的是：files.get(0).minKey() <= files.get(1).minKey() <= ...
     * 同时各文件键区间 [minKey, maxKey] 不重叠。
     */
    private final List<DataFileMeta> files;

    /**
     * 累计这些文件的大小总和 (fileSize)。用来方便在合并、统计等操作时快速获取总大小。
     */
    private final long totalSize;

    /**
     * 私有构造方法，供静态工厂方法内部创建。
     * 使用了 Collections.unmodifiableList(files) 来保证文件列表不可变。
     *
     * @param files 有序的 DataFileMeta 列表
     */
    private SortedRun(List<DataFileMeta> files) {
        // 将文件列表设为不可变，以防止外部修改
        this.files = Collections.unmodifiableList(files);

        // 计算这些文件的总大小
        long totalSize = 0L;
        for (DataFileMeta file : files) {
            totalSize += file.fileSize();
        }
        this.totalSize = totalSize;
    }

    /**
     * 创建一个空的 SortedRun，不包含任何文件。
     *
     * @return 空的 SortedRun 实例
     */
    public static SortedRun empty() {
        return new SortedRun(Collections.emptyList());
    }

    /**
     * 创建一个只包含单个文件的 SortedRun。
     *
     * @param file 单个 DataFileMeta
     * @return 包含该文件的 SortedRun
     */
    public static SortedRun fromSingle(DataFileMeta file) {
        return new SortedRun(Collections.singletonList(file));
    }

    /**
     * 将已经排好序的文件列表直接生成 SortedRun。
     * <p>在调用前，确保传入的列表是按键排序且无重叠。
     *
     * @param sortedFiles 已经有序的文件列表
     * @return 对应的 SortedRun
     */
    public static SortedRun fromSorted(List<DataFileMeta> sortedFiles) {
        return new SortedRun(sortedFiles);
    }

    /**
     * 将无序的文件列表先进行排序，然后创建 SortedRun。
     * <p>排序方式：先按照文件的 minKey 进行排序，最后再做一次校验，确保无重叠。
     *
     * @param unsortedFiles 无序的文件列表
     * @param keyComparator 用于比较 InternalRow 的比较器（比较文件的 minKey, maxKey）
     * @return 有序且无重叠的 SortedRun
     */
    public static SortedRun fromUnsorted(
            List<DataFileMeta> unsortedFiles, Comparator<InternalRow> keyComparator) {
        // 根据 minKey 排序
        unsortedFiles.sort((o1, o2) -> keyComparator.compare(o1.minKey(), o2.minKey()));
        // 直接用排好序的列表创建 SortedRun
        SortedRun run = new SortedRun(unsortedFiles);
        // 验证相邻文件之间无重叠
        run.validate(keyComparator);
        return run;
    }

    /**
     * 获取文件列表（不可变）。
     *
     * @return SortedRun 中包含的所有文件
     */
    public List<DataFileMeta> files() {
        return files;
    }

    /**
     * 判断该 SortedRun 是否为空（不包含任何文件）。
     *
     * @return 如果为空，返回 true；否则返回 false
     */
    public boolean isEmpty() {
        return files.isEmpty();
    }

    /**
     * 判断该 SortedRun 是否非空。
     *
     * @return 如果非空，返回 true；否则返回 false
     */
    public boolean nonEmpty() {
        return !isEmpty();
    }

    /**
     * 获取该 SortedRun 中所有文件大小的总和。
     *
     * @return 所有文件大小之和
     */
    public long totalSize() {
        return totalSize;
    }

    /**
     * 在测试环境可见的方法，用来校验当前 SortedRun 是否真正按键排序且无重叠。
     *
     * @param comparator 用于比较 InternalRow 的比较器
     * @throws IllegalStateException 如果发现相邻文件存在重叠，或未按顺序排列，会抛出异常
     */
    @VisibleForTesting
    public void validate(Comparator<InternalRow> comparator) {
        for (int i = 1; i < files.size(); i++) {
            // 确保第 i 个文件的 minKey 大于前一个文件的 maxKey
            Preconditions.checkState(
                    comparator.compare(files.get(i).minKey(), files.get(i - 1).maxKey()) > 0,
                    "SortedRun is not sorted and may contain overlapping key intervals. "
                            + "This is a bug.");
        }
    }

    /**
     * 判断两个 SortedRun 是否相同（通过比较内部的文件列表）。
     *
     * @param o 待比较的对象
     * @return 如果两个对象的文件列表一致，则返回 true
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SortedRun)) {
            return false;
        }
        SortedRun that = (SortedRun) o;
        return files.equals(that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(files);
    }

    /**
     * 将内部文件列表转换为可读字符串。
     *
     * @return 类似 "[FileMeta1, FileMeta2, ...]" 的字符串
     */
    @Override
    public String toString() {
        return "["
                + files.stream().map(DataFileMeta::toString).collect(Collectors.joining(", "))
                + "]";
    }
}

