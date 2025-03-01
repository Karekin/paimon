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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BiFunctionWithIOE;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/** 查找工具类，用于实现数据查找逻辑。 */
public class LookupUtils {

    /**
     * 从多个层级中查找数据。
     * 该方法根据指定的键和开始层级，在 Levels 对象中查找数据，并根据找到的结果返回对应的值。
     * @param levels 层级结构，包含多个层级的数据
     * @param key 要查找的键
     * @param startLevel 开始查找的层级
     * @param lookup 表示层级的查找操作
     * @param level0Lookup 表示层级 0 的查找操作
     * @return 查找到的结果
     * @throws IOException 如果发生 IO 异常
     */
    public static <T> T lookup(
            Levels levels,
            InternalRow key,
            int startLevel,
            BiFunctionWithIOE<InternalRow, SortedRun, T> lookup,
            BiFunctionWithIOE<InternalRow, TreeSet<DataFileMeta>, T> level0Lookup)
            throws IOException {

        T result = null;

        // 遍历层级结构，从 startLevel 开始查找
        for (int i = startLevel; i < levels.numberOfLevels(); i++) {
            if (i == 0) {
                // 如果是层级 0，调用专用的查找方法
                result = level0Lookup.apply(key, levels.level0());
            } else {
                // 对于其他层级，使用通用的查找方法
                SortedRun level = levels.runOfLevel(i);
                result = lookup.apply(key, level);
            }

            // 如果找到结果，立即返回
            if (result != null) {
                break;
            }
        }

        return result;
    }

    /**
     * 在层级 0 中查找数据。
     * 该方法根据键在层级 0 的文件元数据中查找数据，通过遍历文件元数据，找到可能包含目标键的文件。
     * @param keyComparator 键比较器，用于比较键的大小
     * @param target 目标键
     * @param level0 层级 0 的文件元数据
     * @param lookup 表示查找操作
     * @return 查找到的结果
     * @throws IOException 如果发生 IO 异常
     */
    public static <T> T lookupLevel0(
            Comparator<InternalRow> keyComparator,
            InternalRow target,
            TreeSet<DataFileMeta> level0,
            BiFunctionWithIOE<InternalRow, DataFileMeta, T> lookup)
            throws IOException {
        T result = null;

        // 遍历层级 0 的文件元数据
        for (DataFileMeta file : level0) {
            // 检查文件的键范围是否包含目标键
            if (keyComparator.compare(file.maxKey(), target) >= 0
                    && keyComparator.compare(file.minKey(), target) <= 0) {
                result = lookup.apply(target, file); // 调用查找方法

                // 如果找到结果，立即返回
                if (result != null) {
                    break;
                }
            }
        }

        return result;
    }

    /**
     * 根据二分查找在特定层级中查找数据。
     * 该方法通过二分查找在排序后的文件元数据中定位目标键，并调用查找操作获取结果。
     * @param keyComparator 键比较器，用于比较键的大小
     * @param target 目标键
     * @param level 要查找的层级
     * @param lookup 表示查找操作
     * @return 查找到的结果
     * @throws IOException 如果发生 IO 异常
     */
    public static <T> T lookup(
            Comparator<InternalRow> keyComparator,
            InternalRow target,
            SortedRun level,
            BiFunctionWithIOE<InternalRow, DataFileMeta, T> lookup)
            throws IOException {
        if (level.isEmpty()) {
            return null; // 如果层级为空，返回 null
        }

        List<DataFileMeta> files = level.files(); // 获取层级的文件列表
        int left = 0;
        int right = files.size() - 1;

        // 使用二分查找找到目标键可能存在的文件位置
        while (left < right) {
            int mid = (left + right) / 2;

            if (keyComparator.compare(files.get(mid).maxKey(), target) < 0) {
                // 如果中间文件的最大键小于目标键，说明目标键在右侧
                left = mid + 1;
            } else {
                // 否则，目标键在左侧或中间
                right = mid;
            }
        }

        int index = right;

        // 检查是否需要调整索引
        if (index == files.size() - 1
                && keyComparator.compare(files.get(index).maxKey(), target) < 0) {
            index++; // 调整索引到文件末尾
        }

        // 如果索引有效，调用查找方法；否则返回 null
        return index < files.size() ? lookup.apply(target, files.get(index)) : null;
    }

    /**
     * 计算文件的大小（以 KiB 为单位）。
     * @param file 文件对象
     * @return 文件大小（以 KiB 为单位）
     */
    public static int fileKibiBytes(File file) {
        long kibiBytes = file.length() >> 10; // 将文件大小转换为 KiB

        // 检查是否超出整数范围
        if (kibiBytes > Integer.MAX_VALUE) {
            throw new RuntimeException(
                    "Lookup file is too big: " + MemorySize.ofKibiBytes(kibiBytes));
        }

        return (int) kibiBytes; // 返回文件大小
    }
}