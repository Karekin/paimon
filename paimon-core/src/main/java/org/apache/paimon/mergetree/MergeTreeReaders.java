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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.SizedReaderSupplier;
import org.apache.paimon.utils.FieldsComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 用于创建合并树（Merge Tree）的常用 {@link RecordReader} 的工具类。
 *
 * 该类提供了一系列静态方法，用于从多个排序的运行（Sorted Run）中创建记录读取器。
 * 这些方法能够将多个数据源合并并排序，以支持数据的有序读取。
 */
public class MergeTreeReaders {

    private MergeTreeReaders() {} // 私有构造器，禁止实例化

    /**
     * 创建用于合并树的记录读取器。
     *
     * @param sections 运行列表（每个运行是一个 {@link SortedRun} 列表）
     * @param readerFactory 文件读取器工厂
     * @param userKeyComparator 用户键比较器
     * @param userDefinedSeqComparator 用户定义的序列号比较器
     * @param mergeFunctionWrapper 合并函数包装器
     * @param mergeSorter 合并排序器
     * @return 返回合并树的记录读取器
     * @throws IOException 创建记录读取器时可能抛出的异常
     */
    public static <T> RecordReader<T> readerForMergeTree(
            List<List<SortedRun>> sections,
            FileReaderFactory<KeyValue> readerFactory,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper,
            MergeSorter mergeSorter)
            throws IOException {
        List<ReaderSupplier<T>> readers = new ArrayList<>(); // 创建一个读取器供应商列表
        for (List<SortedRun> section : sections) { // 遍历每个运行列表
            readers.add(() -> readerForSection(
                    section, // 当前运行列表
                    readerFactory, // 文件读取器工厂
                    userKeyComparator, // 用户键比较器
                    userDefinedSeqComparator, // 用户定义的序列号比较器
                    mergeFunctionWrapper, // 合并函数包装器
                    mergeSorter)); // 合并排序器
        }
        return ConcatRecordReader.create(readers); // 创建合并的记录读取器
    }

    /**
     * 为单个运行列表创建记录读取器。
     *
     * @param section 运行列表
     * @param readerFactory 文件读取器工厂
     * @param userKeyComparator 用户键比较器
     * @param userDefinedSeqComparator 用户定义的序列号比较器
     * @param mergeFunctionWrapper 合并函数包装器
     * @param mergeSorter 合并排序器
     * @return 返回记录读取器
     * @throws IOException 创建记录读取器时可能抛出的异常
     */
    public static <T> RecordReader<T> readerForSection(
            List<SortedRun> section,
            FileReaderFactory<KeyValue> readerFactory,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper,
            MergeSorter mergeSorter)
            throws IOException {
        List<SizedReaderSupplier<KeyValue>> readers = new ArrayList<>(); // 创建一个带大小信息的读取器供应商列表
        for (SortedRun run : section) { // 遍历每个运行
            readers.add(new SizedReaderSupplier<KeyValue>() { // 创建带大小信息的读取器供应商
                @Override
                public long estimateSize() {
                    return run.totalSize(); // 返回运行的大小
                }

                @Override
                public RecordReader<KeyValue> get() throws IOException { // 获取记录读取器
                    return readerForRun(run, readerFactory); // 创建运行的记录读取器
                }
            });
        }
        return mergeSorter.mergeSort( // 使用合并排序器合并和排序读取器
                readers,
                userKeyComparator, // 用户键比较器
                userDefinedSeqComparator, // 用户定义的序列号比较器
                mergeFunctionWrapper); // 合并函数包装器
    }

    /**
     * 为单个运行创建记录读取器。
     *
     * @param run 运行
     * @param readerFactory 文件读取器工厂
     * @return 返回记录读取器
     * @throws IOException 创建记录读取器时可能抛出的异常
     */
    private static RecordReader<KeyValue> readerForRun(
            SortedRun run, FileReaderFactory<KeyValue> readerFactory) throws IOException {
        List<ReaderSupplier<KeyValue>> readers = new ArrayList<>(); // 创建一个读取器供应商列表
        for (DataFileMeta file : run.files()) { // 遍历运行中的文件
            readers.add(() -> readerFactory.createRecordReader(file)); // 创建文件的记录读取器
        }
        return ConcatRecordReader.create(readers); // 创建合并的记录读取器
    }
}
