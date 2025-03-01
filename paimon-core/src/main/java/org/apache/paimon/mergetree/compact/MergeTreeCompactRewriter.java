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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * 默认的 {@link CompactRewriter} 实现，用于合并树（Merge Tree）的压缩。
 *
 * 该类提供了合并树压缩的重写逻辑，实现了将多个运行（Sorted Run）合并到一个或多个目标文件的功能。
 */
public class MergeTreeCompactRewriter extends AbstractCompactRewriter {

    // 用于读取数据文件的文件读取器工厂
    protected final FileReaderFactory<KeyValue> readerFactory;

    // 用于写入数据文件的文件写入器工厂
    protected final KeyValueFileWriterFactory writerFactory;

    // 数据键的比较器
    protected final Comparator<InternalRow> keyComparator;

    // 用户定义的序列号比较器
    @Nullable protected final FieldsComparator userDefinedSeqComparator;

    // 合并函数工厂
    protected final MergeFunctionFactory<KeyValue> mfFactory;

    // 合并排序器
    protected final MergeSorter mergeSorter;

    /**
     * 构造函数，初始化 MergeTreeCompactRewriter 对象。
     *
     * @param readerFactory 文件读取器工厂
     * @param writerFactory 文件写入器工厂
     * @param keyComparator 数据键的比较器
     * @param userDefinedSeqComparator 用户定义的序列号比较器
     * @param mfFactory 合并函数工厂
     * @param mergeSorter 合并排序器
     */
    public MergeTreeCompactRewriter(
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter) {
        this.readerFactory = readerFactory; // 文件读取器工厂
        this.writerFactory = writerFactory; // 文件写入器工厂
        this.keyComparator = keyComparator; // 数据键比较器
        this.userDefinedSeqComparator = userDefinedSeqComparator; // 序列号比较器
        this.mfFactory = mfFactory; // 合并函数工厂
        this.mergeSorter = mergeSorter; // 合并排序器
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        return rewriteCompaction(outputLevel, dropDelete, sections); // 调用重写压缩方法
    }

    /**
     * 执行合并树的压缩重写逻辑。
     *
     * @param outputLevel 输出级别
     * @param dropDelete 是否丢弃删除标记
     * @param sections 合并的运行列表
     * @return 返回压缩结果
     * @throws Exception 重写过程中可能抛出的异常
     */
    protected CompactResult rewriteCompaction(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        RollingFileWriter<KeyValue, DataFileMeta> writer = // 创建滚动写入器
                writerFactory.createRollingMergeTreeFileWriter(outputLevel, FileSource.COMPACT);

        RecordReader<KeyValue> reader = null; // 创建记录读取器
        Exception collectedExceptions = null; // 收集异常

        try {
            // 创建合并树的记录读取器
            reader = readerForMergeTree(sections, new ReducerMergeFunctionWrapper(mfFactory.create()));

            // 如果需要丢弃删除标记，使用 DropDeleteReader 装饰器
            if (dropDelete) {
                reader = new DropDeleteReader(reader);
            }

            // 写入数据到目标文件
            writer.write(new RecordReaderIterator<>(reader));
        } catch (Exception e) {
            collectedExceptions = e; // 捕获异常
        } finally {
            try {
                // 确保资源关闭
                IOUtils.closeAll(reader, writer);
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions); // 记录异常
            }
        }

        // 如果发生异常，中断写入并抛出
        if (null != collectedExceptions) {
            writer.abort();
            throw collectedExceptions;
        }

        // 提取压缩前的文件元数据并返回压缩结果
        List<DataFileMeta> before = extractFilesFromSections(sections);
        notifyRewriteCompactBefore(before);
        return new CompactResult(before, writer.result());
    }

    /**
     * 创建合并树的记录读取器。
     *
     * @param sections 合并的运行列表
     * @param mergeFunctionWrapper 合并函数包装器
     * @return 返回记录读取器
     * @throws IOException 读取文件时可能抛出的异常
     */
    protected <T> RecordReader<T> readerForMergeTree(
            List<List<SortedRun>> sections, MergeFunctionWrapper<T> mergeFunctionWrapper)
            throws IOException {
        return MergeTreeReaders.readerForMergeTree(
                sections, // 运行列表
                readerFactory, // 文件读取器工厂
                keyComparator, // 数据键比较器
                userDefinedSeqComparator, // 序列号比较器
                mergeFunctionWrapper, // 合并函数包装器
                mergeSorter); // 合并排序器
    }

    /**
     * 通知重写压缩前的文件元数据。
     *
     * @param files 压缩前的文件元数据
     */
    protected void notifyRewriteCompactBefore(List<DataFileMeta> files) {}
}
