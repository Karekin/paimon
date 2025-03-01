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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.disk.ChannelReaderInputView;
import org.apache.paimon.disk.ChannelReaderInputViewIterator;
import org.apache.paimon.disk.ChannelWithMeta;
import org.apache.paimon.disk.ChannelWriterOutputView;
import org.apache.paimon.disk.FileChannelUtil;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.CachelessSegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.SortMergeReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.reader.SizedReaderSupplier;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.KeyValueWithLevelNoReusingSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 具有键重叠的 Reader 的合并排序器。
 */
public class MergeSorter {

    // 键的数据类型
    private final RowType keyType;
    // 值的数据类型
    private RowType valueType;

    // 排序引擎，用于执行排序操作
    private final SortEngine sortEngine;
    // 溢出阈值，当 Reader 的数量超过该值时，触发溢出操作
    private final int spillThreshold;
    // 压缩选项，用于溢出数据时的压缩配置
    private final CompressOptions compression;

    // 内存段池，用于管理排序时的内存分配
    private final MemorySegmentPool memoryPool;

    // I/O 管理器，可选，用于管理外部存储设备的输入输出操作
    @Nullable private IOManager ioManager;

    /**
     * 构造方法，初始化 MergeSorter。
     * @param options        排序配置选项
     * @param keyType        键的数据类型
     * @param valueType      值的数据类型
     * @param ioManager      I/O 管理器
     */
    public MergeSorter(
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            @Nullable IOManager ioManager) {
        this.sortEngine = options.sortEngine(); // 获取排序引擎
        this.spillThreshold = options.sortSpillThreshold(); // 获取溢出阈值
        this.compression = options.spillCompressOptions(); // 获取压缩选项
        this.keyType = keyType; // 设置键的数据类型
        this.valueType = valueType; // 设置值的数据类型
        // 创建内存段池，用于缓冲区大小和页面大小的配置
        this.memoryPool = new CachelessSegmentPool(options.sortSpillBufferSize(), options.pageSize());
        this.ioManager = ioManager; // 设置 I/O 管理器
    }

    // 获取内存段池
    public MemorySegmentPool memoryPool() {
        return memoryPool;
    }

    // 获取值的数据类型
    public RowType valueType() {
        return valueType;
    }

    // 设置 I/O 管理器
    public void setIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
    }

    // 设置投影后的值类型
    public void setProjectedValueType(RowType projectedType) {
        this.valueType = projectedType;
    }

    /**
     * 合并排序操作，根据 I/O 管理器和溢出阈值决定是否触发溢出。
     * @param lazyReaders                待排序的延迟读取器
     * @param keyComparator              键的比较器
     * @param userDefinedSeqComparator   用户自定义的字段比较器
     * @param mergeFunction              合并函数
     * @return                           合并后的记录读取器
     * @throws IOException               输入输出异常
     */
    public <T> RecordReader<T> mergeSort(
            List<SizedReaderSupplier<KeyValue>> lazyReaders,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunction)
            throws IOException {
        // 检查是否需要触发溢出操作
        if (ioManager != null && lazyReaders.size() > spillThreshold) {
            return spillMergeSort(lazyReaders, keyComparator, userDefinedSeqComparator, mergeFunction);
        }
        // 直接进行内存中的合并排序
        return mergeSortNoSpill(lazyReaders, keyComparator, userDefinedSeqComparator, mergeFunction);
    }

    /**
     * 在内存中进行合并排序，不触发溢出。
     * @param lazyReaders                待排序的延迟读取器
     * @param keyComparator              键的比较器
     * @param userDefinedSeqComparator   用户自定义的字段比较器
     * @param mergeFunction              合并函数
     * @return                           合并后的记录读取器
     * @throws IOException               输入输出异常
     */
    public <T> RecordReader<T> mergeSortNoSpill(
            List<? extends ReaderSupplier<KeyValue>> lazyReaders,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunction)
            throws IOException {
        List<RecordReader<KeyValue>> readers = new ArrayList<>(lazyReaders.size()); // 用于存储读取器
        for (ReaderSupplier<KeyValue> supplier : lazyReaders) {
            try {
                readers.add(supplier.get()); // 获取 Reader 并添加到列表
            } catch (IOException e) {
                // 如果某个 Reader 获取失败，关闭所有 Reader
                readers.forEach(IOUtils::closeQuietly);
                throw e; // 重新抛出异常
            }
        }
        // 创建排序合并读取器
        return SortMergeReader.createSortMergeReader(
                readers, keyComparator, userDefinedSeqComparator, mergeFunction, sortEngine);
    }

    /**
     * 触发溢出的合并排序操作。
     * @param inputReaders               输入的带大小的 Reader 供应商列表
     * @param keyComparator              键的比较器
     * @param userDefinedSeqComparator   用户自定义的字段比较器
     * @param mergeFunction              合并函数
     * @return                           合并后的记录读取器
     * @throws IOException               输入输出异常
     */
    private <T> RecordReader<T> spillMergeSort(
            List<SizedReaderSupplier<KeyValue>> inputReaders,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunction)
            throws IOException {
        List<SizedReaderSupplier<KeyValue>> sortedReaders = new ArrayList<>(inputReaders); // 复制输入列表
        // 根据估计大小排序读取器
        sortedReaders.sort(Comparator.comparingLong(SizedReaderSupplier::estimateSize));
        int spillSize = inputReaders.size() - spillThreshold; // 计算溢出的数量

        // 提取需要溢出的 Reader
        List<ReaderSupplier<KeyValue>> readers = new ArrayList<>(
                sortedReaders.subList(spillSize, sortedReaders.size())); // 留在内存中的 Reader
        // 将前 spillSize 个 Reader 溢出到磁盘
        for (ReaderSupplier<KeyValue> supplier : sortedReaders.subList(0, spillSize)) {
            readers.add(spill(supplier)); // 添加溢出后的 Reader
        }
        // 使用内存中的合并排序方法处理所有 Reader（包括溢出后的）
        return mergeSortNoSpill(readers, keyComparator, userDefinedSeqComparator, mergeFunction);
    }

    /**
     * 溢出 Reader 的数据到外部存储。
     * @param readerSupplier Reader 供应商
     * @return               溢出后的 Reader 供应商
     * @throws IOException   输入输出异常
     */
    private ReaderSupplier<KeyValue> spill(ReaderSupplier<KeyValue> readerSupplier)
            throws IOException {
        // 确保 I/O 管理器不为空
        checkArgument(ioManager != null, "IOManager must not be null when spilling.");

        FileIOChannel.ID channel = ioManager.createChannel(); // 创建 I/O 通道
        // 创建序列化器，用于序列化键值对数据
        KeyValueWithLevelNoReusingSerializer serializer =
                new KeyValueWithLevelNoReusingSerializer(keyType, valueType);
        BlockCompressionFactory compressFactory = BlockCompressionFactory.create(compression); // 创建压缩工厂
        int compressBlock = (int) MemorySize.parse("64 kb").getBytes(); // 压缩块大小

        ChannelWithMeta channelWithMeta;
        ChannelWriterOutputView out =
                FileChannelUtil.createOutputView( // 创建输出视图
                        ioManager, channel, compressFactory, compressBlock);
        try (RecordReader<KeyValue> reader = readerSupplier.get(); ) {
            RecordIterator<KeyValue> batch;
            KeyValue record;
            while ((batch = reader.readBatch()) != null) { // 读取批处理数据
                while ((record = batch.next()) != null) { // 读取记录
                    serializer.serialize(record, out); // 序列化记录
                }
                batch.releaseBatch(); // 释放批处理资源
            }
        } finally {
            out.close(); // 关闭输出视图
            channelWithMeta = new ChannelWithMeta(channel, out.getBlockCount(), out.getWriteBytes());
        }
        // 返回溢出后的 Reader 供应商
        return new SpilledReaderSupplier(
                channelWithMeta, compressFactory, compressBlock, serializer);
    }

    /**
     * 内部类，用于表示溢出后的 Reader 供应商。
     */
    private class SpilledReaderSupplier implements ReaderSupplier<KeyValue> {

        // 通道元数据
        private final ChannelWithMeta channel;
        private final BlockCompressionFactory compressFactory; // 压缩工厂
        private final int compressBlock; // 压缩块大小
        private final KeyValueWithLevelNoReusingSerializer serializer; // 序列化器

        /**
         * 构造方法，初始化 SpilledReaderSupplier。
         * @param channel                通道元数据
         * @param compressFactory        压缩工厂
         * @param compressBlock          压缩块大小
         * @param serializer             序列化器
         */
        public SpilledReaderSupplier(
                ChannelWithMeta channel,
                BlockCompressionFactory compressFactory,
                int compressBlock,
                KeyValueWithLevelNoReusingSerializer serializer) {
            this.channel = channel;
            this.compressFactory = compressFactory;
            this.compressBlock = compressBlock;
            this.serializer = serializer;
        }

        /**
         * 获取记录读取器。
         * @return 记录读取器
         * @throws IOException 输入输出异常
         */
        @Override
        public RecordReader<KeyValue> get() throws IOException {
            // 创建输入视图
            ChannelReaderInputView view = FileChannelUtil.createInputView(
                    ioManager, channel, new ArrayList<>(), compressFactory, compressBlock);
            BinaryRowSerializer rowSerializer = new BinaryRowSerializer(serializer.numFields());
            ChannelReaderInputViewIterator iterator =
                    new ChannelReaderInputViewIterator(view, null, rowSerializer);
            // 返回溢出数据的读取器
            return new ChannelReaderReader(view, iterator, serializer);
        }
    }

    /**
     * 内部类，用于读取溢出到外部存储的数据。
     */
    private static class ChannelReaderReader implements RecordReader<KeyValue> {

        private final ChannelReaderInputView view; // 输入视图
        private final ChannelReaderInputViewIterator iterator; // 迭代器
        private final KeyValueWithLevelNoReusingSerializer serializer; // 序列化器

        /**
         * 构造方法，初始化 ChannelReaderReader。
         * @param view         输入视图
         * @param iterator     迭代器
         * @param serializer   序列化器
         */
        private ChannelReaderReader(
                ChannelReaderInputView view,
                ChannelReaderInputViewIterator iterator,
                KeyValueWithLevelNoReusingSerializer serializer) {
            this.view = view;
            this.iterator = iterator;
            this.serializer = serializer;
        }

        private boolean read = false;

        // 读取数据批处理
        @Override
        public RecordIterator<KeyValue> readBatch() {
            if (read) {
                return null; // 已经读取过，返回 null
            }
            read = true; // 标记为已读
            return new RecordIterator<KeyValue>() {
                @Override
                public KeyValue next() throws IOException {
                    BinaryRow noReuseRow = iterator.next(); // 从迭代器获取下一行
                    if (noReuseRow == null) {
                        return null; // 无数据返回 null
                    }
                    return serializer.fromRow(noReuseRow); // 反序列化为 KeyValue
                }

                @Override
                public void releaseBatch() {}
            };
        }

        // 关闭资源
        @Override
        public void close() throws IOException {
            view.getChannel().closeAndDelete(); // 关闭通道并删除数据
        }
    }
}
