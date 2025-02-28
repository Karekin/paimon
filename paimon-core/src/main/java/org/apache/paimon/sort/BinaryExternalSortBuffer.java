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

package org.apache.paimon.sort;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.ChannelWithMeta;
import org.apache.paimon.disk.ChannelWriterOutputView;
import org.apache.paimon.disk.FileChannelUtil;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.codegen.CodeGenUtils.newNormalizedKeyComputer;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordComparator;

/**
 * 一个可溢出的排序缓冲区。实现SortBuffer接口，用于存储和排序BinaryRow数据。
 */
public class BinaryExternalSortBuffer implements SortBuffer {

    private final BinaryRowSerializer serializer; // BinaryRow序列化器
    private final BinaryInMemorySortBuffer inMemorySortBuffer; // 内存中的排序缓冲区
    private final IOManager ioManager; // Flink的IO管理器
    private SpillChannelManager channelManager; // 溢出文件通道管理器
    private final int maxNumFileHandles; // 最大文件句柄数
    private final BlockCompressionFactory compressionCodecFactory; // 压缩解压工厂
    private final int compressionBlockSize; // 压缩块大小
    private final BinaryExternalMerger merger; // 外部文件合并器

    private final FileIOChannel.Enumerator enumerator; // 文件通道枚举器
    private final List<ChannelWithMeta> spillChannelIDs; // 溢出文件通道及元数据集合
    private final MemorySize maxDiskSize; // 最大磁盘使用大小

    private int numRecords = 0; // 数据总数

    public BinaryExternalSortBuffer(
            BinaryRowSerializer serializer, // BinaryRow序列化器
            RecordComparator comparator, // 数据比较器
            int pageSize, // 内存页大小
            BinaryInMemorySortBuffer inMemorySortBuffer, // 内存排序缓冲区
            IOManager ioManager, // IO管理器
            int maxNumFileHandles, // 最大文件句柄数
            CompressOptions compression, // 压缩选项
            MemorySize maxDiskSize // 最大磁盘使用大小
    ) {
        this.serializer = serializer; // 初始化序列化器
        this.inMemorySortBuffer = inMemorySortBuffer; // 初始化内存排序缓冲区
        this.ioManager = ioManager; // 初始化IO管理器
        this.channelManager = new SpillChannelManager(); // 初始化溢出文件通道管理器
        this.maxNumFileHandles = maxNumFileHandles; // 初始化最大文件句柄数
        this.compressionCodecFactory = BlockCompressionFactory.create(compression); // 初始化压缩解压工厂
        this.compressionBlockSize = (int) MemorySize.parse("64 kb").getBytes(); // 设置压缩块大小
        this.maxDiskSize = maxDiskSize; // 设置最大磁盘使用大小
        // 初始化文件合并器
        this.merger = new BinaryExternalMerger(
                ioManager,
                pageSize,
                maxNumFileHandles,
                channelManager,
                serializer.duplicate(),
                comparator,
                compressionCodecFactory,
                compressionBlockSize
        );
        this.enumerator = ioManager.createChannelEnumerator(); // 初始化文件通道枚举器
        this.spillChannelIDs = new ArrayList<>(); // 初始化溢出文件通道集合
    }

    public static BinaryExternalSortBuffer create(
            IOManager ioManager, // IO管理器
            RowType rowType, // 行类型
            int[] keyFields, // 键字段索引
            long bufferSize, // 缓冲区大小
            int pageSize, // 内存页大小
            int maxNumFileHandles, // 最大文件句柄数
            CompressOptions compression, // 压缩选项
            MemorySize maxDiskSize // 最大磁盘使用大小
    ) {
        // 使用堆内存池创建BinaryExternalSortBuffer
        return create(
                ioManager,
                rowType,
                keyFields,
                new HeapMemorySegmentPool(bufferSize, pageSize),
                maxNumFileHandles,
                compression,
                maxDiskSize
        );
    }

    public static BinaryExternalSortBuffer create(
            IOManager ioManager, // IO管理器
            RowType rowType, // 行类型
            int[] keyFields, // 键字段索引
            MemorySegmentPool pool, // 内存池
            int maxNumFileHandles, // 最大文件句柄数
            CompressOptions compression, // 压缩选项
            MemorySize maxDiskSize // 最大磁盘使用大小
    ) {
        RecordComparator comparator = newRecordComparator(rowType.getFieldTypes(), keyFields); // 创建比较器
        // 创建内存排序缓冲区
        BinaryInMemorySortBuffer sortBuffer = BinaryInMemorySortBuffer.createBuffer(
                newNormalizedKeyComputer(rowType.getFieldTypes(), keyFields), // 规范化键计算器
                new InternalRowSerializer(rowType), // 行序列化器
                comparator,
                pool
        );
        // 返回BinaryExternalSortBuffer实例
        return new BinaryExternalSortBuffer(
                new BinaryRowSerializer(rowType.getFieldCount()), // BinaryRow序列化器
                comparator,
                pool.pageSize(), // 内存页大小
                sortBuffer,
                ioManager,
                maxNumFileHandles,
                compression,
                maxDiskSize
        );
    }

    @Override
    public int size() {
        return numRecords; // 返回数据总数
    }

    @Override
    public void clear() {
        this.numRecords = 0; // 重置数据总数
        // 清理内存缓冲区
        inMemorySortBuffer.clear();
        spillChannelIDs.clear(); // 清理溢出文件通道集合
        // 删除所有溢出文件
        channelManager.close();
        channelManager = new SpillChannelManager();
    }

    @Override
    public long getOccupancy() {
        return inMemorySortBuffer.getOccupancy(); // 获取内存缓冲区大小
    }

    @Override
    public boolean flushMemory() throws IOException {
        boolean isFull = getDiskUsage() >= maxDiskSize.getBytes(); // 判断磁盘是否已满
        if (isFull) {
            return false; // 磁盘已满，无法溢出
        } else {
            spill(); // 溢出到外部存储
            return true; // 溢出成功
        }
    }

    private long getDiskUsage() {
        long bytes = 0;
        // 计算所有溢出文件的总大小
        for (ChannelWithMeta spillChannelID : spillChannelIDs) {
            bytes += spillChannelID.getNumBytes();
        }
        return bytes;
    }

    @VisibleForTesting
    public void write(MutableObjectIterator<BinaryRow> iterator) throws IOException {
        BinaryRow row = serializer.createInstance(); // 创建BinaryRow实例
        // 将迭代器中的数据全部写入
        while ((row = iterator.next(row)) != null) {
            write(row);
        }
    }

    @Override
    public boolean write(InternalRow record) throws IOException {
        while (true) {
            boolean success = inMemorySortBuffer.write(record); // 尝试将数据写入内存缓冲区
            if (success) {
                this.numRecords++; // 数据总数加一
                return true; // 写入成功
            }
            if (inMemorySortBuffer.isEmpty()) {
                // 数据无法写入空缓冲区，说明数据超过限制
                throw new IOException("The record exceeds the maximum size of a sort buffer.");
            } else {
                spill(); // 溢出内存缓冲区到外部存储
                // 合并溢出文件
                if (spillChannelIDs.size() >= maxNumFileHandles) {
                    List<ChannelWithMeta> merged = merger.mergeChannelList(spillChannelIDs);
                    spillChannelIDs.clear();
                    spillChannelIDs.addAll(merged);
                }
            }
        }
    }

    @Override
    public final MutableObjectIterator<BinaryRow> sortedIterator() throws IOException {
        if (spillChannelIDs.isEmpty()) {
            // 如果没有溢出文件，直接返回内存缓冲区的排序结果
            return inMemorySortBuffer.sortedIterator();
        }
        return spilledIterator(); // 返回合并后的排序结果
    }

    private MutableObjectIterator<BinaryRow> spilledIterator() throws IOException {
        spill(); // 溢出当前内存缓冲区
        List<FileIOChannel> openChannels = new ArrayList<>(); // 打开的文件通道列表
        // 获取合并后的排序迭代器
        BinaryMergeIterator<BinaryRow> iterator =
                merger.getMergingIterator(spillChannelIDs, openChannels);
        // 添加打开的通道到管理器
        channelManager.addOpenChannels(openChannels);
        // 返回可迭代结果
        return new MutableObjectIterator<BinaryRow>() {
            @Override
            public BinaryRow next(BinaryRow reuse) throws IOException {
                // 忽略reuse参数，使用迭代器自己的复用对象
                return next();
            }

            @Override
            public BinaryRow next() throws IOException {
                // 获取下一行数据并复制，支持后续的复用逻辑
                BinaryRow row = iterator.next();
                return row == null ? null : row.copy();
            }
        };
    }

    private void spill() throws IOException {
        if (inMemorySortBuffer.isEmpty()) {
            return; // 缓冲区为空，无需溢出
        }

        // 创建下一个文件通道
        FileIOChannel.ID channel = enumerator.next();
        channelManager.addChannel(channel); // 添加到通道管理器

        ChannelWriterOutputView output = null;
        int blockCount;

        try {
            // 创建输出视图，包含压缩功能
            output = FileChannelUtil.createOutputView(
                    ioManager,
                    channel,
                    compressionCodecFactory,
                    compressionBlockSize
            );
            new QuickSort().sort(inMemorySortBuffer); // 对内存缓冲区排序
            // 将排序后的数据写入输出
            inMemorySortBuffer.writeToOutput(output);
            output.close();
            blockCount = output.getBlockCount(); // 记录块的数量
        } catch (IOException e) {
            // 发生异常时，清理资源
            if (output != null) {
                output.close();
                output.getChannel().deleteChannel();
            }
            throw e;
        }

        // 记录溢出文件的元数据
        spillChannelIDs.add(
                new ChannelWithMeta(
                        channel,
                        blockCount,
                        output.getWriteBytes()
                )
        );
        inMemorySortBuffer.clear(); // 清空内存缓冲区
    }
}
