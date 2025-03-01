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

package org.apache.paimon.lookup.hash;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.io.CompressedPageFileOutput;
import org.apache.paimon.io.PageFileOutput;
import org.apache.paimon.lookup.LookupStoreFactory.Context;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** 哈希键值存储的内部写入实现。 */
public class HashLookupStoreWriter implements LookupStoreWriter {

    private static final Logger LOG = LoggerFactory.getLogger(HashLookupStoreWriter.class.getName());

    // 负载因子（哈希表的最大填充率），默认为 0.75
    private final double loadFactor;
    // 临时文件夹，用于存储写入过程中的临时文件
    private final File tempFolder;
    // 输出文件，最终存储数据的文件
    private final File outputFile;
    // 索引文件数组
    private File[] indexFiles;
    // 索引流数组，用于写入索引数据
    private DataOutputStream[] indexStreams;
    // 数据文件数组
    private File[] dataFiles;
    // 数据流数组，用于写入数据
    private DataOutputStream[] dataStreams;
    // 上一次写入的值缓存
    private byte[][] lastValues;
    // 上一次写入的值长度缓存
    private int[] lastValuesLength;
    // 数据流的当前长度
    private long[] dataLengths;
    // 偏移量的最大长度（用于记录索引文件中偏移量的长度）
    private int[] maxOffsetLengths;
    // 键的数量
    private int keyCount;
    // 每个键长度对应的键数量
    private int[] keyCounts;
    // 值的数量
    private int valueCount;
    // 冲突的数量（哈希冲突）
    private int collisions;

    @Nullable private final BloomFilter.Builder bloomFilter; // 布隆过滤器构建器

    @Nullable private final BlockCompressionFactory compressionFactory; // 压缩工厂
    private final int compressPageSize; // 压缩页面大小

    HashLookupStoreWriter(
            double loadFactor,
            File file,
            @Nullable BloomFilter.Builder bloomFilter,
            @Nullable BlockCompressionFactory compressionFactory,
            int compressPageSize)
            throws IOException {
        this.loadFactor = loadFactor;
        this.outputFile = file;
        this.compressionFactory = compressionFactory;
        this.compressPageSize = compressPageSize;
        if (loadFactor <= 0.0 || loadFactor >= 1.0) {
            throw new IllegalArgumentException(
                    "非法负载因子 = " + loadFactor + "，应在 0.0 和 1.0 之间。");
        }

        // 创建临时文件夹
        this.tempFolder = new File(file.getParentFile(), UUID.randomUUID().toString());
        if (!tempFolder.mkdir()) {
            throw new IOException("无法创建临时文件夹: " + tempFolder);
        }
        // 初始化相关数组
        this.indexStreams = new DataOutputStream[0];
        this.dataStreams = new DataOutputStream[0];
        this.indexFiles = new File[0];
        this.dataFiles = new File[0];
        this.lastValues = new byte[0][];
        this.lastValuesLength = new int[0];
        this.dataLengths = new long[0];
        this.maxOffsetLengths = new int[0];
        this.keyCounts = new int[0];
        this.bloomFilter = bloomFilter;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        int keyLength = key.length; // 键的长度

        // 获取对应键长度的索引流
        DataOutputStream indexStream = getIndexStream(keyLength);

        // 写入键
        indexStream.write(key);

        // 检查当前值是否与上次写入的值相同
        byte[] lastValue = lastValues[keyLength];
        boolean sameValue = lastValue != null && Arrays.equals(value, lastValue);

        // 计算数据流的当前偏移量
        long dataLength = dataLengths[keyLength];
        if (sameValue) {
            dataLength -= lastValuesLength[keyLength]; // 如果值相同，调整偏移量
        }

        // 记录偏移量的长度
        int offsetLength = VarLengthIntUtils.encodeLong(indexStream, dataLength);
        maxOffsetLengths[keyLength] = Math.max(offsetLength, maxOffsetLengths[keyLength]);

        // 如果值不同，写入数据流
        if (!sameValue) {
            // 获取对应键长度的数据流
            DataOutputStream dataStream = getDataStream(keyLength);

            // 写入值的长度和值本身
            int valueSize = VarLengthIntUtils.encodeInt(dataStream, value.length);
            dataStream.write(value);

            // 更新数据流的长度
            dataLengths[keyLength] += valueSize + value.length;

            // 更新上一次写入的值缓存
            lastValues[keyLength] = value;
            lastValuesLength[keyLength] = valueSize + value.length;

            valueCount++; // 值的数量递增
        }

        keyCount++; // 键的数量递增
        keyCounts[keyLength]++; // 对应键长度的键数量递增

        // 如果使用了布隆过滤器，将键的哈希值添加到过滤器中
        if (bloomFilter != null) {
            bloomFilter.addHash(MurmurHashUtils.hashBytes(key));
        }
    }

    @Override
    public Context close() throws IOException {
        // 关闭所有数据流和索引流
        for (DataOutputStream dos : dataStreams) {
            if (dos != null) {
                dos.close();
            }
        }
        for (DataOutputStream dos : indexStreams) {
            if (dos != null) {
                dos.close();
            }
        }

        // 输出统计信息
        LOG.info("键的数量: {}", keyCount);
        LOG.info("值的数量: {}", valueCount);

        // 准备需要合并的文件列表
        List<File> filesToMerge = new ArrayList<>();

        // 初始化返回的上下文对象
        int bloomFilterBytes = bloomFilter == null ? 0 : bloomFilter.getBuffer().size();
        HashContext context = new HashContext(
                bloomFilter != null,
                bloomFilter == null ? 0 : bloomFilter.expectedEntries(),
                bloomFilterBytes,
                new int[keyCounts.length],
                new int[keyCounts.length],
                new int[keyCounts.length],
                new int[keyCounts.length],
                new long[keyCounts.length],
                0,
                null);

        // 计算索引和数据的长度
        long indexesLength = bloomFilterBytes;
        long datasLength = 0;

        // 填充上下文对象的各个字段
        for (int i = 0; i < this.keyCounts.length; i++) {
            if (this.keyCounts[i] > 0) {
                // 写入键的数量
                context.keyCounts[i] = keyCounts[i];
                // 写入槽位的数量（根据负载因子计算）
                context.slots[i] = (int) Math.round(keyCounts[i] / loadFactor);
                // 写入槽的大小（键长度 + 偏移量长度）
                context.slotSizes[i] = i + maxOffsetLengths[i];
                // 写入索引的偏移量
                context.indexOffsets[i] = (int) indexesLength;
                // 增加索引的长度
                indexesLength += (long) (i + maxOffsetLengths[i]) * context.slots[i];
                // 写入数据的偏移量
                context.dataOffsets[i] = datasLength;
                // 增加数据的长度
                datasLength += dataLengths[i];
            }
        }

        // 调整数据偏移量以适应合并后的文件结构
        for (int i = 0; i < context.dataOffsets.length; i++) {
            context.dataOffsets[i] = indexesLength + context.dataOffsets[i];
        }

        // 创建输出流
        PageFileOutput output = PageFileOutput.create(outputFile, compressPageSize, compressionFactory);
        try {
            // 如果有布隆过滤器，写入布隆过滤器文件
            if (bloomFilter != null) {
                File bloomFilterFile = new File(tempFolder, "bloomfilter.dat");
                bloomFilterFile.deleteOnExit();
                try (FileOutputStream bfOutputStream = new FileOutputStream(bloomFilterFile)) {
                    bfOutputStream.write(bloomFilter.getBuffer().getArray());
                    LOG.info("布隆过滤器大小: {} 字节", bloomFilter.getBuffer().size());
                }
                filesToMerge.add(bloomFilterFile);
            }

            // 构建索引文件并加入合并列表
            for (int i = 0; i < indexFiles.length; i++) {
                if (indexFiles[i] != null) {
                    filesToMerge.add(buildIndex(i));
                }
            }

            // 输出冲突信息
            LOG.info("冲突的数量: {}", collisions);

            // 将数据文件加入合并列表
            for (File dataFile : dataFiles) {
                if (dataFile != null) {
                    filesToMerge.add(dataFile);
                }
            }

            // 检查磁盘空间是否足够
            checkFreeDiskSpace(filesToMerge);

            // 合并文件并写入输出流
            mergeFiles(filesToMerge, output);
        } finally {
            // 清理临时文件
            cleanup(filesToMerge);
            output.close();
        }

        // 输出最后的存储文件大小
        LOG.info(
                "压缩后的总存储大小: {} Mb",
                new DecimalFormat("#,##0.0").format(outputFile.length() / (1024 * 1024)));

        // 如果输出是压缩的，更新上下文对象
        if (output instanceof CompressedPageFileOutput) {
            CompressedPageFileOutput compressedOutput = (CompressedPageFileOutput) output;
            context = context.copy(compressedOutput.uncompressBytes(), compressedOutput.pages());
        }
        return context;
    }

    /**
     * 构建索引文件。
     *
     * @param keyLength 键的长度
     * @return 索引文件
     * @throws IOException 如果发生 I/O 错误
     */
    private File buildIndex(int keyLength) throws IOException {
        long count = keyCounts[keyLength]; // 键的数量
        int slots = (int) Math.round(count / loadFactor); // 槽的数量
        int offsetLength = maxOffsetLengths[keyLength]; // 偏移量长度
        int slotSize = keyLength + offsetLength; // 槽的大小

        // 初始化索引文件
        File indexFile = new File(tempFolder, "index" + keyLength + ".dat");
        try (RandomAccessFile indexAccessFile = new RandomAccessFile(indexFile, "rw")) {
            indexAccessFile.setLength((long) slots * slotSize); // 设置文件长度
            FileChannel indexChannel = indexAccessFile.getChannel();
            MappedByteBuffer byteBuffer = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexAccessFile.length());

            // 初始化临时流
            File tempIndexFile = indexFiles[keyLength];
            DataInputStream tempIndexStream = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(tempIndexFile)));

            try {
                byte[] keyBuffer = new byte[keyLength]; // 临时缓冲区，用于存储键
                byte[] slotBuffer = new byte[slotSize]; // 临时缓冲区，用于存储槽
                byte[] offsetBuffer = new byte[offsetLength]; // 临时缓冲区，用于存储偏移量

                // 读取所有键
                for (int i = 0; i < count; i++) {
                    // 读取键
                    tempIndexStream.readFully(keyBuffer);

                    // 读取偏移量
                    long offset = VarLengthIntUtils.decodeLong(tempIndexStream);

                    // 计算哈希值
                    long hash = MurmurHashUtils.hashBytesPositive(keyBuffer);

                    boolean collision = false;
                    // 探测槽位，寻找适合插入的位置
                    for (int probe = 0; probe < count; probe++) {
                        int slot = (int) ((hash + probe) % slots); // 计算槽的位置
                        byteBuffer.position(slot * slotSize); // 移动到对应的位置
                        byteBuffer.get(slotBuffer); // 读取槽的数据

                        // 解码偏移量
                        long found = VarLengthIntUtils.decodeLong(slotBuffer, keyLength);
                        if (found == 0) {
                            // 空槽，插入键和偏移量
                            byteBuffer.position(slot * slotSize);
                            byteBuffer.put(keyBuffer);
                            int pos = VarLengthIntUtils.encodeLong(offsetBuffer, offset);
                            byteBuffer.put(offsetBuffer, 0, pos);
                            break;
                        } else {
                            collision = true; // 发生冲突
                            // 检查是否为重复键
                            if (Arrays.equals(keyBuffer, Arrays.copyOf(slotBuffer, keyLength))) {
                                throw new RuntimeException("发现重复键: " + Arrays.toString(keyBuffer));
                            }
                        }
                    }

                    if (collision) {
                        collisions++; // 冲突数量递增
                    }
                }

                // 记录构建的索引文件信息
                String msg = String.format(
                        "  最大的偏移量长度：%d bytes%n  槽的大小：%d bytes",
                        offsetLength, slotSize);

                LOG.info("已构建索引文件: {}\n{}", indexFile.getName(), msg);
            } finally {
                // 关闭输入流
                tempIndexStream.close();

                // 关闭通道
                indexChannel.close();

                // 删除临时索引文件
                if (tempIndexFile.delete()) {
                    LOG.info("已删除临时索引文件: {}", tempIndexFile.getName());
                }
            }
        }

        return indexFile;
    }

    /**
     * 检查磁盘空间是否足够。
     *
     * @param inputFiles 需要合并的文件列表
     */
    private void checkFreeDiskSpace(List<File> inputFiles) {
        // 检查磁盘空间
        long usableSpace = 0;
        long totalSize = 0;
        for (File f : inputFiles) {
            if (f.exists()) {
                totalSize += f.length(); // 累加文件大小
                usableSpace = f.getUsableSpace(); // 获取可用的磁盘空间
            }
        }
        LOG.info("预计存储总大小: {} Mb", new DecimalFormat("#,##0.0").format(totalSize / (1024 * 1024)));
        LOG.info("系统可用的免费空间: {} Mb", new DecimalFormat("#,##0.0").format(usableSpace / (1024 * 1024)));
        if (totalSize / (double) usableSpace >= 0.66) {
            throw new RuntimeException("磁盘空间不足，终止操作");
        }
    }

    /**
     * 合并文件到输出流。
     *
     * @param inputFiles 需要合并的文件列表
     * @param output 输出流
     * @throws IOException 如果发生 I/O 错误
     */
    private void mergeFiles(List<File> inputFiles, PageFileOutput output) throws IOException {
        long startTime = System.nanoTime(); // 记录开始时间

        // 合并文件
        for (File f : inputFiles) {
            if (f.exists()) {
                try (FileInputStream fileInputStream = new FileInputStream(f);
                     BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
                    LOG.info("合并文件: {} 大小: {}", f.getName(), f.length());

                    byte[] buffer = new byte[8192]; // 缓冲区
                    int length;
                    while ((length = bufferedInputStream.read(buffer)) > 0) {
                        output.write(buffer, 0, length); // 写入数据
                    }
                }
            } else {
                LOG.info("跳过合并文件: {} 因为它不存在", f.getName());
            }
        }

        LOG.info("合并时间: {} s", ((System.nanoTime() - startTime) / 1000000000.0));
    }

    /**
     * 清理临时文件和文件夹。
     *
     * @param inputFiles 需要清理的文件列表
     */
    private void cleanup(List<File> inputFiles) {
        for (File f : inputFiles) {
            if (f.exists() && f.delete()) {
                LOG.info("已删除临时文件: {}", f.getName());
            }
        }
        if (tempFolder.exists() && tempFolder.delete()) {
            LOG.info("已删除临时文件夹: {}", tempFolder.getAbsolutePath());
        }
    }

    /**
     * 根据键的长度获取对应的数据流，如果需要则创建一个新的文件。
     *
     * @param keyLength 键的长度
     * @return 数据流
     * @throws IOException 如果发生 I/O 错误
     */
    private DataOutputStream getDataStream(int keyLength) throws IOException {
        // 扩展数组大小，确保足够容纳键长度
        if (dataStreams.length <= keyLength) {
            dataStreams = Arrays.copyOf(dataStreams, keyLength + 1);
            dataFiles = Arrays.copyOf(dataFiles, keyLength + 1);
        }

        DataOutputStream dos = dataStreams[keyLength];
        if (dos == null) {
            File file = new File(tempFolder, "data" + keyLength + ".dat");
            file.deleteOnExit(); // 设置为 JVM 退出时删除
            dataFiles[keyLength] = file;

            // 创建数据流
            dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            dataStreams[keyLength] = dos;

            // 写入一个字节，确保偏移量从 1 开始
            dos.writeByte(0);
        }
        return dos;
    }

    /**
     * 根据键的长度获取对应的索引流，如果需要则创建一个新的文件。
     *
     * @param keyLength 键的长度
     * @return 索引流
     * @throws IOException 如果发生 I/O 错误
     */
    private DataOutputStream getIndexStream(int keyLength) throws IOException {
        // 扩展数组大小，确保足够容纳键长度
        if (indexStreams.length <= keyLength) {
            indexStreams = Arrays.copyOf(indexStreams, keyLength + 1);
            indexFiles = Arrays.copyOf(indexFiles, keyLength + 1);
            keyCounts = Arrays.copyOf(keyCounts, keyLength + 1);
            maxOffsetLengths = Arrays.copyOf(maxOffsetLengths, keyLength + 1);
            lastValues = Arrays.copyOf(lastValues, keyLength + 1);
            lastValuesLength = Arrays.copyOf(lastValuesLength, keyLength + 1);
            dataLengths = Arrays.copyOf(dataLengths, keyLength + 1);
        }

        DataOutputStream dos = indexStreams[keyLength];
        if (dos == null) {
            File file = new File(tempFolder, "temp_index" + keyLength + ".dat");
            file.deleteOnExit(); // 设置为 JVM 退出时删除
            indexFiles[keyLength] = file;

            // 创建索引流
            dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            indexStreams[keyLength] = dos;

            dataLengths[keyLength]++; // 数据长度递增
        }
        return dos;
    }
}
