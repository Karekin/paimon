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
import org.apache.paimon.io.PageFileInput;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.io.cache.FileBasedRandomInputView;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.VarLengthIntUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;


/**
 * 内部哈希键值对存储的读取器实现类
 */
public class HashLookupStoreReader
        implements LookupStoreReader, Iterable<Map.Entry<byte[], byte[]>> {

    private static final Logger LOG =
            LoggerFactory.getLogger(HashLookupStoreReader.class.getName());

    // 不同键长度对应的键计数数组
    private final int[] keyCounts;
    // 不同键长度对应的槽大小数组
    private final int[] slotSizes;
    // 不同键长度对应的槽数目数组
    private final int[] slots;
    // 不同键长度对应的索引偏移数组
    private final int[] indexOffsets;
    // 不同键长度对应的数据偏移数组
    private final long[] dataOffsets;

    // 文件输入视图，用于随机访问文件
    private FileBasedRandomInputView inputView;

    // 索引缓存使用的槽缓冲区，大小为最大槽大小
    private final byte[] slotBuffer;

    // 布隆过滤器，可选
    @Nullable
    private FileBasedBloomFilter bloomFilter;

    /**
     * 构造函数，用于初始化哈希存储读取器
     *
     * @param file 文件路径
     * @param context 上下文信息，包含索引和数据分布的配置
     * @param cacheManager 缓存管理器
     * @param cachePageSize 缓存页大小
     * @param compressionFactory 数据压缩策略
     * @throws IOException 文件操作异常
     */
    public HashLookupStoreReader(
            File file,
            HashContext context,
            CacheManager cacheManager,
            int cachePageSize,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        // 确保文件存在
        if (!file.exists()) {
            throw new FileNotFoundException("文件 " + file.getAbsolutePath() + " 不存在");
        }

        // 初始化键计数、槽数目、槽大小
        this.keyCounts = context.keyCounts;
        this.slots = context.slots;
        this.slotSizes = context.slotSizes;

        // 计算最大槽大小
        int maxSlotSize = 0;
        for (int slotSize : slotSizes) {
            if (slotSize > maxSlotSize) {
                maxSlotSize = slotSize;
            }
        }

        // 初始化槽缓冲区，大小为最大槽大小
        this.slotBuffer = new byte[maxSlotSize];

        // 初始化索引偏移和数据偏移
        this.indexOffsets = context.indexOffsets;
        this.dataOffsets = context.dataOffsets;

        // 打开文件前的日志记录
        LOG.info("打开文件 {}", file.getName());

        // 创建文件输入视图
        PageFileInput fileInput = PageFileInput.create(
                file,
                cachePageSize,
                compressionFactory,
                context.uncompressBytes,
                context.compressPages
        );

        // 初始化文件输入视图
        this.inputView = new FileBasedRandomInputView(fileInput, cacheManager);

        // 初始化布隆过滤器
        if (context.bloomFilterEnabled) {
            this.bloomFilter = new FileBasedBloomFilter(
                    fileInput,
                    cacheManager,
                    context.bloomFilterExpectedEntries,
                    0,
                    context.bloomFilterBytes
            );
        }
    }

    /**
     * 根据键查找对应的值
     *
     * @param key 键
     * @return 对应的值，若不存在返回 null
     * @throws IOException 文件读取异常
     */
    @Override
    public byte[] lookup(byte[] key) throws IOException {
        int keyLength = key.length;

        // 检查键长度是否超出范围或对应键数目为 0
        if (keyLength >= keyCounts.length || keyCounts[keyLength] == 0) {
            return null;
        }

        // 计算哈希值
        int hashcode = MurmurHashUtils.hashBytes(key);

        // 布隆过滤器快速检查
        if (bloomFilter != null && !bloomFilter.testHash(hashcode)) {
            return null;
        }

        // 转换哈希值为正数
        long hashPositive = hashcode & 0x7fffffff;

        // 获取当前键长度对应的槽数目、槽大小等信息
        int numSlots = slots[keyLength];
        int slotSize = slotSizes[keyLength];
        int indexOffset = indexOffsets[keyLength];
        long dataOffset = dataOffsets[keyLength];

        // 线性探测寻找键
        for (int probe = 0; probe < numSlots; probe++) {
            // 计算槽位置
            long slot = (hashPositive + probe) % numSlots;

            // 定位到索引位置
            inputView.setReadPosition(indexOffset + slot * slotSize);

            // 读取槽数据
            inputView.readFully(slotBuffer, 0, slotSize);

            // 解码数据偏移量
            long offset = VarLengthIntUtils.decodeLong(slotBuffer, keyLength);

            // 检查是否为空
            if (offset == 0) {
                continue;
            }

            // 比较键是否匹配
            if (isKey(slotBuffer, key)) {
                // 获取值
                return getValue(dataOffset + offset);
            }
        }

        // 未找到键，返回 null
        return null;
    }

    /**
     * 比较槽中的键是否与目标键匹配
     *
     * @param slotBuffer 槽中的键数据
     * @param key 目标键
     * @return 是否匹配
     */
    private boolean isKey(byte[] slotBuffer, byte[] key) {
        for (int i = 0; i < key.length; i++) {
            if (slotBuffer[i] != key[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * 根据偏移量获取值
     *
     * @param offset 值在数据区的偏移量
     * @return 值
     * @throws IOException 文件读取异常
     */
    private byte[] getValue(long offset) throws IOException {
        inputView.setReadPosition(offset);

        // 读取数据长度
        int size = VarLengthIntUtils.decodeInt(inputView);

        // 读取数据
        byte[] res = new byte[size];
        inputView.readFully(res);
        return res;
    }

    /**
     * 关闭文件输入视图
     *
     * @throws IOException 文件关闭异常
     */
    @Override
    public void close() throws IOException {
        inputView.close();
        inputView = null;
    }

    /**
     * 获取迭代器，用于遍历存储中的键值对
     *
     * @return 迭代器
     */
    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
        return new StorageIterator(true);
    }

    /**
     * 获取只包含键的迭代器
     *
     * @return 迭代器
     */
    public Iterator<Map.Entry<byte[], byte[]>> keys() {
        return new StorageIterator(false);
    }

    /**
     * 存储迭代器，用于遍历存储中的键值对
     */
    private class StorageIterator implements Iterator<Map.Entry<byte[], byte[]>> {

        // 重用的键值对对象，减少 GC 压力
        private final FastEntry entry = new FastEntry();
        // 是否包含值
        private final boolean withValue;
        // 当前处理的键长度
        private int currentKeyLength = 0;
        // 当前槽缓冲区
        private byte[] currentSlotBuffer;
        // 当前已处理的键数目
        private long keyIndex = 0;
        // 当前键长度的键总数
        private long keyLimit = 0;
        // 当前数据区偏移量
        private long currentDataOffset;
        // 当前索引区偏移量
        private int currentIndexOffset;

        /**
         * 构造函数，初始化迭代器
         *
         * @param withValue 是否包含值
         */
        public StorageIterator(boolean withValue) {
            this.withValue = withValue;
            nextKeyLength();
        }

        /**
         * 跳转到下一个键长度
         */
        private void nextKeyLength() {
            for (int i = currentKeyLength + 1; i < keyCounts.length; i++) {
                // 查找下一个有效键长度
                long c = keyCounts[i];
                if (c > 0) {
                    currentKeyLength = i;
                    // 更新当前需处理的键数目
                    keyLimit += c;
                    // 更新当前槽缓冲区大小
                    currentSlotBuffer = new byte[slotSizes[i]];
                    // 更新索引区和数据区偏移量
                    currentIndexOffset = indexOffsets[i];
                    currentDataOffset = dataOffsets[i];
                    break;
                }
            }
        }

        /**
         * 是否还有下一个元素
         *
         * @return 是否还有下一个元素
         */
        @Override
        public boolean hasNext() {
            return keyIndex < keyLimit;
        }

        /**
         * 获取下一个键值对
         *
         * @return 下一个键值对
         */
        @Override
        public FastEntry next() {
            try {
                inputView.setReadPosition(currentIndexOffset);

                long offset = 0;
                // 跳过空槽
                while (offset == 0) {
                    inputView.readFully(currentSlotBuffer);
                    // 解码数据偏移量
                    offset = VarLengthIntUtils.decodeLong(currentSlotBuffer, currentKeyLength);
                    currentIndexOffset += currentSlotBuffer.length;
                }

                // 获取键
                byte[] key = Arrays.copyOf(currentSlotBuffer, currentKeyLength);

                byte[] value = null;
                if (withValue) {
                    // 获取值
                    long valueOffset = currentDataOffset + offset;
                    value = getValue(valueOffset);
                }

                // 设置键值对
                entry.set(key, value);

                // 如果已达当前键长度的键总数，跳转到下一个键长度
                if (++keyIndex == keyLimit) {
                    nextKeyLength();
                }

                return entry;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        /**
         * 移除操作未实现
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException("尚未实现");
        }

        /**
         * 快速键值对类，减少对象创建开销
         */
        private class FastEntry implements Map.Entry<byte[], byte[]> {

            private byte[] key;
            private byte[] val;

            /**
             * 设置键值对
             *
             * @param k 键
             * @param v 值
             */
            protected void set(byte[] k, byte[] v) {
                this.key = k;
                this.val = v;
            }

            /**
             * 获取键
             *
             * @return 键
             */
            @Override
            public byte[] getKey() {
                return key;
            }

            /**
             * 获取值
             *
             * @return 值
             */
            @Override
            public byte[] getValue() {
                return val;
            }

            /**
             * 设置值（未实现）
             *
             * @param value 新值
             * @return 旧值
             * @throws UnsupportedOperationException 该操作未实现
             */
            @Override
            public byte[] setValue(byte[] value) {
                throw new UnsupportedOperationException("不支持此操作");
            }
        }
    }
}
