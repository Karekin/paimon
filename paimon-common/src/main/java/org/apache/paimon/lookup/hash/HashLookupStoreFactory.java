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
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

/**
 * 使用哈希查找记录的查找存储工厂。
 * 该类实现了 LookupStoreFactory 接口，用于创建基于哈希的查找存储。
 */
public class HashLookupStoreFactory implements LookupStoreFactory {

    // 缓存管理器，用于管理数据块的缓存
    private final CacheManager cacheManager;

    // 缓存页大小，表示每个缓存块的大小
    private final int cachePageSize;

    // 负载因子，用于控制哈希表的负载
    private final double loadFactor;

    // 压缩工厂（可为空），用于对数据进行压缩和解压缩
    @Nullable private final BlockCompressionFactory compressionFactory;

    /**
     * 构造函数，初始化 HashLookupStoreFactory。
     * @param cacheManager 缓存管理器
     * @param cachePageSize 缓存页大小
     * @param loadFactor 负载因子
     * @param compression 压缩选项
     */
    public HashLookupStoreFactory(
            CacheManager cacheManager,
            int cachePageSize,
            double loadFactor,
            CompressOptions compression) {
        this.cacheManager = cacheManager; // 保存缓存管理器
        this.cachePageSize = cachePageSize; // 保存缓存页大小
        this.loadFactor = loadFactor; // 保存负载因子
        this.compressionFactory = BlockCompressionFactory.create(compression); // 创建压缩工厂
    }

    /**
     * 创建查找存储读取器。
     * @param file 文件对象，表示存储数据的文件
     * @param context 上下文对象，用于在写入器和读取器之间传递共享信息
     * @return 查找存储读取器
     * @throws IOException 如果发生 IO 异常
     */
    @Override
    public HashLookupStoreReader createReader(File file, Context context) throws IOException {
        // 调用 HashLookupStoreReader 的构造函数，创建一个读取器
        return new HashLookupStoreReader(
                file, // 文件对象
                (HashContext) context, // 上下文对象（必须是 HashContext 类型）
                cacheManager, // 缓存管理器
                cachePageSize, // 缓存页大小
                compressionFactory // 压缩工厂
        );
    }

    /**
     * 创建查找存储写入器。
     * @param file 文件对象，表示存储数据的文件
     * @param bloomFilter 布隆过滤器构建器（可为空）
     * @return 查找存储写入器
     * @throws IOException 如果发生 IO 异常
     */
    @Override
    public HashLookupStoreWriter createWriter(File file, @Nullable BloomFilter.Builder bloomFilter)
            throws IOException {
        // 调用 HashLookupStoreWriter 的构造函数，创建一个写入器
        return new HashLookupStoreWriter(
                loadFactor, // 负载因子
                file, // 文件对象
                bloomFilter, // 布隆过滤器构建器
                compressionFactory, // 压缩工厂
                cachePageSize // 缓存页大小
        );
    }
}
