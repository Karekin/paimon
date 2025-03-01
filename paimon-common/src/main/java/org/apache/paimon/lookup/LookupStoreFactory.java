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

package org.apache.paimon.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.hash.HashLookupStoreFactory;
import org.apache.paimon.lookup.sort.SortLookupStoreFactory;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.function.Function;

/**
 * 查找存储工厂接口，用于创建键值查找存储。
 * 该接口定义了创建查找存储写入器和读取器的规范，以及一些静态方法用于生成布隆过滤器和创建具体的工厂实例。
 */
public interface LookupStoreFactory {

    /**
     * 创建查找存储写入器。
     * @param file 文件对象，表示存储数据的文件
     * @param bloomFilter 布隆过滤器构建器（可为空）
     * @return 查找存储写入器
     * @throws IOException 如果发生 IO 异常
     */
    LookupStoreWriter createWriter(File file, @Nullable BloomFilter.Builder bloomFilter)
            throws IOException;

    /**
     * 创建查找存储读取器。
     * @param file 文件对象，表示存储数据的文件
     * @param context 上下文对象，用于在写入器和读取器之间传递共享信息
     * @return 查找存储读取器
     * @throws IOException 如果发生 IO 异常
     */
    LookupStoreReader createReader(File file, Context context) throws IOException;

    /**
     * 创建布隆过滤器生成器。
     * 该方法根据配置生成布隆过滤器的生成器函数，用于在数据写入时生成布隆过滤器。
     * @param options 配置选项
     * @return 布隆过滤器生成器函数
     */
    static Function<Long, BloomFilter.Builder> bfGenerator(Options options) {
        Function<Long, BloomFilter.Builder> bfGenerator = rowCount -> null;

        // 如果启用了布隆过滤器
        if (options.get(CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_ENABLED)) {
            double bfFpp = options.get(CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_FPP);
            bfGenerator =
                    rowCount -> {
                        if (rowCount > 0) {
                            return BloomFilter.builder(rowCount, bfFpp);
                        }
                        return null;
                    };
        }

        return bfGenerator;
    }

    /**
     * 创建具体的 LookupStoreFactory 实例。
     * 根据配置选项创建不同类型的查找存储工厂，支持排序类型和哈希类型。
     * @param options 核心配置选项
     * @param cacheManager 缓存管理器
     * @param keyComparator 键比较器
     * @return 查找存储工厂实例
     * @throws IllegalArgumentException 如果不支持的文件类型被指定
     */
    static LookupStoreFactory create(
            CoreOptions options, CacheManager cacheManager, Comparator<MemorySlice> keyComparator) {
        CompressOptions compression = options.lookupCompressOptions();

        // 根据配置的查找本地文件类型创建对应的工厂
        switch (options.lookupLocalFileType()) {
            case SORT:
                return new SortLookupStoreFactory(
                        keyComparator, cacheManager, options.cachePageSize(), compression);
            case HASH:
                return new HashLookupStoreFactory(
                        cacheManager,
                        options.cachePageSize(),
                        options.toConfiguration().get(CoreOptions.LOOKUP_HASH_LOAD_FACTOR),
                        compression);
            default:
                throw new IllegalArgumentException(
                        "Unsupported lookup local file type: " + options.lookupLocalFileType());
        }
    }

    /**
     * 上下文接口，用于在写入器和读取器之间传递共享信息。
     * 该接口目前没有具体的实现，但提供了一个通用的上下文概念，用于在写入器和读取器之间传递必要的数据或状态。
     */
    interface Context {}
}
