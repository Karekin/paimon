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

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;

/**
 * 本地表查询的实现类，用于缓存数据和文件。
 * 该类实现了 TableQuery 接口，主要用于在本地缓存表数据和文件，以便高效地进行查询操作。
 */
public class LocalTableQuery implements TableQuery {

    // 存储表数据的缓存视图，键为分区（BinaryRow），值为桶的 LookupLevels<KeyValue> 映射
    private final Map<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> tableView;

    // 核心配置选项
    private final CoreOptions options;

    // 键比较器的提供者，用于生成 InternalRow 的比较器
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;

    // KeyValue 文件读取器工厂的构建器
    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    // 查找存储工厂，用于创建查找存储
    private final LookupStoreFactory lookupStoreFactory;

    // 开始查找的层级（用于确定从哪一层级开始查找数据）
    private final int startLevel;

    // IO 管理器，用于文件的读写操作
    private IOManager ioManager;

    // 查找文件的缓存（可为空）
    @Nullable private Cache<String, LookupFile> lookupFileCache;

    // 分区的行类型
    private final RowType partitionType;

    /**
     * 构造函数，用于初始化 LocalTableQuery。
     * @param table 表对象，用于获取表的核心配置和存储信息
     */
    public LocalTableQuery(FileStoreTable table) {
        this.options = table.coreOptions(); // 获取表的核心配置
        this.tableView = new HashMap<>(); // 初始化表数据缓存视图

        // 获取表的存储对象，并确保它是 KeyValueFileStore 类型
        FileStore<?> tableStore = table.store();
        if (!(tableStore instanceof KeyValueFileStore)) {
            throw new UnsupportedOperationException(
                    "Table Query only supports table with primary key."); // 抛出不支持的异常
        }
        KeyValueFileStore store = (KeyValueFileStore) tableStore;

        // 初始化 KeyValue 文件读取器工厂的构建器
        this.readerFactoryBuilder = store.newReaderFactoryBuilder();

        // 初始化分区类型和键类型
        this.partitionType = table.schema().logicalPartitionType();
        RowType keyType = readerFactoryBuilder.keyType();

        // 初始化键比较器提供者
        this.keyComparatorSupplier = new KeyComparatorSupplier(readerFactoryBuilder.keyType());

        // 初始化查找存储工厂
        this.lookupStoreFactory =
                LookupStoreFactory.create(
                        options,
                        new CacheManager(options.lookupCacheMaxMemory()), // 创建缓存管理器
                        new RowCompactedSerializer(keyType).createSliceComparator()); // 创建行压缩序列化的比较器

        // 根据配置选项确定开始查找的层级
        if (options.needLookup()) {
            startLevel = 1; // 如果需要查找，则从第 1 层开始
        } else {
            // 如果不需要查找，检查配置的序列字段和合并引擎是否支持
            if (options.sequenceField().size() > 0) {
                throw new UnsupportedOperationException(
                        "Not support sequence field definition, but is: "
                                + options.sequenceField());
            }

            if (options.mergeEngine() != DEDUPLICATE) {
                throw new UnsupportedOperationException(
                        "Only support deduplicate merge engine, but is: " + options.mergeEngine());
            }

            startLevel = 0; // 如果不需要查找，则从第 0 层开始
        }
    }

    /**
     * 刷新指定分区和桶的文件信息。
     * @param partition 分区
     * @param bucket 桶
     * @param beforeFiles 刷新前的文件列表
     * @param dataFiles 刷新后的文件列表
     */
    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        // 获取或初始化指定分区和桶的 LookupLevels
        LookupLevels<KeyValue> lookupLevels =
                tableView.computeIfAbsent(partition, k -> new HashMap<>()).get(bucket);

        if (lookupLevels == null) {
            // 如果 LookupLevels 为空，检查 beforeFiles 是否为空（初始阶段应为空）
            Preconditions.checkArgument(
                    beforeFiles.isEmpty(),
                    "The before file should be empty for the initial phase.");
            // 创建新的 LookupLevels
            newLookupLevels(partition, bucket, dataFiles);
        } else {
            // 如果 LookupLevels 存在，更新文件信息
            lookupLevels.getLevels().update(beforeFiles, dataFiles);
        }
    }

    /**
     * 创建新的 LookupLevels。
     * @param partition 分区
     * @param bucket 桶
     * @param dataFiles 文件列表
     */
    private void newLookupLevels(BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        // 创建 Levels 对象，用于管理数据文件的层级
        Levels levels = new Levels(keyComparatorSupplier.get(), dataFiles, options.numLevels());

        // 创建 KeyValue 文件读取器工厂
        KeyValueFileReaderFactory factory =
                readerFactoryBuilder.build(partition, bucket, DeletionVector.emptyFactory());

        // 获取配置选项
        Options options = this.options.toConfiguration();

        // 如果查找文件缓存为空，初始化缓存
        if (lookupFileCache == null) {
            lookupFileCache =
                    LookupFile.createCache(
                            options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION), // 缓存保留时间
                            options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE)); // 缓存最大磁盘大小
        }

        // 创建 LookupLevels 对象
        LookupLevels<KeyValue> lookupLevels =
                new LookupLevels<>(
                        levels,
                        keyComparatorSupplier.get(), // 键比较器
                        readerFactoryBuilder.keyType(), // 键类型
                        new LookupLevels.KeyValueProcessor(
                                readerFactoryBuilder.projectedValueType()), // KeyValue 处理器
                        file ->
                                factory.createRecordReader(
                                        file.schemaId(), // 文件的模式 ID
                                        file.fileName(), // 文件名
                                        file.fileSize(), // 文件大小
                                        file.level()), // 文件层级
                        file ->
                                Preconditions.checkNotNull(ioManager, "IOManager is required.")
                                        .createChannel(
                                                localFilePrefix(
                                                        partitionType, partition, bucket, file))
                                        .getPathFile(), // 获取文件路径
                        lookupStoreFactory, // 查找存储工厂
                        bfGenerator(options), // 生成布隆过滤器
                        lookupFileCache); // 查找文件缓存

        // 将新的 LookupLevels 添加到缓存视图中
        tableView.computeIfAbsent(partition, k -> new HashMap<>()).put(bucket, lookupLevels);
    }

    /**
     * 根据分区、桶和键查找数据。
     * @param partition 分区
     * @param bucket 桶
     * @param key 键
     * @return 查找到的 InternalRow，如果未找到则返回 null
     * @throws IOException 如果发生 IO 异常
     */
    @Nullable
    @Override
    public synchronized InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
            throws IOException {
        // 获取指定分区的桶映射
        Map<Integer, LookupLevels<KeyValue>> buckets = tableView.get(partition);
        if (buckets == null || buckets.isEmpty()) {
            return null; // 如果分区不存在或为空，返回 null
        }

        // 获取指定桶的 LookupLevels
        LookupLevels<KeyValue> lookupLevels = buckets.get(bucket);
        if (lookupLevels == null) {
            return null; // 如果桶不存在，返回 null
        }

        // 查找指定键的数据
        KeyValue kv = lookupLevels.lookup(key, startLevel);
        if (kv == null || kv.valueKind().isRetract()) {
            return null; // 如果未找到或数据被回撤，返回 null
        } else {
            return kv.value(); // 返回查找到的值
        }
    }

    /**
     * 设置值投影。
     * @param projection 投影配置
     * @return 当前 LocalTableQuery 对象
     */
    @Override
    public LocalTableQuery withValueProjection(int[][] projection) {
        this.readerFactoryBuilder.withValueProjection(projection); // 设置投影配置
        return this; // 返回当前对象
    }

    /**
     * 设置 IO 管理器。
     * @param ioManager IO 管理器
     * @return 当前 LocalTableQuery 对象
     */
    public LocalTableQuery withIOManager(IOManager ioManager) {
        this.ioManager = ioManager; // 设置 IO 管理器
        return this; // 返回当前对象
    }

    /**
     * 创建值序列化器。
     * @return 值序列化器
     */
    @Override
    public InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(readerFactoryBuilder.projectedValueType()); // 创建值序列化器
    }

    /**
     * 关闭 LocalTableQuery，释放资源。
     * @throws IOException 如果发生 IO 异常
     */
    @Override
    public void close() throws IOException {
        // 遍历缓存视图并关闭每个 LookupLevels
        for (Map.Entry<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> buckets :
                tableView.entrySet()) {
            for (Map.Entry<Integer, LookupLevels<KeyValue>> bucket :
                    buckets.getValue().entrySet()) {
                bucket.getValue().close(); // 关闭 LookupLevels
            }
        }

        // 清除查找文件缓存
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
        }

        // 清空缓存视图
        tableView.clear();
    }
}
