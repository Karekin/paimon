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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.flink.FlinkConnectorOptions.LookupCacheMode;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.OutOfRangeException;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.apache.paimon.shade.guava30.com.google.common.primitives.Ints;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_CACHE_MODE;
import static org.apache.paimon.flink.query.RemoteTableQuery.isRemoteServiceAvailable;
import static org.apache.paimon.lookup.RocksDBOptions.LOOKUP_CACHE_ROWS;
import static org.apache.paimon.lookup.RocksDBOptions.LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/**
 * 文件存储查找表函数。
 * 它实现了一个查找表函数，用于文件存储（例如 HDFS、本地文件等），支持动态分区加载、缓存刷新等功能。
 * 该类实现了 {@link TableFunction}，用于与 Flink 等数据处理框架集成。
 */
public class FileStoreLookupFunction implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;

    /**
     * 日志记录器，用于记录运行时的日志信息，便于调试和维护。
     */
    private static final Logger LOG = LoggerFactory.getLogger(FileStoreLookupFunction.class);

    /**
     * 文件存储的表对象，包含了表的元数据和数据路径等信息。
     */
    private final Table table;

    /**
     * 动态分区加载器，用于根据当前分区信息加载数据。
     * 如果表不支持动态分区加载，则该值可能为 null。
     */
    @Nullable private final DynamicPartitionLoader partitionLoader;

    /**
     * 需要投影的字段，即从表中查询并返回的字段列表。
     */
    private final List<String> projectFields;

    /**
     * 用于连接的主键字段，用于在主表和查找表之间进行匹配。
     */
    private final List<String> joinKeys;

    /**
     * 数据过滤条件，用于在查询时筛选符合条件的记录。
     */
    @Nullable private final Predicate predicate;

    /**
     * 缓存刷新的时间间隔，类型为 {@link java.time.Duration}。
     * 该字段在对象反序列化时不会被恢复，需要重新初始化。
     */
    private transient Duration refreshInterval;

    /**
     * 临时目录路径，用于存储缓存数据。
     * 该字段在对象反序列化时不会被恢复，需要重新初始化。
     */
    private transient File path;

    /**
     * 缓存表对象，用于存储和管理缓存数据。
     * 该字段在对象反序列化时不会被恢复，需要重新初始化。
     */
    private transient LookupTable lookupTable;

    /**
     * 缓存过期时间，表示下一次缓存刷新的时间。
     * 该字段在对象反序列化时不会被恢复，需要重新初始化。
     */
    private transient long nextLoadTime;

    /**
     * 函数上下文对象，包含函数运行时的环境信息。
     */
    protected FunctionContext functionContext;

    /**
     * 构造方法，用于初始化查找表函数。
     *
     * @param table               文件存储的表对象。
     * @param projection          需要投影的字段索引数组。
     * @param joinKeyIndex        用于连接的主键字段索引数组。
     * @param predicate           数据过滤条件。
     */
    public FileStoreLookupFunction(
            Table table, int[] projection, int[] joinKeyIndex, @Nullable Predicate predicate) {
        /**
         * 验证表是否支持流式读取。
         * 这是确保表在流式处理场景下能够正确工作的必要条件。
         */
        TableScanUtils.streamingReadingValidate(table);

        /**
         * 保存表对象。
         */
        this.table = table;

        /**
         * 初始化动态分区加载器。
         * 该加载器会根据表的分区信息动态加载数据。
         */
        this.partitionLoader = DynamicPartitionLoader.of(table);

        /**
         * 从投影字段索引中提取主键字段名称。
         * 主键字段用于连接主表和查找表。
         */
        this.joinKeys =
                Arrays.stream(joinKeyIndex)
                        .mapToObj(i -> table.rowType().getFieldNames().get(projection[i]))
                        .collect(Collectors.toList());

        /**
         * 从投影字段索引中提取需要投影的字段名称。
         * 这些字段会在查询时返回给用户。
         */
        this.projectFields =
                Arrays.stream(projection)
                        .mapToObj(i -> table.rowType().getFieldNames().get(i))
                        .collect(Collectors.toList());

        /**
         * 确保所有主键字段都被包含在投影字段中。
         * 主键字段对于数据的唯一性和连接的准确性至关重要。
         */
        for (String field : table.primaryKeys()) {
            if (!projectFields.contains(field)) {
                projectFields.add(field);
            }
        }

        /**
         * 如果动态分区加载器存在，将其分区键添加到需要投影的字段和主键中。
         * 这是为了确保分区信息能够被正确加载和查询。
         */
        if (partitionLoader != null) {
            partitionLoader.addPartitionKeysTo(joinKeys, projectFields);
        }

        /**
         * 保存数据过滤条件。
         */
        this.predicate = predicate;
    }

    /**
     * 初始化查找表函数。
     *
     * @param context 函数上下文，包含函数运行时的环境信息。
     * @throws Exception 初始化过程中可能出现的异常。
     */
    public void open(FunctionContext context) throws Exception {
        // 保存函数上下文对象。
        this.functionContext = context;

        /**
         * 获取临时目录路径，并根据该路径初始化函数。
         * 临时目录用于存储缓存数据以及临时文件。
         */
        String tmpDirectory = getTmpDirectory(context);
        open(tmpDirectory);
    }

    /**
     * 根据临时目录路径初始化查找表函数。
     * 当使用临时目录路径重写时，会调用此方法。
     *
     * @param tmpDirectory 临时目录路径。
     * @throws Exception 初始化过程中可能出现的异常。
     */
    void open(String tmpDirectory) throws Exception {
        // 初始化临时目录路径。
        this.path = new File(tmpDirectory, "lookup-" + UUID.randomUUID());

        /**
         * 创建临时目录，如果创建失败，抛出运行时异常。
         * 临时目录是缓存数据存储的必要条件。
         */
        if (!path.mkdirs()) {
            throw new RuntimeException("Failed to create dir: " + path);
        }

        /**
         * 开始初始化查找表函数。
         */
        open();
    }

    /**
     * 初始化查找表函数的核心逻辑。
     *
     * @throws Exception 初始化过程中可能出现的异常。
     */
    private void open() throws Exception {
        if (partitionLoader != null) {
            partitionLoader.open();
        }

        // 初始时，缓存尚未加载。
        this.nextLoadTime = -1;

        /**
         * 从表的配置中读取选项。
         */
        Options options = Options.fromMap(table.options());

        /**
         * 获取刷新时间间隔的配置值。
         * 如果没有配置，则使用默认值。
         */
        this.refreshInterval =
                options.getOptional(LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL)
                        .orElse(options.get(CONTINUOUS_DISCOVERY_INTERVAL));

        /**
         * 获取完整的字段名称列表。
         */
        List<String> fieldNames = table.rowType().getFieldNames();

        /**
         * 将投影字段名称转换为字段索引。
         */
        int[] projection = projectFields.stream().mapToInt(fieldNames::indexOf).toArray();

        /**
         * 获取文件存储表对象。
         */
        FileStoreTable storeTable = (FileStoreTable) table;

        /**
         * 如果缓存模式为自动，并且主键和连接键完全一致，
         * 则尝试创建远程缓存表或本地缓存表。
         */
        if (options.get(LOOKUP_CACHE_MODE) == LookupCacheMode.AUTO
                && new HashSet<>(table.primaryKeys()).equals(new HashSet<>(joinKeys))) {
            if (isRemoteServiceAvailable(storeTable)) {
                /**
                 * 如果远程服务可用，创建远程缓存表。
                 */
                this.lookupTable =
                        PrimaryKeyPartialLookupTable.createRemoteTable(
                                storeTable, projection, joinKeys);
            } else {
                try {
                    /**
                     * 如果远程服务不可用，创建本地缓存表。
                     */
                    this.lookupTable =
                            PrimaryKeyPartialLookupTable.createLocalTable(
                                    storeTable,
                                    projection,
                                    path,
                                    joinKeys,
                                    getRequireCachedBucketIds());
                } catch (UnsupportedOperationException ignore2) {
                }
            }
        }

        if (lookupTable == null) {
            /**
             * 如果缓存表未被创建，使用全缓存模式创建缓存表。
             */
            FullCacheLookupTable.Context context =
                    new FullCacheLookupTable.Context(
                            storeTable,
                            projection,
                            predicate,
                            createProjectedPredicate(projection),
                            path,
                            joinKeys,
                            getRequireCachedBucketIds());
            this.lookupTable = FullCacheLookupTable.create(context, options.get(LOOKUP_CACHE_ROWS));
        }

        /**
         * 刷新动态分区，确保分区信息是最新的。
         */
        refreshDynamicPartition(false);

        /**
         * 打开缓存表，加载数据到缓存。
         */
        lookupTable.open();
    }

    /**
     * 创建投影后的过滤条件。
     *
     * @param projection 投影字段索引数组。
     * @return 投影后的过滤条件。
     */
    @Nullable
    private Predicate createProjectedPredicate(int[] projection) {
        Predicate adjustedPredicate = null;
        if (predicate != null) {
            // 根据投影字段索引调整过滤条件。
            adjustedPredicate =
                    transformFieldMapping(
                            this.predicate,
                            IntStream.range(0, table.rowType().getFieldCount())
                                    .map(i -> Ints.indexOf(projection, i))
                                    .toArray())
                            .orElse(null);
        }
        return adjustedPredicate;
    }

    /**
     * 查找符合条件的记录。
     *
     * @param keyRow 查询键。
     * @return 匹配的记录集合。
     */
    public Collection<RowData> lookup(RowData keyRow) {
        try {
            // 检查缓存是否需要刷新。
            checkRefresh();

            InternalRow key = new FlinkRowWrapper(keyRow);
            if (partitionLoader != null) {
                InternalRow partition = refreshDynamicPartition(true);
                if (partition == null) {
                    return Collections.emptyList();
                }
                key = JoinedRow.join(key, partition);
            }

            List<InternalRow> results = lookupTable.get(key);
            List<RowData> rows = new ArrayList<>(results.size());
            for (InternalRow matchedRow : results) {
                rows.add(new FlinkRowData(matchedRow));
            }
            return rows;
        } catch (OutOfRangeException e) {
            // 如果超出范围，重新打开缓存表。
            reopen();
            return lookup(keyRow);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 刷新动态分区。
     *
     * @param reopen 是否重新打开缓存表。
     * @return 分区数据。
     * @throws Exception 刷新过程中可能出现的异常。
     */
    @Nullable
    private BinaryRow refreshDynamicPartition(boolean reopen) throws Exception {
        if (partitionLoader == null) {
            return null;
        }

        /**
         * 检查分区是否有变化。
         */
        boolean partitionChanged = partitionLoader.checkRefresh();
        BinaryRow partition = partitionLoader.partition();
        if (partition == null) {
            return null;
        }

        /**
         * 根据分区信息创建特定分区过滤条件。
         */
        lookupTable.specificPartitionFilter(createSpecificPartFilter(partition));

        if (partitionChanged && reopen) {
            /**
             * 如果分区发生了变化，并且需要重新打开缓存表，
             * 则关闭并重新打开缓存表。
             */
            lookupTable.close();
            lookupTable.open();
        }

        return partition;
    }

    /**
     * 创建特定分区的过滤条件。
     *
     * @param partition 分区数据。
     * @return 过滤条件。
     */
    private Predicate createSpecificPartFilter(BinaryRow partition) {
        RowType rowType = table.rowType();
        List<String> partitionKeys = table.partitionKeys();
        Object[] partitionSpec =
                new RowDataToObjectArrayConverter(rowType.project(partitionKeys))
                        .convert(partition);
        Map<String, Object> partitionMap = new HashMap<>(partitionSpec.length);
        for (int i = 0; i < partitionSpec.length; i++) {
            partitionMap.put(partitionKeys.get(i), partitionSpec[i]);
        }

        /**
         * 根据行类型创建分区过滤条件。
         */
        return createPartitionPredicate(rowType, partitionMap);
    }

    /**
     * 重新打开缓存表。
     */
    private void reopen() {
        try {
            close();
            open();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 检查缓存是否需要刷新。
     *
     * @throws Exception 检查过程中可能出现的异常。
     */
    private void checkRefresh() throws Exception {
        if (nextLoadTime > System.currentTimeMillis()) {
            return;
        }
        if (nextLoadTime > 0) {
            LOG.info(
                    "Lookup table {} has refreshed after {} second(s), refreshing",
                    table.name(),
                    refreshInterval.toMillis() / 1000);
        }

        /**
         * 刷新缓存表。
         */
        refresh();

        /**
         * 计算下一次刷新的时间。
         */
        nextLoadTime = System.currentTimeMillis() + refreshInterval.toMillis();
    }

    /**
     * 对外暴露缓存表对象，用于测试。
     *
     * @return 缓存表对象。
     */
    @VisibleForTesting
    LookupTable lookupTable() {
        return lookupTable;
    }

    /**
     * 刷新缓存表。
     *
     * @throws Exception 刷新过程中可能出现的异常。
     */
    private void refresh() throws Exception {
        lookupTable.refresh();
    }

    /**
     * 关闭查找表函数。
     *
     * @throws IOException 关闭过程中可能出现的 I/O 异常。
     */
    @Override
    public void close() throws IOException {
        if (lookupTable != null) {
            // 关闭缓存表。
            lookupTable.close();
            lookupTable = null;
        }

        if (path != null) {
            // 删除临时目录。
            FileIOUtils.deleteDirectoryQuietly(path);
        }
    }

    /**
     * 获取临时目录路径。
     *
     * @param context 函数上下文，包含函数运行时的环境信息。
     * @return 临时目录路径。
     */
    private static String getTmpDirectory(FunctionContext context) {
        try {
            // 获取函数上下文中的运行时环境信息。
            Field field = context.getClass().getDeclaredField("context");
            field.setAccessible(true);
            StreamingRuntimeContext runtimeContext =
                    extractStreamingRuntimeContext(field.get(context));
            String[] tmpDirectories =
                    runtimeContext.getTaskManagerRuntimeInfo().getTmpDirectories();
            return tmpDirectories[ThreadLocalRandom.current().nextInt(tmpDirectories.length)];
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 提取流式运行时上下文对象。
     *
     * @param runtimeContext 运行时上下文对象。
     * @return 流式运行时上下文对象。
     * @throws NoSuchFieldException   如果运行时上下文对象中没有相关字段。
     * @throws IllegalAccessException 如果无法访问运行时上下文对象中的字段。
     */
    private static StreamingRuntimeContext extractStreamingRuntimeContext(Object runtimeContext)
            throws NoSuchFieldException, IllegalAccessException {
        if (runtimeContext instanceof StreamingRuntimeContext) {
            return (StreamingRuntimeContext) runtimeContext;
        }

        Field field = runtimeContext.getClass().getDeclaredField("runtimeContext");
        field.setAccessible(true);
        return extractStreamingRuntimeContext(field.get(runtimeContext));
    }

    /**
     * 获取当前查找连接子任务需要缓存的桶 ID 集合。
     *
     * @return 缓存的桶 ID 集合。
     */
    protected Set<Integer> getRequireCachedBucketIds() {
        // TODO: Implement the method when Flink support bucket shuffle for lookup join.
        return null;
    }
}
