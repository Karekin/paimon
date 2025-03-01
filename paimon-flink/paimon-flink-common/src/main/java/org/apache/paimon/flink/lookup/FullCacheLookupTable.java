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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.RocksDBState;
import org.apache.paimon.lookup.RocksDBStateFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.ExecutorUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.PartialRow;
import org.apache.paimon.utils.TypeUtils;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_ASYNC;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_ASYNC_PENDING_SNAPSHOT_COUNT;

/**
 * 全缓存查询表的查找表。
 * 该类实现了LookupTable接口，用于管理全量缓存和相关的数据查找操作。
 * 它负责从文件存储表中加载数据到缓存中，并提供数据查找和刷新功能。
 */
public abstract class FullCacheLookupTable implements LookupTable {
    // 日志记录器，用于记录类的相关日志
    private static final Logger LOG = LoggerFactory.getLogger(FullCacheLookupTable.class);

    /** 用于同步操作的锁对象 */
    protected final Object lock = new Object();

    /** 上下文信息，包含表的相关配置和投影信息 */
    protected final Context context;

    /** 投影的行类型，定义了表中列的结构和类型 */
    protected final RowType projectedType;

    /** 是否启用异步刷新 */
    protected final boolean refreshAsync;

    /** 用户定义的序列比较器，用于比较动态添加的序列字段 */
    @Nullable protected final FieldsComparator userDefinedSeqComparator;

    /** 动态追加用户定义序列字段的数量 */
    protected final int appendUdsFieldNumber;

    /** 岩石数据库状态工厂，用于管理状态存储 */
    protected RocksDBStateFactory stateFactory;

    /** 刷新执行器，用于异步刷新操作 */
    @Nullable private final ExecutorService refreshExecutor;

    /** 记录缓存中遇到的异常 */
    private final AtomicReference<Exception> cachedException;

    /** 最大允许挂起的快照数量 */
    private final int maxPendingSnapshotCount;

    /** 文件存储表实例，用于与文件存储系统交互 */
    private final FileStoreTable table;

    /** 刷新任务的未来对象，用于跟踪刷新任务的执行状态 */
    private Future<?> refreshFuture;

    /** 流数据读取器，用于从文件存储表中读取数据 */
    private LookupStreamingReader reader;

    /** 特定分区过滤器，用于过滤特定分区的数据 */
    private Predicate specificPartition;

    /**
     * 构造函数，初始化FullCacheLookupTable对象。
     * <p>
     * 该构造函数主要负责：
     * - 初始化投影类型，根据是否存在主键和动态序列字段动态调整。
     * - 创建用户定义的序列比较器和追加的序列字段数量。
     * - 配置异步刷新选项和执行器。
     * - 初始化其他相关配置和参数。
     *
     * @param context 上下文信息，包含表的配置、投影和过滤条件等。
     */
    public FullCacheLookupTable(Context context) {
        this.table = context.table; // 获取文件存储表实例
        List<String> sequenceFields = new ArrayList<>(); // 初始化序列字段列表

        // 如果表中存在主键，则从核心选项中获取序列字段
        if (table.primaryKeys().size() > 0) {
            sequenceFields = new CoreOptions(table.options()).sequenceField();
        }

        // 根据投影信息生成初始的投影类型
        RowType projectedType = TypeUtils.project(table.rowType(), context.projection);

        // 如果存在序列字段，动态调整投影类型
        if (sequenceFields.size() > 0) {
            RowType.Builder builder = RowType.builder(); // 创建行类型构建器
            // 将原投影类型中的字段添加到构建器中
            projectedType.getFields().forEach(f -> builder.field(f.name(), f.type()));

            // 动态添加用户定义的序列字段（如果投影中未包含）
            RowType rowType = table.rowType(); // 获取表的行类型
            AtomicInteger appendUdsFieldNumber = new AtomicInteger(0); // 记录追加的字段数量

            // 遍历序列字段，检查是否未包含在投影中
            sequenceFields.stream()
                    .filter(projectedType::notContainsField) // 过滤未包含的字段
                    .map(rowType::getField) // 获取字段
                    .forEach(
                            f -> {
                                appendUdsFieldNumber.incrementAndGet(); // 增加追加的字段数量
                                builder.field(f.name(), f.type()); // 将字段添加到构建器
                            });

            projectedType = builder.build(); // 构建最终的投影类型
            context = context.copy(table.rowType().getFieldIndices(projectedType.getFieldNames())); // 更新上下文
            this.userDefinedSeqComparator = UserDefinedSeqComparator.create(projectedType, sequenceFields); // 创建序列比较器
            this.appendUdsFieldNumber = appendUdsFieldNumber.get(); // 记录追加的字段数量
        } else {
            this.userDefinedSeqComparator = null; // 如果无序列字段，置为null
            this.appendUdsFieldNumber = 0; // 追加字段数量为0
        }

        this.context = context; // 保存上下文信息

        // 获取表的配置选项
        Options options = Options.fromMap(context.table.options());
        this.projectedType = projectedType; // 设置投影类型
        this.refreshAsync = options.get(LOOKUP_REFRESH_ASYNC); // 是否启用异步刷新

        // 如果启用异步刷新，创建单线程执行器
        this.refreshExecutor =
                this.refreshAsync
                        ? Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                String.format(
                                        "%s-lookup-refresh",
                                        Thread.currentThread().getName())))
                        : null; // 如果未启用，置为null

        this.cachedException = new AtomicReference<>(); // 初始化异常记录器
        this.maxPendingSnapshotCount = options.get(LOOKUP_REFRESH_ASYNC_PENDING_SNAPSHOT_COUNT); // 设置最大挂起快照数量
    }

    /**
     * 设置特定分区过滤器，用于过滤特定分区的数据。
     *
     * @param filter 过滤器，用于过滤数据行。
     */
    @Override
    public void specificPartitionFilter(Predicate filter) {
        this.specificPartition = filter; // 设置特定分区过滤器
    }

    /**
     * 初始化RocksDB状态工厂。
     * 该状态工厂用于管理与RocksDB相关的状态存储和操作。
     *
     * @throws Exception 如果初始化过程中出现异常
     */
    protected void openStateFactory() throws Exception {
        this.stateFactory = new RocksDBStateFactory(
                context.tempPath.toString(), // 数据存储路径
                context.table.coreOptions().toConfiguration(), // 表的配置
                null // 暂无其他配置项
        );
    }

    /**
     * 初始化数据加载，将数据从文件存储表中加载到缓存中。
     * <p>
     * 该方法的主要步骤：
     * - 根据表和上下文的配置，创建流数据读取器。
     * - 使用外部排序缓冲器对数据进行排序。
     * - 将排序后的数据写入批量加载器中。
     * - 完成数据批量加载。
     *
     * @throws Exception 如果加载过程中出现异常
     */
    protected void bootstrap() throws Exception {
        Predicate scanPredicate =
                PredicateBuilder.andNullable(context.tablePredicate, specificPartition); // 创建扫描过滤器

        this.reader = new LookupStreamingReader( // 创建流数据读取器
                context.table, // 文件存储表
                context.projection, // 数据投影
                scanPredicate, // 扫描过滤器
                context.requiredCachedBucketIds // 必须缓存的桶ID列表
        );

        BinaryExternalSortBuffer bulkLoadSorter =
                RocksDBState.createBulkLoadSorter( // 创建外部排序缓冲器
                        IOManager.create(context.tempPath.toString()), // 输入输出管理器
                        context.table.coreOptions() // 表的配置
                );

        Predicate predicate = projectedPredicate(); // 获取投影谓词

        // 读取数据并加载到排序缓冲器
        try (RecordReaderIterator<InternalRow> batch = new RecordReaderIterator<>(reader.nextBatch(true))) {
            while (batch.hasNext()) {
                InternalRow row = batch.next(); // 获取数据行
                if (predicate == null || predicate.test(row)) { // 如果没有谓词或数据行满足谓词
                    bulkLoadSorter.write( // 将数据行写入排序缓冲器
                            GenericRow.of(toKeyBytes(row), toValueBytes(row))
                    );
                }
            }
        }

        MutableObjectIterator<BinaryRow> keyIterator = bulkLoadSorter.sortedIterator(); // 获取排序后的键迭代器
        BinaryRow row = new BinaryRow(2); // 创建二进制行对象
        TableBulkLoader bulkLoader = createBulkLoader(); // 创建批量加载器

        // 遍历排序后的键并写入批量加载器
        try {
            while ((row = keyIterator.next(row)) != null) { // 如果存在下一行
                bulkLoader.write(row.getBinary(0), row.getBinary(1)); // 写入键值对
            }
        } catch (BulkLoader.WriteException e) { // 捕获批量加载错误
            throw new RuntimeException(
                    "批量加载异常，最可能的原因是数据中存在重复，请检查您的查询表",
                    e.getCause()
            );
        }

        bulkLoader.finish(); // 完成批量加载
        bulkLoadSorter.clear(); // 清空排序缓冲器
    }

    /**
     * 刷新缓存中的数据。
     * <p>
     * 该方法负责根据配置的刷新策略（同步或异步）刷新缓存数据。
     * 如果启用异步刷新，将根据最大挂起快照数量限制，控制刷新任务的并发执行。
     *
     * @throws Exception 如果刷新过程中出现异常
     */
    @Override
    public void refresh() throws Exception {
        if (refreshExecutor == null) { // 如果未启用异步刷新
            doRefresh(); // 直接执行刷新操作
            return;
        }

        Long latestSnapshotId = table.snapshotManager().latestSnapshotId(); // 获取最新的快照ID
        Long nextSnapshotId = reader.nextSnapshotId(); // 获取下一个快照ID

        // 检查快照ID差距是否超过最大允许值
        if (latestSnapshotId != null
                && nextSnapshotId != null
                && latestSnapshotId - nextSnapshotId > maxPendingSnapshotCount) {
            LOG.warn(
                    "文件存储表的快照ID 差距超过最大允许值，建议增大查询算子的并行度",
                    latestSnapshotId, nextSnapshotId, maxPendingSnapshotCount);
            if (refreshFuture != null) {
                // 等待之前的刷新任务完成
                refreshFuture.get();
            }
            doRefresh(); // 执行刷新操作
        } else {
            Future<?> currentFuture = null;
            try {
                currentFuture = refreshExecutor.submit(() -> { // 提交刷新任务到执行器
                    try {
                        doRefresh(); // 执行刷新操作
                    } catch (Exception e) {
                        LOG.error("刷新查询表 异常", context.table.name(), e); // 记录错误日志
                        cachedException.set(e); // 更新异常信息
                    }
                });
            } catch (RejectedExecutionException e) {
                LOG.warn("添加刷新任务失败", context.table.name(), e); // 记录警告日志
            }
            if (currentFuture != null) {
                refreshFuture = currentFuture; // 更新刷新任务的未来对象
            }
        }
    }

    /**
     * 执行实际的刷新操作。
     * <p>
     * 该方法是一个无限循环，不断读取数据并执行刷新操作。
     * 它会从流数据读取器中获取下一批数据，并调用{@link#refresh(Iterator)}方法进行刷新。
     */
    private void doRefresh() throws Exception {
        while (true) { // 无限循环
            try (RecordReaderIterator<InternalRow> batch = new RecordReaderIterator<>(reader.nextBatch(false))) { // 获取下一批数据
                if (!batch.hasNext()) { // 如果没有更多数据
                    return;
                }
                refresh(batch); // 执行刷新操作
            }
        }
    }

    /**
     * 根据给定的键获取内部行数据。
     * <p>
     * 该方法是最终的获取方法，会根据是否启用异步刷新决定是否使用同步锁。
     * 如果追加了用户定义的序列字段，会动态丢弃这些字段。
     *
     * @param key 查询键，用于查找数据
     * @return 匹配的数据行列表
     * @throws IOException 如果获取过程中出现IO异常
     */
    @Override
    public final List<InternalRow> get(InternalRow key) throws IOException {
        List<InternalRow> values;

        // 如果启用异步刷新，同步访问
        if (refreshAsync) {
            synchronized (lock) {
                values = innerGet(key); // 调用子类的实现
            }
        } else {
            values = innerGet(key); // 直接调用子类的实现
        }

        // 如果追加了用户定义的序列字段，丢弃这些字段
        if (appendUdsFieldNumber == 0) {
            return values; // 直接返回结果
        }

        List<InternalRow> dropSequence = new ArrayList<>(values.size()); // 创建动态序列字段丢弃列表
        for (InternalRow matchedRow : values) {
            dropSequence.add(
                    new PartialRow(matchedRow.getFieldCount() - appendUdsFieldNumber, matchedRow)
            ); // 创建部分行对象，丢弃动态追加的字段
        }
        return dropSequence;
    }

    /**
     * 刷新数据行。
     * <p>
     * 该方法会遍历输入的记录迭代器，并调用{@link#refreshRow(InternalRow, Predicate)}方法刷新每一行数据。
     *
     * @param input 数据行记录迭代器
     * @throws IOException 如果刷新过程中出现IO异常
     */
    public void refresh(Iterator<InternalRow> input) throws IOException {
        Predicate predicate = projectedPredicate(); // 获取投影谓词

        while (input.hasNext()) { // 遍历记录
            InternalRow row = input.next(); // 获取数据行
            if (refreshAsync) {
                synchronized (lock) { // 如果启用异步刷新，同步访问
                    refreshRow(row, predicate); // 调用子类的实现
                }
            } else {
                refreshRow(row, predicate); // 直接调用子类的实现
            }
        }
    }

    public abstract List<InternalRow> innerGet(InternalRow key) throws IOException;

    protected abstract void refreshRow(InternalRow row, Predicate predicate) throws IOException;


    /**
     * 获取投影谓词。
     * <p>
     * 该方法返回current projection predicate，用于过滤投影后的数据行。
     *
     * @return current projection predicate 或 null
     */
    @Nullable
    public Predicate projectedPredicate() {
        return context.projectedPredicate; // 返回上下文中的投影谓词
    }

    /**
     * 将InternalRow对象转换为键的字节数组。
     * <p>
     * 该方法是抽象方法，需要由子类实现，用于生成键的字节数组。
     *
     * @param row 数据行对象
     * @return 键的字节数组
     * @throws IOException 如果转换过程中出现异常
     */
    public abstract byte[] toKeyBytes(InternalRow row) throws IOException;

    /**
     * 将InternalRow对象转换为值的字节数组。
     * <p>
     * 该方法是抽象方法，需要由子类实现，用于生成值的字节数组。
     *
     * @param row 数据行对象
     * @return 值的字节数组
     * @throws IOException 如果转换过程中出现异常
     */
    public abstract byte[] toValueBytes(InternalRow row) throws IOException;

    /**
     * 创建一个TableBulkLoader对象。
     * <p>
     * 该方法是抽象方法，需要由子类实现，用于创建批量加载器。
     *
     * @return TableBulkLoader对象
     */
    public abstract TableBulkLoader createBulkLoader();

    /**
     * 关闭资源。
     * <p>
     * 该方法会优雅地关闭异步刷新执行器，关闭RocksDB状态工厂，并删除临时路径。
     *
     * @throws IOException 如果关闭过程中出现IO异常
     */
    @Override
    public void close() throws IOException {
        try {
            if (refreshExecutor != null) { // 如果刷新执行器不为空
                ExecutorUtils.gracefulShutdown(1L, TimeUnit.MINUTES, refreshExecutor); // 优雅地关闭执行器
            }
        } finally {
            stateFactory.close(); // 关闭RocksDB状态工厂
            FileIOUtils.deleteDirectory(context.tempPath); // 删除临时路径
        }
    }

    /**
     * TableBulkLoader接口，用于批量加载数据到表中。
     */
    public interface TableBulkLoader {

        /**
         * 写入键值对到表中。
         *
         * @param key 键字节数组
         * @param value 值字节数组
         * @throws BulkLoader.WriteException 如果写入过程中出现异常
         * @throws IOException 如果写入过程中出现IO异常
         */
        void write(byte[] key, byte[] value) throws BulkLoader.WriteException, IOException;

        /**
         * 完成批量加载。
         *
         * @throws IOException 如果完成过程中出现IO异常
         */
        void finish() throws IOException;
    }

    /**
     * 根据上下文创建FullCacheLookupTable实例。
     * <p>
     * 该工厂方法会根据表的主键情况和上下文中指定的连接键，动态选择不同的实现类。
     *
     * @param context 上下文信息
     * @param lruCacheSize LRU缓存大小
     * @return FullCacheLookupTable实例
     */
    static FullCacheLookupTable create(Context context, long lruCacheSize) {
        List<String> primaryKeys = context.table.primaryKeys(); // 获取表的主键列表

        if (primaryKeys.isEmpty()) { // 如果没有主键
            return new NoPrimaryKeyLookupTable(context, lruCacheSize); // 返回无主键的查找表
        } else {
            // 检查主键是否与连接键匹配
            if (new HashSet<>(primaryKeys).equals(new HashSet<>(context.joinKey))) {
                return new PrimaryKeyLookupTable(context, lruCacheSize, context.joinKey); // 返回主键查找表
            } else {
                return new SecondaryIndexLookupTable(context, lruCacheSize); // 返回二级索引查找表
            }
        }
    }

    /**
     * Context类，用于存储LookupTable的上下文信息。
     * <p>
     * 该类包含表、投影、过滤条件、临时路径、连接键和必须缓存的桶ID等信息。
     */
    public static class Context {

        /** 文件存储表实例 */
        public final FileStoreTable table;

        /** 数据投影 */
        public final int[] projection;

        /** 表谓词 */
        @Nullable public final Predicate tablePredicate;

        /** 投影谓词 */
        @Nullable public final Predicate projectedPredicate;

        /** 临时路径 */
        public final File tempPath;

        /** 连接键 */
        public final List<String> joinKey;

        /** 必须缓存的桶ID */
        public final Set<Integer> requiredCachedBucketIds;

        /**
         * 构造函数，初始化Context对象。
         *
         * @param table 文件存储表
         * @param projection 数据投影
         * @param tablePredicate 表谓词
         * @param projectedPredicate 投影谓词
         * @param tempPath 临时路径
         * @param joinKey 连接键
         * @param requiredCachedBucketIds 必须缓存的桶ID
         */
        public Context(
                FileStoreTable table,
                int[] projection,
                @Nullable Predicate tablePredicate,
                @Nullable Predicate projectedPredicate,
                File tempPath,
                List<String> joinKey,
                @Nullable Set<Integer> requiredCachedBucketIds) {
            this.table = table;
            this.projection = projection;
            this.tablePredicate = tablePredicate;
            this.projectedPredicate = projectedPredicate;
            this.tempPath = tempPath;
            this.joinKey = joinKey;
            this.requiredCachedBucketIds = requiredCachedBucketIds;
        }

        /**
         * 创建一个新的Context对象，使用新的投影。
         *
         * @param newProjection 新的投影
         * @return 新的Context对象
         */
        public Context copy(int[] newProjection) {
            return new Context(
                    table,
                    newProjection,
                    tablePredicate,
                    projectedPredicate,
                    tempPath,
                    joinKey,
                    requiredCachedBucketIds
            );
        }
    }
}
