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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.LookupLevels.ContainsValueProcessor;
import org.apache.paimon.mergetree.LookupLevels.KeyValueProcessor;
import org.apache.paimon.mergetree.LookupLevels.PositionedKeyValueProcessor;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.compact.CompactStrategy;
import org.apache.paimon.mergetree.compact.ForceUpLevel0Compaction;
import org.apache.paimon.mergetree.compact.FullChangelogMergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter.FirstRowMergeFunctionWrapperFactory;
import org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter.LookupMergeFunctionWrapperFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.MergeTreeCompactManager;
import org.apache.paimon.mergetree.compact.MergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.UniversalCompaction;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.apache.paimon.CoreOptions.ChangelogProducer.FULL_COMPACTION;
import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;

/** {@link FileStoreWrite} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreWrite extends MemoryFileStoreWrite<KeyValue> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueFileStoreWrite.class);

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final KeyValueFileWriterFactory.Builder writerFactoryBuilder;
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    private final Supplier<FieldsComparator> udsComparatorSupplier;
    private final Supplier<RecordEqualiser> valueEqualiserSupplier;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final CoreOptions options;
    private final FileIO fileIO;
    private final RowType keyType;
    private final RowType valueType;
    private final RowType partitionType;
    @Nullable private final RecordLevelExpire recordLevelExpire;
    @Nullable private Cache<String, LookupFile> lookupFileCache;

    public KeyValueFileStoreWrite(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            String commitUser,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            Supplier<Comparator<InternalRow>> keyComparatorSupplier,
            Supplier<FieldsComparator> udsComparatorSupplier,
            Supplier<RecordEqualiser> valueEqualiserSupplier,
            MergeFunctionFactory<KeyValue> mfFactory,
            FileStorePathFactory pathFactory,
            Map<String, FileStorePathFactory> format2PathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            @Nullable IndexMaintainer.Factory<KeyValue> indexFactory,
            @Nullable DeletionVectorsMaintainer.Factory deletionVectorsMaintainerFactory,
            CoreOptions options,
            KeyValueFieldsExtractor extractor,
            String tableName) {
        super(
                commitUser,
                snapshotManager,
                scan,
                options,
                indexFactory,
                deletionVectorsMaintainerFactory,
                tableName);
        this.fileIO = fileIO;
        this.partitionType = partitionType;
        this.keyType = keyType;
        this.valueType = valueType;
        this.udsComparatorSupplier = udsComparatorSupplier;
        this.readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        schemaManager,
                        schema,
                        keyType,
                        valueType,
                        FileFormatDiscover.of(options),
                        pathFactory,
                        extractor,
                        options);
        this.recordLevelExpire = RecordLevelExpire.create(options, valueType);
        this.writerFactoryBuilder =
                KeyValueFileWriterFactory.builder(
                        fileIO,
                        schema.id(),
                        keyType,
                        valueType,
                        options.fileFormat(),
                        format2PathFactory,
                        options.targetFileSize(true));
        this.keyComparatorSupplier = keyComparatorSupplier;
        this.valueEqualiserSupplier = valueEqualiserSupplier;
        this.mfFactory = mfFactory;
        this.options = options;
    }

    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 初始化一个MergeTreeWriter实例
    */
    @Override
    protected MergeTreeWriter createWriter(
            @Nullable Long snapshotId, // 可为空的快照ID
            BinaryRow partition, // 分区信息
            int bucket,// 存储桶编号
            List<DataFileMeta> restoreFiles,// 需要恢复的数据文件元数据列表
            long restoredMaxSeqNumber,// 恢复的最大序列号
            @Nullable CommitIncrement restoreIncrement,// 可为空的恢复增量信息
            ExecutorService compactExecutor, // 用于执行压缩任务的线程池
            @Nullable DeletionVectorsMaintainer dvMaintainer) { // 可为空的删除向量维护器
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Creating merge tree writer for partition {} bucket {} from restored files {}",
                    partition,
                    bucket,
                    restoreFiles);
        }
        // 根据分区、存储桶和选项构建KeyValueFileWriterFactory
        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options);
        // 获取键的比较器
        Comparator<InternalRow> keyComparator = keyComparatorSupplier.get();
        // 根据键比较器、恢复文件和选项中的层数构建Levels对象
        Levels levels = new Levels(keyComparator, restoreFiles, options.numLevels());
        // 初始化通用压缩策略
        UniversalCompaction universalCompaction =
                new UniversalCompaction(
                        options.maxSizeAmplificationPercent(),// 最大尺寸放大百分比
                        options.sortedRunSizeRatio(),// 有序运行尺寸比例
                        options.numSortedRunCompactionTrigger(),// 有序运行压缩触发数
                        options.optimizedCompactionInterval());// 优化压缩间隔
        // 根据是否需要查找来确定压缩策略
        CompactStrategy compactStrategy =
                options.needLookup()
                        ? new ForceUpLevel0Compaction(universalCompaction)// 需要查找时使用强制提升到Level0的压缩策略
                        : universalCompaction;// 否则使用通用压缩策略
        // 创建压缩管理器
        CompactManager compactManager =
                createCompactManager(
                        partition, bucket, compactStrategy, compactExecutor, levels, dvMaintainer);
        // 返回一个新的MergeTreeWriter实例
        return new MergeTreeWriter(
                bufferSpillable(),// 溢出缓冲区
                options.writeBufferSpillDiskSize(),// 写入缓冲区溢出到磁盘的大小
                options.localSortMaxNumFileHandles(),// 本地排序时最大文件句柄数
                options.spillCompressOptions(),// 溢出压缩选项
                ioManager,// IO管理器
                compactManager,// 压缩管理器
                restoredMaxSeqNumber,// 恢复的最大序列号
                keyComparator,// 键比较器
                mfFactory.create(),// 内存表工厂创建的实例
                writerFactory,// 写入工厂
                options.commitForceCompact(),// 提交时是否强制压缩
                options.changelogProducer(),// 日志生产者
                restoreIncrement,// 恢复增量信息
                UserDefinedSeqComparator.create(valueType, options));// 用户定义的序列号比较器
    }

    @VisibleForTesting
    public boolean bufferSpillable() {
        return options.writeBufferSpillable(fileIO.isObjectStore(), isStreamingMode);
    }

    private CompactManager createCompactManager(
            BinaryRow partition,
            int bucket,
            CompactStrategy compactStrategy,
            ExecutorService compactExecutor,
            Levels levels,
            @Nullable DeletionVectorsMaintainer dvMaintainer) {
        if (options.writeOnly()) {
            return new NoopCompactManager();
        } else {
            Comparator<InternalRow> keyComparator = keyComparatorSupplier.get();
            @Nullable FieldsComparator userDefinedSeqComparator = udsComparatorSupplier.get();
            CompactRewriter rewriter =
                    createRewriter(
                            partition,
                            bucket,
                            keyComparator,
                            userDefinedSeqComparator,
                            levels,
                            dvMaintainer);
            return new MergeTreeCompactManager(
                    compactExecutor,
                    levels,
                    compactStrategy,
                    keyComparator,
                    options.compactionFileSize(true),
                    options.numSortedRunStopTrigger(),
                    rewriter,
                    compactionMetrics == null
                            ? null
                            : compactionMetrics.createReporter(partition, bucket),
                    dvMaintainer,
                    options.prepareCommitWaitCompaction());
        }
    }

    private MergeTreeCompactRewriter createRewriter(
            BinaryRow partition,
            int bucket,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            Levels levels,
            @Nullable DeletionVectorsMaintainer dvMaintainer) {
        DeletionVector.Factory dvFactory = DeletionVector.factory(dvMaintainer);
        FileReaderFactory<KeyValue> readerFactory =
                readerFactoryBuilder.build(partition, bucket, dvFactory);
        if (recordLevelExpire != null) {
            readerFactory = recordLevelExpire.wrap(readerFactory);
        }
        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options);
        MergeSorter mergeSorter = new MergeSorter(options, keyType, valueType, ioManager);
        int maxLevel = options.numLevels() - 1;
        MergeEngine mergeEngine = options.mergeEngine();
        ChangelogProducer changelogProducer = options.changelogProducer();
        LookupStrategy lookupStrategy = options.lookupStrategy();
        if (changelogProducer.equals(FULL_COMPACTION)) {
            return new FullChangelogMergeTreeCompactRewriter(
                    maxLevel,
                    mergeEngine,
                    readerFactory,
                    writerFactory,
                    keyComparator,
                    userDefinedSeqComparator,
                    mfFactory,
                    mergeSorter,
                    valueEqualiserSupplier.get(),
                    options.changelogRowDeduplicate());
        } else if (lookupStrategy.needLookup) {
            LookupLevels.ValueProcessor<?> processor;
            LookupMergeTreeCompactRewriter.MergeFunctionWrapperFactory<?> wrapperFactory;
            FileReaderFactory<KeyValue> lookupReaderFactory = readerFactory;
            if (lookupStrategy.isFirstRow) {
                if (options.deletionVectorsEnabled()) {
                    throw new UnsupportedOperationException(
                            "First row merge engine does not need deletion vectors because there is no deletion of old data in this merge engine.");
                }
                lookupReaderFactory =
                        readerFactoryBuilder
                                .copyWithoutProjection()
                                .withValueProjection(new int[0][])
                                .build(partition, bucket, dvFactory);
                processor = new ContainsValueProcessor();
                wrapperFactory = new FirstRowMergeFunctionWrapperFactory();
            } else {
                processor =
                        lookupStrategy.deletionVector
                                ? new PositionedKeyValueProcessor(
                                        valueType,
                                        lookupStrategy.produceChangelog
                                                || mergeEngine != DEDUPLICATE
                                                || !options.sequenceField().isEmpty())
                                : new KeyValueProcessor(valueType);
                wrapperFactory =
                        new LookupMergeFunctionWrapperFactory<>(
                                valueEqualiserSupplier.get(),
                                options.changelogRowDeduplicate(),
                                lookupStrategy,
                                UserDefinedSeqComparator.create(valueType, options));
            }
            return new LookupMergeTreeCompactRewriter(
                    maxLevel,
                    mergeEngine,
                    createLookupLevels(partition, bucket, levels, processor, lookupReaderFactory),
                    readerFactory,
                    writerFactory,
                    keyComparator,
                    userDefinedSeqComparator,
                    mfFactory,
                    mergeSorter,
                    wrapperFactory,
                    lookupStrategy.produceChangelog,
                    dvMaintainer,
                    options);
        } else {
            return new MergeTreeCompactRewriter(
                    readerFactory,
                    writerFactory,
                    keyComparator,
                    userDefinedSeqComparator,
                    mfFactory,
                    mergeSorter);
        }
    }

    private <T> LookupLevels<T> createLookupLevels(
            BinaryRow partition,
            int bucket,
            Levels levels,
            LookupLevels.ValueProcessor<T> valueProcessor,
            FileReaderFactory<KeyValue> readerFactory) {
        if (ioManager == null) {
            throw new RuntimeException(
                    "Can not use lookup, there is no temp disk directory to use.");
        }
        LookupStoreFactory lookupStoreFactory =
                LookupStoreFactory.create(
                        options,
                        cacheManager,
                        new RowCompactedSerializer(keyType).createSliceComparator());
        Options options = this.options.toConfiguration();
        if (lookupFileCache == null) {
            lookupFileCache =
                    LookupFile.createCache(
                            options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                            options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
        }
        return new LookupLevels<>(
                levels,
                keyComparatorSupplier.get(),
                keyType,
                valueProcessor,
                readerFactory::createRecordReader,
                file ->
                        ioManager
                                .createChannel(
                                        localFilePrefix(partitionType, partition, bucket, file))
                                .getPathFile(),
                lookupStoreFactory,
                bfGenerator(options),
                lookupFileCache);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
        }
    }
}
