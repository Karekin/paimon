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

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendOnlyWriter;
import org.apache.paimon.append.BucketedAppendCompactManager;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOExceptionSupplier;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * AppendOnlyFileStore 的文件存储写入器。
 * 这个类负责处理对 AppendOnlyFileStore 的写入操作，包括文件的创建、数据写入、合并等。
 * @author snowblink123
 * @create 2023-09-18 0:11
 */
public class AppendOnlyFileStoreWrite extends MemoryFileStoreWrite<InternalRow>
        implements BundleFileStoreWriter {

    private static final Logger LOG = LoggerFactory.getLogger(AppendOnlyFileStoreWrite.class);

    // 文件 I/O 操作工具
    private final FileIO fileIO;

    // 原始文件切片读取工具
    private final RawFileSplitRead read;

    // 数据 Schema 的 ID
    private final long schemaId;

    // Row 的类型结构
    private final RowType rowType;

    // 文件存储的文件格式
    private final FileFormat fileFormat;

    // 文件存储路径的生成工厂
    private final FileStorePathFactory pathFactory;

    // 文件的目标大小
    private final long targetFileSize;

    // 合并操作的最小文件数量
    private final int compactionMinFileNum;

    // 合并操作的最大文件数量
    private final int compactionMaxFileNum;

    // 提交时是否强制执行合并
    private final boolean commitForceCompact;

    // 文件的压缩格式
    private final String fileCompression;

    // 内存溢写时的压缩选项
    private final CompressOptions spillCompression;

    // 是否使用写入缓冲区
    private final boolean useWriteBuffer;

    // 是否允许内存溢写到磁盘
    private final boolean spillable;

    // 内存溢写到磁盘的最大磁盘空间
    private final MemorySize maxDiskSize;

    // 统计信息收集器的工厂
    private final SimpleColStatsCollector.Factory[] statsCollectors;

    // 文件索引的选项
    private final FileIndexOptions fileIndexOptions;

    // 桶模式
    private final BucketMode bucketMode;

    // 是否强制写入缓冲区溢写
    private boolean forceBufferSpill = false;

    // 是否跳过合并操作
    private final boolean skipCompaction;

    public AppendOnlyFileStoreWrite(
            FileIO fileIO,
            RawFileSplitRead read,
            long schemaId,
            String commitUser,
            RowType rowType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            BucketMode bucketMode,
            @Nullable DeletionVectorsMaintainer.Factory dvMaintainerFactory,
            String tableName) {
        // 初始化父类
        super(commitUser, snapshotManager, scan, options, null, dvMaintainerFactory, tableName);
        this.fileIO = fileIO;
        this.read = read;
        this.schemaId = schemaId;
        this.rowType = rowType;
        this.fileFormat = options.fileFormat();
        this.pathFactory = pathFactory;
        this.bucketMode = bucketMode;
        this.targetFileSize = options.targetFileSize(false);
        this.compactionMinFileNum = options.compactionMinFileNum();
        this.compactionMaxFileNum = options.compactionMaxFileNum().orElse(5);
        this.commitForceCompact = options.commitForceCompact();
        // 如果是 Unaware 模式，跳过合并操作
        if (bucketMode == BucketMode.BUCKET_UNAWARE) {
            super.withIgnorePreviousFiles(true);
            this.skipCompaction = true;
        } else {
            this.skipCompaction = options.writeOnly();
        }
        this.fileCompression = options.fileCompression();
        this.spillCompression = options.spillCompressOptions();
        this.useWriteBuffer = options.useWriteBufferForAppend();
        this.spillable = options.writeBufferSpillable(fileIO.isObjectStore(), isStreamingMode);
        this.maxDiskSize = options.writeBufferSpillDiskSize();
        this.statsCollectors =
                StatsCollectorFactories.createStatsFactories(options, rowType.getFieldNames());
        this.fileIndexOptions = options.indexColumnsOptions();
    }

    /**
     * 创建 RecordWriter 用于写入数据。
     * 这个方法会根据是否需要合并操作来创建不同的写入器。
     *
     * @param snapshotId           快照 ID
     * @param partition            数据分区
     * @param bucket               桶 ID
     * @param restoredFiles        恢复的文件元数据列表
     * @param restoredMaxSeqNumber 恢复的最大序列号
     * @param restoreIncrement     恢复的提交增量
     * @param compactExecutor      合并线程池
     * @param dvMaintainer         删除向量维护者
     * @return 记录写入器
     */
    @Override
    protected RecordWriter<InternalRow> createWriter(
            @Nullable Long snapshotId,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable DeletionVectorsMaintainer dvMaintainer) {
        // 默认使用空操作合并管理器
        CompactManager compactManager = new NoopCompactManager();

        // 如果不跳过合并，则创建合并管理器
        if (!skipCompaction) {
            // 使用删除向量维护者创建删除向量工厂
            Function<String, DeletionVector> dvFactory =
                    dvMaintainer != null
                            ? f -> dvMaintainer.deletionVectorOf(f).orElse(null)
                            : null;
            compactManager =
                    new BucketedAppendCompactManager(
                            compactExecutor,
                            restoredFiles,
                            dvMaintainer,
                            compactionMinFileNum,
                            compactionMaxFileNum,
                            targetFileSize,
                            files -> compactRewrite(partition, bucket, dvFactory, files),
                            compactionMetrics == null
                                    ? null
                                    : compactionMetrics.createReporter(partition, bucket));
        }

        // 创建 AppendOnlyWriter 用于写入数据
        return new AppendOnlyWriter(
                fileIO,
                ioManager,
                schemaId,
                fileFormat,
                targetFileSize,
                rowType,
                restoredMaxSeqNumber,
                compactManager,
                files -> createFilesIterator(partition, bucket, files, null),
                commitForceCompact,
                pathFactory.createDataFilePathFactory(partition, bucket),
                restoreIncrement,
                useWriteBuffer || forceBufferSpill,
                spillable || forceBufferSpill,
                fileCompression,
                spillCompression,
                statsCollectors,
                maxDiskSize,
                fileIndexOptions,
                options.asyncFileWrite());
    }

    /**
     * 执行文件合并操作。
     * 这个方法会重写文件并返回合并后的文件元数据列表。
     *
     * @param partition 数据分区
     * @param bucket    桶 ID
     * @param dvFactory 删除向量工厂
     * @param toCompact 需要合并的文件元数据列表
     * @return 合并后的文件元数据列表
     * @throws Exception 如果合并过程发生异常
     */
    public List<DataFileMeta> compactRewrite(
            BinaryRow partition,
            int bucket,
            @Nullable Function<String, DeletionVector> dvFactory,
            List<DataFileMeta> toCompact)
            throws Exception {
        if (toCompact.isEmpty()) {
            return Collections.emptyList();
        }
        Exception collectedExceptions = null;
        RowDataRollingFileWriter rewriter =
                createRollingFileWriter(
                        partition,
                        bucket,
                        new LongCounter(toCompact.get(0).minSequenceNumber()),
                        FileSource.COMPACT);

        List<IOExceptionSupplier<DeletionVector>> dvFactories = null;
        if (dvFactory != null) {
            // 创建删除向量工厂列表
            dvFactories = new ArrayList<>(toCompact.size());
            for (DataFileMeta file : toCompact) {
                dvFactories.add(() -> dvFactory.apply(file.fileName()));
            }
        }

        try {
            // 写入合并后的内容
            rewriter.write(createFilesIterator(partition, bucket, toCompact, dvFactories));
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                // 关闭写入器
                rewriter.close();
            } catch (Exception e) {
                // 收集异常
                collectedExceptions =
                        ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }

        if (collectedExceptions != null) {
            throw collectedExceptions;
        }

        return rewriter.result();
    }

    /**
     * 创建一个 RowDataRollingFileWriter，用于写入滚动文件。
     *
     * @param partition    数据分区
     * @param bucket       桶 ID
     * @param seqNumCounter 序列号计数器
     * @param fileSource   文件来源
     * @return RowDataRollingFileWriter
     */
    private RowDataRollingFileWriter createRollingFileWriter(
            BinaryRow partition, int bucket, LongCounter seqNumCounter, FileSource fileSource) {
        return new RowDataRollingFileWriter(
                fileIO,
                schemaId,
                fileFormat,
                targetFileSize,
                rowType,
                pathFactory.createDataFilePathFactory(partition, bucket),
                seqNumCounter,
                fileCompression,
                statsCollectors,
                fileIndexOptions,
                fileSource,
                options.asyncFileWrite());
    }

    /**
     * 创建一个 RecordReaderIterator，用于读取文件中的记录。
     *
     * @param partition 数据分区
     * @param bucket    桶 ID
     * @param files     文件元数据列表
     * @param dvFactories 删除向量工厂列表
     * @return RecordReaderIterator
     * @throws IOException 如果读取文件时发生 I/O 异常
     */
    private RecordReaderIterator<InternalRow> createFilesIterator(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<IOExceptionSupplier<DeletionVector>> dvFactories)
            throws IOException {
        return new RecordReaderIterator<>(
                read.createReader(partition, bucket, files, dvFactories));
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePrevious) {
        // 如果是 Unaware 模式，则需要忽略之前的文件
        super.withIgnorePreviousFiles(ignorePrevious || bucketMode == BucketMode.BUCKET_UNAWARE);
    }

    @Override
    protected void forceBufferSpill() throws Exception {
        if (ioManager == null) {
            return;
        }
        // 强制内存溢写
        forceBufferSpill = true;
        LOG.info(
                "Force buffer spill for append-only file store write, writer number is: {}",
                writers.size());
        for (Map<Integer, WriterContainer<InternalRow>> bucketWriters : writers.values()) {
            for (WriterContainer<InternalRow> writerContainer : bucketWriters.values()) {
                // 将写入器转换为缓冲写入器
                ((AppendOnlyWriter) writerContainer.writer).toBufferedWriter();
            }
        }
    }

    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle)
            throws Exception {
        // 获取写入器容器
        WriterContainer<InternalRow> container = getWriterWrapper(partition, bucket);
        // 写入记录包
        ((AppendOnlyWriter) container.writer).writeBundle(bundle);
    }
}
