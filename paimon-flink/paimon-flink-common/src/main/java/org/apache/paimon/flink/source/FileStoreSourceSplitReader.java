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

package org.apache.paimon.flink.source;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.utils.Pool;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 文件存储源的 {@link SplitReader} 实现。
 */
public class FileStoreSourceSplitReader
        implements SplitReader<BulkFormat.RecordIterator<RowData>, FileStoreSourceSplit> {

    // 日志记录器
    private static final Logger LOG = LoggerFactory.getLogger(FileStoreSourceSplitReader.class);

    // 表读取对象，用于读取数据
    private final TableRead tableRead;

    // 记录限制器，用于控制读取的记录数
    @Nullable private final RecordLimiter limiter;

    // 分片队列，存储待处理的分片
    private final Queue<FileStoreSourceSplit> splits;

    // 对象池，用于管理 FileStoreRecordIterator
    private final Pool<FileStoreRecordIterator> pool;

    // 当前分片的延迟读取器
    @Nullable private LazyRecordReader currentReader;

    // 当前分片的 ID
    @Nullable private String currentSplitId;

    // 当前读取的记录数
    private long currentNumRead;

    // 当前分片的第一批记录
    private RecordIterator<InternalRow> currentFirstBatch;

    // 标记是否暂停读取
    private boolean paused;

    // 用于唤醒的原子标志
    private final AtomicBoolean wakeup;

    // 文件存储源的读取指标
    private final FileStoreSourceReaderMetrics metrics;

    /**
     * 构造函数。
     *
     * @param tableRead 表读取对象。
     * @param limiter 记录限制器。
     * @param metrics 文件存储源读取的指标对象。
     */
    public FileStoreSourceSplitReader(
            TableRead tableRead,
            @Nullable RecordLimiter limiter,
            FileStoreSourceReaderMetrics metrics) {
        this.tableRead = tableRead;
        this.limiter = limiter;
        this.splits = new LinkedList<>();
        this.pool = new Pool<>(1); // 对象池大小为 1
        this.pool.add(new FileStoreRecordIterator());
        this.paused = false;
        this.metrics = metrics;
        this.wakeup = new AtomicBoolean(false);
    }

    /**
     * 从当前分片中读取数据。如果读取器处于暂停状态，返回空记录。
     *
     * @return 包含分片记录的对象。
     * @throws IOException 如果发生 I/O 错误。
     */
    @Override
    public RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>> fetch() throws IOException {
        if (paused) {
            return new EmptyRecordsWithSplitIds<>();
        }

        checkSplitOrStartNext();

        // 从对象池中获取 FileStoreRecordIterator
        FileStoreRecordIterator iterator = poll();
        if (iterator == null) {
            LOG.info("由于唤醒操作，跳过等待对象池。");
            return new EmptyRecordsWithSplitIds<>();
        }

        // 获取下一批记录
        RecordIterator<InternalRow> nextBatch;
        if (currentFirstBatch != null) {
            nextBatch = currentFirstBatch;
            currentFirstBatch = null;
        } else {
            nextBatch =
                    reachLimit()
                            ? null
                            : Objects.requireNonNull(currentReader).recordReader().readBatch();
        }

        // 如果没有更多记录，完成分片处理
        if (nextBatch == null) {
            pool.recycler().recycle(iterator);
            return finishSplit();
        }

        // 返回当前分片的记录
        return FlinkRecordsWithSplitIds.forRecords(currentSplitId, iterator.replace(nextBatch));
    }

    /**
     * 检查是否达到读取限制。
     *
     * @return 如果达到限制返回 true，否则返回 false。
     */
    private boolean reachLimit() {
        return limiter != null && limiter.reachLimit();
    }

    /**
     * 从对象池中获取 FileStoreRecordIterator。如果被唤醒，则退出等待。
     *
     * @return FileStoreRecordIterator 对象或 null。
     * @throws IOException 如果线程中断。
     */
    @Nullable
    private FileStoreRecordIterator poll() throws IOException {
        FileStoreRecordIterator iterator = null;
        while (iterator == null && !wakeup.get()) {
            try {
                iterator = this.pool.pollEntry(Duration.ofSeconds(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("线程被中断", e);
            }
        }
        if (wakeup.get()) {
            wakeup.compareAndSet(true, false);
        }
        return iterator;
    }

    /**
     * 处理分片更改，例如添加新的分片。
     *
     * @param splitsChange 分片更改对象。
     */
    @Override
    public void handleSplitsChanges(SplitsChange<FileStoreSourceSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "不支持的分片更改类型：%s。",
                            splitsChange.getClass()));
        }

        splits.addAll(splitsChange.splits());
    }

    /**
     * 暂停或恢复指定分片的读取操作。
     *
     * @param splitsToPause 要暂停的分片列表。
     * @param splitsToResume 要恢复的分片列表。
     */
    public void pauseOrResumeSplits(
            Collection<FileStoreSourceSplit> splitsToPause,
            Collection<FileStoreSourceSplit> splitsToResume) {
        for (FileStoreSourceSplit split : splitsToPause) {
            if (split.splitId().equals(currentSplitId)) {
                paused = true;
                break;
            }
        }

        for (FileStoreSourceSplit split : splitsToResume) {
            if (split.splitId().equals(currentSplitId)) {
                paused = false;
                break;
            }
        }
    }

    /**
     * 唤醒分片读取器。
     */
    @Override
    public void wakeUp() {
        wakeup.compareAndSet(false, true);
        LOG.info("唤醒分片读取器。");
    }

    /**
     * 关闭分片读取器，释放资源。
     *
     * @throws Exception 如果关闭失败。
     */
    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            if (currentReader.lazyRecordReader != null) {
                currentReader.lazyRecordReader.close();
            }
        }
    }

    /**
     * 检查当前分片是否为空，如果为空则启动下一个分片。
     *
     * @throws IOException 如果没有分片可用。
     */
    private void checkSplitOrStartNext() throws IOException {
        if (currentReader != null) {
            return;
        }

        final FileStoreSourceSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("没有更多分片可供读取。");
        }

        // 更新指标信息
        if (nextSplit.split() instanceof DataSplit) {
            long eventTime =
                    ((DataSplit) nextSplit.split())
                            .earliestFileCreationEpochMillis()
                            .orElse(FileStoreSourceReaderMetrics.UNDEFINED);
            metrics.recordSnapshotUpdate(eventTime);
        }

        currentSplitId = nextSplit.splitId();
        currentReader = new LazyRecordReader(nextSplit.split());
        currentNumRead = nextSplit.recordsToSkip();
        if (limiter != null) {
            limiter.add(currentNumRead);
        }
        if (currentNumRead > 0) {
            seek(currentNumRead);
        }
    }

    /**
     * 跳过指定数量的记录，以定位到目标记录位置。
     *
     * @param toSkip 要跳过的记录数。
     * @throws IOException 如果发生 I/O 错误。
     */
    private void seek(long toSkip) throws IOException {
        while (true) {
            // 从当前记录读取器中读取下一批记录
            RecordIterator<InternalRow> nextBatch =
                    Objects.requireNonNull(currentReader).recordReader().readBatch();

            // 如果没有更多记录可以读取，抛出异常
            if (nextBatch == null) {
                throw new RuntimeException(
                        String.format(
                                "skip(%s) 超过了剩余记录的数量。", toSkip));
            }

            // 遍历当前批次的记录，逐条跳过
            while (toSkip > 0 && nextBatch.next() != null) {
                toSkip--;
            }

            // 如果已经跳过了指定数量的记录，保存当前批次并退出
            if (toSkip == 0) {
                currentFirstBatch = nextBatch;
                return;
            }

            // 如果当前批次处理完但未跳过完目标数量，释放当前批次资源，继续读取下一批
            nextBatch.releaseBatch();
        }
    }

    /**
     * 完成当前分片的处理，并释放相关资源。
     *
     * @return 包含已完成分片的记录对象。
     * @throws IOException 如果发生 I/O 错误。
     */
    private FlinkRecordsWithSplitIds finishSplit() throws IOException {
        if (currentReader != null) {
            if (currentReader.lazyRecordReader != null) {
                currentReader.lazyRecordReader.close(); // 关闭当前分片的延迟读取器
            }
            currentReader = null; // 清除当前读取器
        }

        // 创建包含已完成分片 ID 的记录对象
        final FlinkRecordsWithSplitIds finishRecords =
                FlinkRecordsWithSplitIds.finishedSplit(currentSplitId);
        currentSplitId = null; // 清除当前分片 ID
        return finishRecords;
    }

    /**
     * 文件存储记录迭代器的实现，用于逐条读取分片中的记录。
     */
    private class FileStoreRecordIterator implements BulkFormat.RecordIterator<RowData> {

        private RecordIterator<InternalRow> iterator;

        private final MutableRecordAndPosition<RowData> recordAndPosition =
                new MutableRecordAndPosition<>();

        /**
         * 替换当前迭代器为新的记录迭代器。
         *
         * @param iterator 新的记录迭代器。
         * @return 当前 FileStoreRecordIterator 实例。
         */
        public FileStoreRecordIterator replace(RecordIterator<InternalRow> iterator) {
            this.iterator = iterator;
            this.recordAndPosition.set(null, RecordAndPosition.NO_OFFSET, currentNumRead);
            return this;
        }

        /**
         * 获取下一个记录及其位置信息。
         *
         * @return 包含记录及位置信息的对象；如果达到限制或没有更多记录，返回 null。
         */
        @Nullable
        @Override
        public RecordAndPosition<RowData> next() {
            if (reachLimit()) {
                return null;
            }
            InternalRow row;
            try {
                row = iterator.next();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (row == null) {
                return null;
            }

            // 设置记录及其位置
            recordAndPosition.setNext(new FlinkRowData(row));
            currentNumRead++;
            if (limiter != null) {
                limiter.increment();
            }
            return recordAndPosition;
        }

        /**
         * 释放当前批次的资源并将迭代器回收到对象池中。
         */
        @Override
        public void releaseBatch() {
            this.iterator.releaseBatch();
            pool.recycler().recycle(this);
        }
    }

    /**
     * 延迟创建 {@link RecordReader}，以提高性能（特别是在有读取限制的情况下）。
     */
    private class LazyRecordReader {

        private final Split split;

        private RecordReader<InternalRow> lazyRecordReader;

        /**
         * 构造函数。
         *
         * @param split 分片对象。
         */
        private LazyRecordReader(Split split) {
            this.split = split;
        }

        /**
         * 获取当前分片的记录读取器。如果尚未创建，则延迟初始化。
         *
         * @return 记录读取器对象。
         * @throws IOException 如果发生 I/O 错误。
         */
        public RecordReader<InternalRow> recordReader() throws IOException {
            if (lazyRecordReader == null) {
                lazyRecordReader = tableRead.createReader(split);
            }
            return lazyRecordReader;
        }
    }

    /**
     * 空的 {@link RecordsWithSplitIds} 实现，用于指示读取器已暂停或被唤醒。
     */
    private static class EmptyRecordsWithSplitIds<T> implements RecordsWithSplitIds<T> {

        /**
         * 获取下一个分片 ID（无分片时返回 null）。
         *
         * @return null。
         */
        @Nullable
        @Override
        public String nextSplit() {
            return null;
        }

        /**
         * 从当前分片获取下一条记录（无记录时返回 null）。
         *
         * @return null。
         */
        @Nullable
        @Override
        public T nextRecordFromSplit() {
            return null;
        }

        /**
         * 获取已完成分片的集合（返回空集合）。
         *
         * @return 空集合。
         */
        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }
    }

}
