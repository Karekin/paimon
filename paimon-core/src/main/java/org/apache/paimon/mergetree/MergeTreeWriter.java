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

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MergeTreeWriter 是一个 {@link RecordWriter}，用于写入记录并生成 {@link CompactIncrement}。
 * 该类负责将数据写入缓冲区，并在必要时执行数据合并（compaction）。
 */
public class MergeTreeWriter implements RecordWriter<KeyValue>, MemoryOwner {

    // 是否允许写缓冲区溢出到磁盘
    private final boolean writeBufferSpillable;
    // 磁盘缓冲区的最大使用大小
    private final MemorySize maxDiskSize;
    // 排序时的最大扇出数（控制归并排序的层次）
    private final int sortMaxFan;
    // 排序时的压缩选项
    private final CompressOptions sortCompression;
    // IO管理器，管理数据的输入输出
    private final IOManager ioManager;

    // 键的RowType类型
    private final RowType keyType;
    // 值的RowType类型
    private final RowType valueType;
    // 管理数据合并的组件
    private final CompactManager compactManager;
    // 用于比较键的比较器
    private final Comparator<InternalRow> keyComparator;
    // 合并函数，定义数据合并逻辑
    private final MergeFunction<KeyValue> mergeFunction;
    // 文件写入工厂，创建数据文件写入器
    private final KeyValueFileWriterFactory writerFactory;
    // 是否强制提交时进行合并
    private final boolean commitForceCompact;
    // 变更日志生产者，控制是否记录变更日志
    private final ChangelogProducer changelogProducer;
    // 用户定义的序列比较器，可用于控制排序逻辑（可为空）
    @Nullable private final FieldsComparator userDefinedSeqComparator;

    // 记录新生成的数据文件
    private final LinkedHashSet<DataFileMeta> newFiles;
    // 记录被删除的数据文件
    private final LinkedHashSet<DataFileMeta> deletedFiles;
    // 记录新生成的变更日志文件
    private final LinkedHashSet<DataFileMeta> newFilesChangelog;
    // 记录合并前的文件状态
    private final LinkedHashMap<String, DataFileMeta> compactBefore;
    // 记录合并后的文件状态
    private final LinkedHashSet<DataFileMeta> compactAfter;
    // 记录合并时的变更日志文件
    private final LinkedHashSet<DataFileMeta> compactChangelog;

    // 记录删除文件的合并信息（可为空）
    @Nullable private CompactDeletionFile compactDeletionFile;

    // 用于生成新的序列号
    private long newSequenceNumber;
    // 负责缓存写入数据的缓冲区
    private WriteBuffer writeBuffer;
    // 是否仅支持插入数据（即不支持更新和删除）
    private boolean isInsertOnly;

    /**
     * MergeTreeWriter 构造函数，初始化写入组件和状态。
     *
     * @param writeBufferSpillable 是否允许缓冲区溢出到磁盘
     * @param maxDiskSize 最大磁盘使用大小
     * @param sortMaxFan 归并排序的最大扇出数
     * @param sortCompression 排序压缩选项
     * @param ioManager IO管理器
     * @param compactManager 合并管理器
     * @param maxSequenceNumber 当前的最大序列号
     * @param keyComparator 键比较器
     * @param mergeFunction 数据合并函数
     * @param writerFactory 文件写入工厂
     * @param commitForceCompact 是否强制提交时合并
     * @param changelogProducer 变更日志生产者
     * @param increment 先前的提交增量（用于恢复状态）
     * @param userDefinedSeqComparator 用户自定义序列比较器（可为空）
     */
    public MergeTreeWriter(
            boolean writeBufferSpillable,
            MemorySize maxDiskSize,
            int sortMaxFan,
            CompressOptions sortCompression,
            IOManager ioManager,
            CompactManager compactManager,
            long maxSequenceNumber,
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            KeyValueFileWriterFactory writerFactory,
            boolean commitForceCompact,
            ChangelogProducer changelogProducer,
            @Nullable CommitIncrement increment,
            @Nullable FieldsComparator userDefinedSeqComparator) {

        // 初始化构造参数
        this.writeBufferSpillable = writeBufferSpillable;
        this.maxDiskSize = maxDiskSize;
        this.sortMaxFan = sortMaxFan;
        this.sortCompression = sortCompression;
        this.ioManager = ioManager;
        this.keyType = writerFactory.keyType();
        this.valueType = writerFactory.valueType();
        this.compactManager = compactManager;
        this.newSequenceNumber = maxSequenceNumber + 1; // 设置新的序列号
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.writerFactory = writerFactory;
        this.commitForceCompact = commitForceCompact;
        this.changelogProducer = changelogProducer;
        this.userDefinedSeqComparator = userDefinedSeqComparator;

        // 初始化文件集合
        this.newFiles = new LinkedHashSet<>();
        this.deletedFiles = new LinkedHashSet<>();
        this.newFilesChangelog = new LinkedHashSet<>();
        this.compactBefore = new LinkedHashMap<>();
        this.compactAfter = new LinkedHashSet<>();
        this.compactChangelog = new LinkedHashSet<>();

        // 如果有先前的增量数据，则恢复文件状态
        if (increment != null) {
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            deletedFiles.addAll(increment.newFilesIncrement().deletedFiles());
            newFilesChangelog.addAll(increment.newFilesIncrement().changelogFiles());

            // 记录合并前和合并后的文件状态
            increment.compactIncrement().compactBefore().forEach(f -> compactBefore.put(f.fileName(), f));
            compactAfter.addAll(increment.compactIncrement().compactAfter());
            compactChangelog.addAll(increment.compactIncrement().changelogFiles());

            // 更新合并删除文件信息
            updateCompactDeletionFile(increment.compactDeletionFile());
        }
    }

    /**
     * 生成新的序列号，每次调用时递增。
     *
     * @return 新的序列号
     */
    private long newSequenceNumber() {
        return newSequenceNumber++;
    }

    /**
     * 获取 {@link CompactManager} 实例，该实例负责管理数据合并操作。
     *
     * @return 数据合并管理器
     */
    @VisibleForTesting
    CompactManager compactManager() {
        return compactManager;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 设置内存池，用于分配数据缓冲区，支持排序和存储数据。
     *
     * @param memoryPool 内存池实例
     */
    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        // 使用指定的内存池和参数，创建一个新的排序缓冲区用于写入数据
        this.writeBuffer =
                new SortBufferWriteBuffer(
                        keyType,                 // 键的类型
                        valueType,               // 值的类型
                        userDefinedSeqComparator, // 用户定义的序列比较器，用于排序
                        memoryPool,              // 内存池，用于分配内存
                        writeBufferSpillable,    // 是否允许写入缓冲区溢出到磁盘
                        maxDiskSize,             // 最大磁盘使用大小
                        sortMaxFan,              // 排序时的最大扇出数
                        sortCompression,         // 排序压缩方式
                        ioManager);              // IO管理器，用于处理磁盘IO操作
    }

    /**
     * 写入一条 {@link KeyValue} 记录。
     *
     * @param kv 要写入的键值对
     * @throws Exception 写入失败时抛出异常
     */
    @Override
    public void write(KeyValue kv) throws Exception {
        // 生成新的序列号
        long sequenceNumber = newSequenceNumber();
        // 尝试将记录写入写缓冲区
        boolean success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());

        // 如果写入失败（缓冲区已满），先刷新缓冲区，然后再尝试写入
        if (!success) {
            flushWriteBuffer(false, false);
            success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());

            // 若仍然无法写入，则抛出异常
            if (!success) {
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    /**
     * 触发数据合并（Compaction）。
     *
     * @param fullCompaction 是否执行完整合并
     * @throws Exception 合并失败时抛出异常
     */
    @Override
    public void compact(boolean fullCompaction) throws Exception {
        flushWriteBuffer(true, fullCompaction);
    }

    /**
     * 添加新的数据文件到 {@link CompactManager}，用于后续合并。
     *
     * @param files 新的数据文件列表
     */
    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        files.forEach(compactManager::addNewFile);
    }

    /**
     * 获取当前存储的所有数据文件。
     *
     * @return 数据文件集合
     */
    @Override
    public Collection<DataFileMeta> dataFiles() {
        return compactManager.allFiles();
    }

    /**
     * 获取当前写入的最大序列号。
     *
     * @return 最大序列号
     */
    @Override
    public long maxSequenceNumber() {
        return newSequenceNumber - 1;
    }

    /**
     * 获取当前写缓冲区的内存占用情况。
     *
     * @return 内存占用大小
     */
    @Override
    public long memoryOccupancy() {
        return writeBuffer.memoryOccupancy();
    }

    /**
     * 释放部分内存，如果释放失败，则触发缓冲区刷新操作。
     *
     * @throws Exception 如果刷新失败，则抛出异常
     */
    @Override
    public void flushMemory() throws Exception {
        boolean success = writeBuffer.flushMemory();
        if (!success) {
            flushWriteBuffer(false, false);
        }
    }

    /**
     * 刷新写缓冲区，将缓冲区中的数据写入文件，并触发合并操作。
     *
     * @param waitForLatestCompaction 是否等待最新的合并任务完成
     * @param forcedFullCompaction 是否强制执行完整合并
     * @throws Exception 发生异常时抛出
     */
    private void flushWriteBuffer(boolean waitForLatestCompaction, boolean forcedFullCompaction)
            throws Exception {
        if (writeBuffer.size() > 0) {
            // 如果合并管理器需要等待最新的合并任务完成，则设置 waitForLatestCompaction 为 true
            if (compactManager.shouldWaitForLatestCompaction()) {
                waitForLatestCompaction = true;
            }

            // 创建变更日志文件写入器（如果启用了变更日志功能）
            final RollingFileWriter<KeyValue, DataFileMeta> changelogWriter =
                    (changelogProducer == ChangelogProducer.INPUT && !isInsertOnly)
                            ? writerFactory.createRollingChangelogFileWriter(0)
                            : null;

            // 创建主数据文件写入器
            final RollingFileWriter<KeyValue, DataFileMeta> dataWriter =
                    writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);

            try {
                // 遍历写缓冲区中的数据，执行合并，并写入文件
                writeBuffer.forEach(
                        keyComparator,
                        mergeFunction,
                        changelogWriter == null ? null : changelogWriter::write,
                        dataWriter::write);
            } finally {
                // 清空写缓冲区
                writeBuffer.clear();

                // 关闭变更日志写入器
                if (changelogWriter != null) {
                    changelogWriter.close();
                }
                // 关闭数据写入器
                dataWriter.close();
            }

            // 获取数据写入的文件元数据
            List<DataFileMeta> dataMetas = dataWriter.result();

            // 如果启用了变更日志，记录日志文件
            if (changelogWriter != null) {
                newFilesChangelog.addAll(changelogWriter.result());
            } else if (changelogProducer == ChangelogProducer.INPUT && isInsertOnly) {
                // 如果仅插入模式下启用了变更日志，则复制数据文件作为变更日志
                List<DataFileMeta> changelogMetas = new ArrayList<>();
                for (DataFileMeta dataMeta : dataMetas) {
                    DataFileMeta changelogMeta =
                            dataMeta.rename(writerFactory.newChangelogPath(0).getName());
                    writerFactory.copyFile(dataMeta.fileName(), changelogMeta.fileName(), 0);
                    changelogMetas.add(changelogMeta);
                }
                newFilesChangelog.addAll(changelogMetas);
            }

            // 记录新生成的数据文件
            for (DataFileMeta dataMeta : dataMetas) {
                newFiles.add(dataMeta);
                compactManager.addNewFile(dataMeta);
            }
        }

        // 如果需要等待最新的合并任务完成，则执行同步
        trySyncLatestCompaction(waitForLatestCompaction);

        // 触发数据合并（可能是完整合并，也可能是增量合并）
        compactManager.triggerCompaction(forcedFullCompaction);
    }


    /**
     *  准备提交数据，将写缓冲区中的数据刷入存储，并处理合并任务。
     *
     *  @param waitCompaction 是否等待当前的合并任务完成
     *  @return 返回当前提交的增量数据
     *  @throws Exception 提交失败时抛出异常
     */
    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        // 刷新写缓冲区，将数据写入文件
        flushWriteBuffer(waitCompaction, false);

        // 如果配置了强制提交时进行合并，则需要等待合并任务完成
        if (commitForceCompact) {
            waitCompaction = true;
        }

        // 重新判断是否需要等待合并任务
        // 例如，在写入过程中多次失败，可能导致 Level 0 文件积累过多
        // 这里的等待可以避免 Level 0 过多的问题
        if (compactManager.shouldWaitForPreparingCheckpoint()) {
            waitCompaction = true;
        }

        // 尝试同步最新的合并结果
        trySyncLatestCompaction(waitCompaction);

        // 生成并返回提交增量
        return drainIncrement();
    }

    /**
     *  判断是否有正在进行的合并任务。
     *
     *  @return 如果有正在进行的合并任务，则返回 true，否则返回 false
     */
    @Override
    public boolean isCompacting() {
        return compactManager.isCompacting();
    }

    /**
     *  强制同步合并任务，确保所有的合并操作都已完成。
     *
     *  @throws Exception 如果同步失败，则抛出异常
     */
    @Override
    public void sync() throws Exception {
        trySyncLatestCompaction(true);
    }

    /**
     *  设置是否仅支持插入数据。
     *
     *  @param insertOnly 如果为 true，则不允许删除或更新，只能插入
     */
    @Override
    public void withInsertOnly(boolean insertOnly) {
        // 如果已经有数据写入到缓冲区，则不允许再修改 insertOnly 模式
        if (insertOnly && writeBuffer != null && writeBuffer.size() > 0) {
            throw new IllegalStateException(
                    "Insert-only can only be set before any record is received.");
        }
        this.isInsertOnly = insertOnly;
    }

    /**
     *  生成提交增量数据，将本次写入和合并的文件信息汇总并清空状态。
     *
     *  @return 返回包含新文件、删除文件、合并前后文件的增量数据
     */
    private CommitIncrement drainIncrement() {
        // 构造数据增量，包括新写入的文件、删除的文件和变更日志文件
        DataIncrement dataIncrement =
                new DataIncrement(
                        new ArrayList<>(newFiles),
                        new ArrayList<>(deletedFiles),
                        new ArrayList<>(newFilesChangelog));

        // 构造合并增量，包括合并前的文件、合并后的文件以及变更日志
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        new ArrayList<>(compactBefore.values()),
                        new ArrayList<>(compactAfter),
                        new ArrayList<>(compactChangelog));

        // 记录合并过程中的删除文件
        CompactDeletionFile drainDeletionFile = compactDeletionFile;

        // 清空相关状态，以准备下一次提交
        newFiles.clear();
        deletedFiles.clear();
        newFilesChangelog.clear();
        compactBefore.clear();
        compactAfter.clear();
        compactChangelog.clear();
        compactDeletionFile = null;

        // 返回最终的提交增量数据
        return new CommitIncrement(dataIncrement, compactIncrement, drainDeletionFile);
    }

    /**
     *  尝试同步最新的合并任务，确保合并任务完成后再执行后续操作。
     *
     *  @param blocking 是否阻塞等待合并任务完成
     *  @throws Exception 发生异常时抛出
     */
    private void trySyncLatestCompaction(boolean blocking) throws Exception {
        Optional<CompactResult> result = compactManager.getCompactionResult(blocking);
        result.ifPresent(this::updateCompactResult);
    }

    /**
     *  处理合并任务的结果，更新合并前后的文件列表，并删除不再需要的文件。
     *
     *  @param result 合并任务的结果
     */
    private void updateCompactResult(CompactResult result) {
        // 记录合并后的文件名
        Set<String> afterFiles =
                result.after().stream().map(DataFileMeta::fileName).collect(Collectors.toSet());

        // 遍历合并前的文件，决定哪些文件需要删除或保留
        for (DataFileMeta file : result.before()) {
            if (compactAfter.remove(file)) {
                // 该文件是中间合并文件（不是新增数据文件），合并完成后可以直接删除
                // 但需要确保：
                // 1. 该文件不是升级合并的输出
                // 2. 该文件不是升级合并的输入
                if (!compactBefore.containsKey(file.fileName())
                        && !afterFiles.contains(file.fileName())) {
                    writerFactory.deleteFile(file.fileName(), file.level());
                }
            } else {
                compactBefore.put(file.fileName(), file);
            }
        }

        // 记录合并后的文件
        compactAfter.addAll(result.after());
        compactChangelog.addAll(result.changelog());

        // 处理删除文件的合并信息
        updateCompactDeletionFile(result.deletionFile());
    }

    /**
     *  更新合并过程中的删除文件信息。
     *
     *  @param newDeletionFile 新的删除文件信息
     */
    private void updateCompactDeletionFile(@Nullable CompactDeletionFile newDeletionFile) {
        if (newDeletionFile != null) {
            compactDeletionFile =
                    compactDeletionFile == null
                            ? newDeletionFile
                            : newDeletionFile.mergeOldFile(compactDeletionFile);
        }
    }

    /**
     *  关闭写入器，释放资源并删除未提交的临时文件。
     *
     *  @throws Exception 关闭失败时抛出异常
     */
    @Override
    public void close() throws Exception {
        // 取消正在进行的合并任务，避免作业取消时被阻塞
        compactManager.cancelCompaction();
        sync();
        compactManager.close();

        // 记录需要删除的临时文件
        List<DataFileMeta> delete = new ArrayList<>(newFiles);
        newFiles.clear();
        deletedFiles.clear();

        // 删除变更日志文件
        for (DataFileMeta file : newFilesChangelog) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }
        newFilesChangelog.clear();

        // 删除合并后的临时文件（如果不是升级合并的输出）
        for (DataFileMeta file : compactAfter) {
            if (!compactBefore.containsKey(file.fileName())) {
                delete.add(file);
            }
        }
        compactAfter.clear();

        // 删除合并过程中的变更日志文件
        for (DataFileMeta file : compactChangelog) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }
        compactChangelog.clear();

        // 删除所有未提交的数据文件
        for (DataFileMeta file : delete) {
            writerFactory.deleteFile(file.fileName(), file.level());
        }

        // 清理合并过程中的删除文件
        if (compactDeletionFile != null) {
            compactDeletionFile.clean();
        }
    }

}
