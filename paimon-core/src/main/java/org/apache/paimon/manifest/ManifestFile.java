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

package org.apache.paimon.manifest;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * 该文件包含多个 {@link ManifestEntry}，表示自上次快照以来的额外更改。
 */
public class ManifestFile extends ObjectsFile<ManifestEntry> {

    private final SchemaManager schemaManager; // 模式管理器
    private final RowType partitionType; // 分区类型
    private final FormatWriterFactory writerFactory; // 写文件格式工厂
    private final long suggestedFileSize; // 建议的文件大小

    private ManifestFile(
            FileIO fileIO, // 文件输入输出对象
            SchemaManager schemaManager, // 模式管理器
            RowType partitionType, // 分区类型
            ManifestEntrySerializer serializer, // ManifestEntry 序列化器
            RowType schema, // 模式
            FormatReaderFactory readerFactory, // 文件读取格式工厂
            FormatWriterFactory writerFactory, // 文件写入格式工厂
            String compression, // 压缩方式
            PathFactory pathFactory, // 路径工厂
            long suggestedFileSize, // 建议的文件大小
            @Nullable SegmentsCache<Path> cache) { // 文件缓存
        super(
                fileIO,
                serializer,
                schema,
                readerFactory,
                writerFactory,
                compression,
                pathFactory,
                cache);
        this.schemaManager = schemaManager;
        this.partitionType = partitionType;
        this.writerFactory = writerFactory;
        this.suggestedFileSize = suggestedFileSize;
    }

    @VisibleForTesting // 用于测试
    public long suggestedFileSize() {
        return suggestedFileSize;
    }

    /**
     * 将多个 {@link ManifestEntry} 写入清单文件。
     *
     * <p>注意：该方法是原子性的。
     */
    public List<ManifestFileMeta> write(List<ManifestEntry> entries) {
        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer = createRollingWriter();
        try {
            writer.write(entries); // 写入数据
            writer.close(); // 关闭写入器
        } catch (Exception e) {
            throw new RuntimeException(e); // 捕获并抛出异常
        }
        return writer.result(); // 返回写入结果
    }

    public RollingFileWriter<ManifestEntry, ManifestFileMeta> createRollingWriter() {
        return new RollingFileWriter<>(
                () -> new ManifestEntryWriter(writerFactory, pathFactory.newPath(), compression),
                suggestedFileSize);
    }

    private class ManifestEntryWriter extends SingleFileWriter<ManifestEntry, ManifestFileMeta> {

        private final SimpleStatsCollector partitionStatsCollector; // 分区统计收集器
        private final SimpleStatsConverter partitionStatsSerializer; // 分区统计序列化器

        private long numAddedFiles = 0; // 新增文件数量
        private long numDeletedFiles = 0; // 删除文件数量
        private long schemaId = Long.MIN_VALUE; // 模式 ID

        ManifestEntryWriter(FormatWriterFactory factory, Path path, String fileCompression) {
            super(
                    ManifestFile.this.fileIO,
                    factory,
                    path,
                    serializer::toRow,
                    fileCompression,
                    false);

            this.partitionStatsCollector = new SimpleStatsCollector(partitionType);
            this.partitionStatsSerializer = new SimpleStatsConverter(partitionType);
        }

        @Override
        public void write(ManifestEntry entry) throws IOException {
            super.write(entry); // 调用父类的写入方法

            switch (entry.kind()) { // 根据 ManifestEntry 的类型进行统计
                case ADD:
                    numAddedFiles++;
                    break;
                case DELETE:
                    numDeletedFiles++;
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown entry kind: " + entry.kind());
            }
            schemaId = Math.max(schemaId, entry.file().schemaId()); // 更新模式 ID

            partitionStatsCollector.collect(entry.partition()); // 收集分区统计
        }

        @Override
        public ManifestFileMeta result() throws IOException {
            return new ManifestFileMeta(
                    path.getName(), // 文件名
                    fileIO.getFileSize(path), // 文件大小
                    numAddedFiles, // 新增文件数量
                    numDeletedFiles, // 删除文件数量
                    partitionStatsSerializer.toBinary(partitionStatsCollector.extract()), // 分区统计二进制
                    numAddedFiles + numDeletedFiles > 0 // 选择模式 ID
                            ? schemaId
                            : schemaManager.latest().get().id());
        }
    }

    /** ManifestFile 的创建工厂。 */
    public static class Factory {

        private final FileIO fileIO; // 文件输入输出对象
        private final SchemaManager schemaManager; // 模式管理器
        private final RowType partitionType; // 分区类型
        private final FileFormat fileFormat; // 文件格式
        private final String compression; // 压缩方式
        private final FileStorePathFactory pathFactory; // 文件存储路径工厂
        private final long suggestedFileSize; // 建议的文件大小
        @Nullable private final SegmentsCache<Path> cache; // 文件缓存

        public Factory(
                FileIO fileIO,
                SchemaManager schemaManager,
                RowType partitionType,
                FileFormat fileFormat,
                String compression,
                FileStorePathFactory pathFactory,
                long suggestedFileSize,
                @Nullable SegmentsCache<Path> cache) {
            this.fileIO = fileIO;
            this.schemaManager = schemaManager;
            this.partitionType = partitionType;
            this.fileFormat = fileFormat;
            this.compression = compression;
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
            this.cache = cache;
        }

        public boolean isCacheEnabled() {
            return cache != null; // 是否启用缓存
        }

        public ManifestFile create() {
            RowType entryType = VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA);
            return new ManifestFile(
                    fileIO,
                    schemaManager,
                    partitionType,
                    new ManifestEntrySerializer(),
                    entryType,
                    fileFormat.createReaderFactory(entryType),
                    fileFormat.createWriterFactory(entryType),
                    compression,
                    pathFactory.manifestFileFactory(),
                    suggestedFileSize,
                    cache);
        }

        public ObjectsFile<SimpleFileEntry> createSimpleFileEntryReader() {
            RowType entryType = VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA);
            return new ObjectsFile<>(
                    fileIO,
                    new SimpleFileEntrySerializer(),
                    entryType,
                    fileFormat.createReaderFactory(entryType),
                    fileFormat.createWriterFactory(entryType),
                    compression,
                    pathFactory.manifestFileFactory(),
                    cache);
        }
    }
}
