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

package org.apache.paimon.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.utils.FileUtils.checkExists;

/**
 * 一个包含多个 {@link T} 对象的文件，提供读取和写入功能。
 */
public class ObjectsFile<T> implements SimpleFileReader<T> {

    protected final FileIO fileIO; // 文件输入输出对象
    protected final ObjectSerializer<T> serializer; // 对象序列化器
    protected final FormatReaderFactory readerFactory; // 格式读取器工厂
    protected final FormatWriterFactory writerFactory; // 格式写入器工厂
    protected final String compression; // 压缩方式
    protected final PathFactory pathFactory; // 路径工厂

    @Nullable private final ObjectsCache<Path, T> cache; // 对象缓存

    public ObjectsFile(
            FileIO fileIO,
            ObjectSerializer<T> serializer,
            RowType formatType,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            String compression,
            PathFactory pathFactory,
            @Nullable SegmentsCache<Path> cache) {
        this.fileIO = fileIO;
        this.serializer = serializer;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.compression = compression;
        this.pathFactory = pathFactory;
        this.cache =
                cache == null
                        ? null
                        : new ObjectsCache<>(
                        cache,
                        serializer,
                        formatType,
                        this::fileSize,
                        this::createIterator);
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public long fileSize(String fileName) {
        try {
            return fileIO.getFileSize(pathFactory.toPath(fileName)); // 获取文件大小
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<T> read(String fileName) {
        return read(fileName, null);
    }

    public List<T> read(String fileName, @Nullable Long fileSize) {
        return read(fileName, fileSize, Filter.alwaysTrue(), Filter.alwaysTrue());
    }

    public List<T> readWithIOException(String fileName) throws IOException {
        return readWithIOException(fileName, null);
    }

    public List<T> readWithIOException(String fileName, @Nullable Long fileSize)
            throws IOException {
        return readWithIOException(fileName, fileSize, Filter.alwaysTrue(), Filter.alwaysTrue());
    }

    public boolean exists(String fileName) {
        try {
            return fileIO.exists(pathFactory.toPath(fileName)); // 检查文件是否存在
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<T> read(
            String fileName,
            @Nullable Long fileSize,
            Filter<InternalRow> loadFilter,
            Filter<InternalRow> readFilter) {
        try {
            return readWithIOException(fileName, fileSize, loadFilter, readFilter);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read manifest list " + fileName, e);
        }
    }

    private List<T> readWithIOException(
            String fileName,
            @Nullable Long fileSize,
            Filter<InternalRow> loadFilter,
            Filter<InternalRow> readFilter)
            throws IOException {
        Path path = pathFactory.toPath(fileName);
        if (cache != null) {
            return cache.read(path, fileSize, loadFilter, readFilter); // 使用缓存读取数据
        }

        return readFromIterator(createIterator(path, fileSize), serializer, readFilter); // 从迭代器中读取数据
    }

    public String writeWithoutRolling(Collection<T> records) {
        return writeWithoutRolling(records.iterator());
    }

    public String writeWithoutRolling(Iterator<T> records) {
        Path path = pathFactory.newPath(); // 创建新路径
        try {
            try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
                try (FormatWriter writer = writerFactory.create(out, compression)) {
                    while (records.hasNext()) {
                        writer.addElement(serializer.toRow(records.next())); // 将对象写入文件
                    }
                }
            }
            return path.getName(); // 返回文件名
        } catch (Throwable e) {
            fileIO.deleteQuietly(path); // 删除失败的文件
            throw new RuntimeException(
                    "Exception occurs when writing records to " + path + ". Clean up.", e);
        }
    }

    private CloseableIterator<InternalRow> createIterator(Path file, @Nullable Long fileSize)
            throws IOException {
        return FileUtils.createFormatReader(fileIO, readerFactory, file, fileSize)
                .toCloseableIterator(); // 创建格式读取器的迭代器
    }

    private long fileSize(Path file) throws IOException {
        try {
            return fileIO.getFileSize(file); // 获取文件大小
        } catch (IOException e) {
            checkExists(fileIO, file); // 检查文件是否存在
            throw e;
        }
    }

    public void delete(String fileName) {
        fileIO.deleteQuietly(pathFactory.toPath(fileName)); // 删除文件
    }

    public static <V> List<V> readFromIterator(
            CloseableIterator<InternalRow> inputIterator,
            ObjectSerializer<V> serializer,
            Filter<InternalRow> readFilter) {
        try (CloseableIterator<InternalRow> iterator = inputIterator) {
            List<V> result = new ArrayList<>();
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                if (readFilter.test(row)) {
                    result.add(serializer.fromRow(row)); // 将行数据转换为对象
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
