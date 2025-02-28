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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.AsyncPositionOutputStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

/**
 * 一个用于生成单个文件的文件写入器，实现了 {@link FileWriter} 接口。
 *
 * @param <T> 要写入的记录类型。
 * @param <R> 在写完文件后生成的结果类型。
 */
public abstract class SingleFileWriter<T, R> implements FileWriter<T, R> {

    private static final Logger LOG = LoggerFactory.getLogger(SingleFileWriter.class); // 日志工具

    // 文件 I/O 实例
    protected final FileIO fileIO;

    // 文件路径
    protected final Path path;

    // 将记录转换为 InternalRow 的函数
    private final Function<T, InternalRow> converter;

    // 数据写入器
    private final FormatWriter writer;

    // 输出流
    private PositionOutputStream out;

    // 已写入的记录数量
    private long recordCount;

    // 标记写入器是否已关闭
    protected boolean closed;

    /**
     * 构造方法，初始化单个文件写入器。
     *
     * @param fileIO 文件 I/O 实例
     * @param factory 文件写入工厂实例
     * @param path 文件路径
     * @param converter 将记录转换为 InternalRow 的函数
     * @param compression 压缩方式
     * @param asyncWrite 是否异步写入
     */
    public SingleFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            Function<T, InternalRow> converter,
            String compression,
            boolean asyncWrite) {
        this.fileIO = fileIO;
        this.path = path;
        this.converter = converter;

        // 打开文件输出流
        try {
            out = fileIO.newOutputStream(path, false);
            if (asyncWrite) {
                out = new AsyncPositionOutputStream(out); // 如果启用异步写入，包装为异步输出流
            }
            writer = factory.create(out, compression); // 创建格式化的数据写入器
        } catch (IOException e) {
            LOG.warn(
                    "打开批量写入器时失败，关闭输出流并抛出错误。",
                    e);
            if (out != null) {
                abort(); // 如果发生异常，回滚写入操作
            }
            throw new UncheckedIOException(e);
        }

        this.recordCount = 0; // 初始化已写入的记录数量
        this.closed = false; // 初始状态为未关闭
    }

    /**
     * 获取文件路径。
     *
     * @return 文件路径
     */
    public Path path() {
        return path;
    }

    /**
     * 写入单条记录。
     *
     * @param record 要写入的记录
     * @throws IOException 如果在写入时发生 I/O 错误
     */
    @Override
    public void write(T record) throws IOException {
        writeImpl(record); // 调用实际的写入实现方法
    }

    /**
     * 写入批量记录。
     *
     * @param bundle 包含多条记录的 BundleRecords
     * @throws IOException 如果在写入时发生 I/O 错误
     */
    public void writeBundle(BundleRecords bundle) throws IOException {
        if (closed) {
            throw new RuntimeException("写入器已关闭！");
        }

        try {
            // 如果数据写入器支持批处理，则调用相应方法
            if (writer instanceof BundleFormatWriter) {
                ((BundleFormatWriter) writer).writeBundle(bundle);
            } else {
                // 否则逐条写入记录
                for (InternalRow row : bundle) {
                    writer.addElement(row);
                }
            }
            recordCount += bundle.rowCount(); // 更新记录计数
        } catch (Throwable e) {
            LOG.warn("写入文件 " + path + " 时发生异常。正在清理资源。", e);
            abort(); // 发生异常时回滚写入操作
            throw e; // 重新抛出异常
        }
    }

    /**
     * 实际写入单条记录的实现方法。
     *
     * @param record 要写入的记录
     * @return 转换后的 InternalRow 行数据
     * @throws IOException 如果在写入时发生 I/O 错误
     */
    protected InternalRow writeImpl(T record) throws IOException {
        if (closed) {
            throw new RuntimeException("写入器已关闭！");
        }

        try {
            InternalRow rowData = converter.apply(record); // 将记录转换为 InternalRow 行数据
            writer.addElement(rowData); // 将行数据写入文件
            recordCount++; // 增加记录计数
            return rowData;
        } catch (Throwable e) {
            LOG.warn("写入文件 " + path + " 时发生异常。正在清理资源。", e);
            abort(); // 发生异常时回滚写入操作
            throw e; // 重新抛出异常
        }
    }

    /**
     * 获取已写入的记录数量。
     *
     * @return 记录数量
     */
    @Override
    public long recordCount() {
        return recordCount;
    }

    /**
     * 判断是否达到目标文件大小。
     *
     * @param suggestedCheck 是否建议检查
     * @param targetSize 目标文件大小
     * @return 是否达到目标文件大小
     * @throws IOException 如果在检查时发生 I/O 错误
     */
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return writer.reachTargetSize(suggestedCheck, targetSize);
    }

    /**
     * 回滚写入操作，清理资源。
     */
    @Override
    public void abort() {
        IOUtils.closeQuietly(out); // 关闭输出流
        fileIO.deleteQuietly(path); // 删除文件
    }

    /**
     * 获取回滚执行器，专门用于回滚操作，避免持有整个写入器的引用。
     *
     * @return 回滚执行器
     */
    public AbortExecutor abortExecutor() {
        if (!closed) {
            throw new RuntimeException("写入器应处于已关闭状态！");
        }

        return new AbortExecutor(fileIO, path);
    }

    /**
     * 关闭文件写入器。
     *
     * @throws IOException 如果在关闭时发生 I/O 错误
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return; // 如果已关闭，直接返回
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("关闭文件 " + path);
        }

        try {
            writer.close(); // 关闭数据写入器
            out.flush(); // 刷新输出流
            out.close(); // 关闭输出流
        } catch (IOException e) {
            LOG.warn("关闭文件 " + path + " 时发生异常。正在清理资源。", e);
            abort(); // 发生异常时回滚写入操作
            throw e; // 重新抛出异常
        } finally {
            closed = true; // 标记写入器为已关闭状态
        }
    }

    /**
     * 回滚执行器，专门用于回滚操作，避免持有整个写入器的引用。
     */
    public static class AbortExecutor {

        // 文件 I/O 实例
        private final FileIO fileIO;

        // 文件路径
        private final Path path;

        /**
         * 构造方法，初始化回滚执行器。
         *
         * @param fileIO 文件 I/O 实例
         * @param path 文件路径
         */
        private AbortExecutor(FileIO fileIO, Path path) {
            this.fileIO = fileIO;
            this.path = path;
        }

        /**
         * 回滚写入操作，删除文件。
         */
        public void abort() {
            fileIO.deleteQuietly(path); // 删除文件
        }
    }
}
