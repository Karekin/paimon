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

package org.apache.paimon.lookup;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;
import org.rocksdb.TtlDB;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * RocksDB的批量加载器。
 * 该类用于将数据批量加载到RocksDB中，通过生成SST文件并将其加载到数据库中。
 */
public class BulkLoader {

    private final String uuid = UUID.randomUUID().toString(); // 用于生成唯一的SST文件名
    private final ColumnFamilyHandle columnFamily; // 列族句柄
    private final String path; // 数据存储路径
    private final RocksDB db; // RocksDB数据库实例
    private final boolean isTtlEnabled; // 是否启用时间戳生命周期管理
    private final Options options; // RocksDB选项配置
    private final List<String> files = new ArrayList<>(); // 存储生成的SST文件路径
    private final int currentTimeSeconds; // 当前时间（秒级时间戳）

    private SstFileWriter writer = null; // 当前写入的SST文件写入器
    private int sstIndex = 0; // 当前SST文件的索引
    private long recordNum = 0; // 已记录的数据数量

    /**
     * 构造函数，初始化BulkLoader对象。
     * <p>
     * 该构造函数接收以下几个参数：
     * - db: RocksDB数据库实例
     * - options: RocksDB选项配置
     * - columnFamily: 列族句柄
     * - path: 数据存储路径
     *
     * @param db 数据库实例
     * @param options 数据库选项
     * @param columnFamily 列族句柄
     * @param path 数据存储路径
     */
    public BulkLoader(RocksDB db, Options options, ColumnFamilyHandle columnFamily, String path) {
        this.db = db;
        this.isTtlEnabled = db instanceof TtlDB; // 是否启用了TTL
        this.options = options;
        this.columnFamily = columnFamily;
        this.path = path;
        this.currentTimeSeconds = (int) (System.currentTimeMillis() / 1000); // 当前时间秒级时间戳
    }

    /**
     * 向写入器中写入键值对。
     * <p>
     * 该方法会根据当前状态，创建新的SST文件写入器或继续写入当前写入器。
     * 如果启用了TTL，会在值中追加时间戳。
     *
     * @param key 键字节数组
     * @param value 值字节数组
     * @throws WriteException 如果写入过程中出现异常
     */
    public void write(byte[] key, byte[] value) throws WriteException {
        try {
            if (writer == null) { // 如果写入器为空
                writer = new SstFileWriter(new EnvOptions(), options); // 创建新的SST文件写入器
                // 生成新的SST文件路径
                String filePath = new File(this.path, "sst-" + uuid + "-" + (sstIndex++)).getPath();
                writer.open(filePath); // 打开文件写入器
                files.add(filePath); // 将文件路径添加到列表
            }

            if (isTtlEnabled) { // 如果启用了TTL
                value = appendTimestamp(value); // 在值中追加时间戳
            }

            try {
                writer.put(key, value); // 写入键值对
            } catch (RocksDBException e) { // 捕获RocksDB异常
                throw new WriteException(e); // 抛出自定义的写入异常
            }

            recordNum++; // 增加已记录的数据数量
            // 如果已记录的数据数量达到1000，并且当前SST文件大小大于等于目标文件大小，关闭当前写入器
            if (recordNum % 1000 == 0 && writer.fileSize() >= options.targetFileSizeBase()) {
                writer.finish(); // 完成写入
                writer.close(); // 关闭写入器
                writer = null; // 重置写入器
                recordNum = 0; // 重置记录数量
            }
        } catch (RocksDBException e) { // 捕获RocksDB异常
            throw new RuntimeException(e); // 抛出运行时异常
        }
    }

    /**
     * 向值字节数组末尾追加时间戳。
     * <p>
     * 该方法会将当前时间戳转换为4字节的字节数组，并追加到值的末尾。
     * 时间戳在值中的存储格式为：
     * - 第一个字节：时间戳的低8位
     * - 第二个字节：时间戳的次低8位
     * - 第三个字节：时间戳的次高8位
     * - 第四个字节：时间戳的高8位
     *
     * @param value 值字节数组
     * @return 包含时间戳的值字节数组
     */
    private byte[] appendTimestamp(byte[] value) {
        byte[] newValue = new byte[value.length + 4]; // 创建新的字节数组
        System.arraycopy(value, 0, newValue, 0, value.length); // 将原值拷贝到新数组
        // 将时间戳转换为4字节的字节数组
        newValue[value.length] = (byte) (currentTimeSeconds & 0xff);
        newValue[value.length + 1] = (byte) ((currentTimeSeconds >> 8) & 0xff);
        newValue[value.length + 2] = (byte) ((currentTimeSeconds >> 16) & 0xff);
        newValue[value.length + 3] = (byte) ((currentTimeSeconds >> 24) & 0xff);
        return newValue; // 返回新数组
    }

    /**
     * 完成批量加载。
     * <p>
     * 该方法会关闭当前的SST文件写入器，并将生成的SST文件加载到RocksDB数据库中。
     * 加载过程通过RocksDB的`ingestExternalFile`方法完成。
     */
    public void finish() {
        try {
            if (writer != null) { // 如果写入器不为空
                writer.finish(); // 完成写入
                writer.close(); // 关闭写入器
            }

            if (files.size() > 0) { // 如果生成了SST文件
                IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions(); // 创建加载选项
                db.ingestExternalFile(columnFamily, files, ingestOptions); // 加载SST文件到数据库
                ingestOptions.close(); // 关闭加载选项
            }
        } catch (RocksDBException e) { // 捕获RocksDB异常
            throw new RuntimeException(e); // 抛出运行时异常
        }
    }

    /**
     * 写入异常类，用于表示写入过程中的异常。
     * <p>
     * 该类继承自{@link Exception}，并提供一个构造函数来接受异常的成因。
     */
    public static class WriteException extends Exception {
        public WriteException(Throwable cause) {
            super(cause);
        }
    }
}