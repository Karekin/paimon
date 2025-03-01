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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.index.IndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** 删除向量索引文件。 */
public class DeletionVectorsIndexFile extends IndexFile {

    public static final String DELETION_VECTORS_INDEX = "DELETION_VECTORS"; // 删除向量索引文件类型标识
    public static final byte VERSION_ID_V1 = 1; // 文件版本号

    private final MemorySize targetSizePerIndexFile; // 每个索引文件的目标大小

    /**
     * 构造函数，初始化删除向量索引文件。
     * @param fileIO 文件输入输出对象
     * @param pathFactory 路径工厂
     * @param targetSizePerIndexFile 每个索引文件的目标大小
     */
    public DeletionVectorsIndexFile(
            FileIO fileIO, PathFactory pathFactory, MemorySize targetSizePerIndexFile) {
        super(fileIO, pathFactory);
        this.targetSizePerIndexFile = targetSizePerIndexFile;
    }

    /**
     * 从指定的索引文件元数据中读取所有删除向量。
     * @param fileMeta 索引文件元数据
     * @return 包含文件名和对应删除向量的映射表
     * @throws UncheckedIOException 如果读取文件时发生 I/O 错误
     */
    public Map<String, DeletionVector> readAllDeletionVectors(IndexFileMeta fileMeta) {
        LinkedHashMap<String, Pair<Integer, Integer>> deletionVectorRanges =
                fileMeta.deletionVectorsRanges();
        checkNotNull(deletionVectorRanges);

        String indexFileName = fileMeta.fileName();
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        Path filePath = pathFactory.toPath(indexFileName);
        try (SeekableInputStream inputStream = fileIO.newInputStream(filePath)) {
            checkVersion(inputStream); // 检查版本号
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            for (Map.Entry<String, Pair<Integer, Integer>> entry :
                    deletionVectorRanges.entrySet()) {
                deletionVectors.put(
                        entry.getKey(),
                        readDeletionVector(dataInputStream, entry.getValue().getRight()));
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "无法从文件 " + filePath + " 读取删除向量，删除向量范围："
                            + deletionVectorRanges,
                    e);
        }
        return deletionVectors;
    }

    /**
     * 从多个索引文件元数据中读取所有删除向量。
     * @param indexFiles 索引文件元数据列表
     * @return 包含文件名和对应删除向量的映射表
     */
    public Map<String, DeletionVector> readAllDeletionVectors(List<IndexFileMeta> indexFiles) {
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        indexFiles.forEach(indexFile -> deletionVectors.putAll(readAllDeletionVectors(indexFile)));
        return deletionVectors;
    }

    /**
     * 从同一索引文件的多个删除文件中读取所有删除向量。
     * @param dataFileToDeletionFiles 数据文件与删除文件的映射表
     * @return 包含文件名和对应删除向量的映射表
     */
    public Map<String, DeletionVector> readDeletionVector(
            Map<String, DeletionFile> dataFileToDeletionFiles) {
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        if (dataFileToDeletionFiles.isEmpty()) {
            return deletionVectors;
        }

        String indexFile = dataFileToDeletionFiles.values().stream().findAny().get().path();
        try (SeekableInputStream inputStream = fileIO.newInputStream(new Path(indexFile))) {
            checkVersion(inputStream); // 检查版本号
            for (String dataFile : dataFileToDeletionFiles.keySet()) {
                DeletionFile deletionFile = dataFileToDeletionFiles.get(dataFile);
                checkArgument(deletionFile.path().equals(indexFile));
                inputStream.seek(deletionFile.offset());
                DataInputStream dataInputStream = new DataInputStream(inputStream);
                deletionVectors.put(
                        dataFile, readDeletionVector(dataInputStream, (int) deletionFile.length()));
            }
        } catch (Exception e) {
            throw new RuntimeException("无法从文件 " + indexFile + " 读取删除向量", e);
        }
        return deletionVectors;
    }

    /**
     * 从删除文件中读取删除向量。
     * @param deletionFile 删除文件
     * @return 删除向量对象
     */
    public DeletionVector readDeletionVector(DeletionFile deletionFile) {
        String indexFile = deletionFile.path();
        try (SeekableInputStream inputStream = fileIO.newInputStream(new Path(indexFile))) {
            checkVersion(inputStream); // 检查版本号
            checkArgument(deletionFile.path().equals(indexFile));
            inputStream.seek(deletionFile.offset());
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            return readDeletionVector(dataInputStream, (int) deletionFile.length());
        } catch (Exception e) {
            throw new RuntimeException("无法从文件 " + indexFile + " 读取删除向量", e);
        }
    }

    /**
     * 将删除向量写入新文件。
     * @param input 包含文件名和对应删除向量的映射表
     * @return 写入的新索引文件元数据列表
     * @throws UncheckedIOException 如果写入文件时发生 I/O 错误
     */
    public List<IndexFileMeta> write(Map<String, DeletionVector> input) {
        try {
            DeletionVectorIndexFileWriter writer =
                    new DeletionVectorIndexFileWriter(
                            this.fileIO, this.pathFactory, this.targetSizePerIndexFile);
            return writer.write(input);
        } catch (IOException e) {
            throw new RuntimeException("无法写入删除向量", e);
        }
    }

    /**
     * 检查文件版本号。
     * @param in 输入流
     * @throws IOException 如果读取流时发生 I/O 错误
     */
    private void checkVersion(InputStream in) throws IOException {
        int version = in.read();
        if (version != VERSION_ID_V1) {
            throw new RuntimeException(
                    "版本号不匹配，实际版本号: " + version + ", 预期版本号: " + VERSION_ID_V1);
        }
    }

    /**
     * 从数据输入流中读取删除向量。
     * @param inputStream 数据输入流
     * @param size 删除向量的大小
     * @return 删除向量对象
     */
    private DeletionVector readDeletionVector(DataInputStream inputStream, int size) {
        try {
            // 检查大小
            int actualSize = inputStream.readInt();
            if (actualSize != size) {
                throw new RuntimeException(
                        "大小不匹配，实际大小: " + actualSize + ", 预期大小: " + size);
            }

            // 读取删除向量字节数据
            byte[] bytes = new byte[size];
            inputStream.readFully(bytes);

            // 检查校验和
            int checkSum = calculateChecksum(bytes);
            int actualCheckSum = inputStream.readInt();
            if (actualCheckSum != checkSum) {
                throw new RuntimeException(
                        "校验和不匹配，实际校验和: "
                                + actualCheckSum
                                + ", 预期校验和: "
                                + checkSum);
            }
            return DeletionVector.deserializeFromBytes(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("无法读取删除向量", e);
        }
    }

    /**
     * 计算字节数组的校验和。
     * @param bytes 字节数组
     * @return 校验和
     */
    public static int calculateChecksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
