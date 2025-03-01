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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DeletionFile;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * 删除向量可以高效地记录文件中被删除的行的位置，从而在处理文件时可以过滤掉这些已删除的行。
 */
public interface DeletionVector {

    /**
     * 标记指定位置的行已删除。
     * @param position 要标记为已删除的行的位置。
     */
    void delete(long position);

    /**
     * 将另一个 {@link DeletionVector} 合并到当前的删除向量中。
     * @param deletionVector 要合并的另一个删除向量。
     */
    void merge(DeletionVector deletionVector);

    /**
     * 标记指定位置的行已删除。
     * @param position 要标记为已删除的行的位置。
     * @return 如果添加的位置之前已被删除，则返回 false；如果之前未被删除，则返回 true。
     */
    default boolean checkedDelete(long position) {
        if (isDeleted(position)) {
            return false;
        } else {
            delete(position);
            return true;
        }
    }

    /**
     * 检查指定位置的行是否被标记为已删除。
     * @param position 要检查的行的位置。
     * @return 如果行被标记为已删除，则返回 true；否则返回 false。
     */
    boolean isDeleted(long position);

    /**
     * 确定删除向量是否为空，即是否没有删除任何行。
     * @return 如果删除向量为空，返回 true；否则返回 false。
     */
    boolean isEmpty();

    /** 返回删除向量中添加的不同整数的数量。 */
    long getCardinality();

    /**
     * 将删除向量序列化为字节数组，以便存储或传输。
     * @return 表示序列化删除向量的字节数组。
     */
    byte[] serializeToBytes();

    /**
     * 从字节数组反序列化删除向量。
     * @param bytes 包含序列化删除向量的字节数组。
     * @return 表示反序列化数据的 DeletionVector 实例。
     */
    static DeletionVector deserializeFromBytes(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             DataInputStream dis = new DataInputStream(bis)) {
            int magicNum = dis.readInt();
            if (magicNum == BitmapDeletionVector.MAGIC_NUMBER) {
                return BitmapDeletionVector.deserializeFromDataInput(dis);
            } else {
                throw new RuntimeException("无效的魔术数字：" + magicNum);
            }
        } catch (IOException e) {
            throw new RuntimeException("无法反序列化删除向量", e);
        }
    }

    static DeletionVector read(FileIO fileIO, DeletionFile deletionFile) throws IOException {
        Path path = new Path(deletionFile.path());
        try (SeekableInputStream input = fileIO.newInputStream(path)) {
            input.seek(deletionFile.offset());
            DataInputStream dis = new DataInputStream(input);
            int actualLength = dis.readInt();
            if (actualLength != deletionFile.length()) {
                throw new RuntimeException(
                        "大小不匹配，实际大小："
                                + actualLength
                                + "，预期大小："
                                + deletionFile.length()
                                + "，文件路径："
                                + path);
            }
            int magicNum = dis.readInt();
            if (magicNum == BitmapDeletionVector.MAGIC_NUMBER) {
                return BitmapDeletionVector.deserializeFromDataInput(dis);
            } else {
                throw new RuntimeException("无效的魔术数字：" + magicNum);
            }
        }
    }

    static Factory emptyFactory() {
        return fileName -> Optional.empty();
    }

    static Factory factory(@Nullable DeletionVectorsMaintainer dvMaintainer) {
        if (dvMaintainer == null) {
            return emptyFactory();
        }
        return dvMaintainer::deletionVectorOf;
    }

    static Factory factory(
            FileIO fileIO, List<DataFileMeta> files, @Nullable List<DeletionFile> deletionFiles) {
        DeletionFile.Factory factory = DeletionFile.factory(files, deletionFiles);
        return fileName -> {
            Optional<DeletionFile> deletionFile = factory.create(fileName);
            if (deletionFile.isPresent()) {
                return Optional.of(DeletionVector.read(fileIO, deletionFile.get()));
            }
            return Optional.empty();
        };
    }

    /** 创建 {@link DeletionVector} 的接口。 */
    interface Factory {
        Optional<DeletionVector> create(String fileName) throws IOException;
    }
}
