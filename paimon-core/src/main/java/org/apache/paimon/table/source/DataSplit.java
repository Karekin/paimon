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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMeta08Serializer;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.FunctionWithIOException;
import org.apache.paimon.utils.SerializationUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFilePathFactory.INDEX_PATH_SUFFIX;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 数据切分类。大多数批量计算引擎需要输入切分。
 * 此类用于表示数据切分的逻辑单元，通常用于读取和处理分区数据。
 * 该类实现了`Split`接口，主要功能包括：
 * - 提供对数据切分的基本信息的访问，如快照ID、分区信息、桶编号等。
 * - 支持数据文件和删除文件的管理。
 * - 提供数据文件的统计信息，如行数。
 * - 支持将数据文件转换为原始文件格式。
 * - 提供数据文件的索引文件信息。
 * - 实现了对象的序列化和反序列化。
 */
public class DataSplit implements Split {

    /** 用于序列化版本控制的序列化序列号。 */
    private static final long serialVersionUID = 7L;

    /** 魔数（Magic Number）用于标记文件格式或数据结构。 */
    private static final long MAGIC = -2394839472490812314L;

    /** 当前版本号。 */
    private static final int VERSION = 2;

    /** 数据快照ID。 */
    private long snapshotId = 0;

    /** 分区信息（二进制行格式）。 */
    private BinaryRow partition;

    /** 桶编号（用于数据分区）。 */
    private int bucket = -1;

    /** 桶路径（存储桶的路径）。 */
    private String bucketPath;

    /** 前置数据文件列表。 */
    private List<DataFileMeta> beforeFiles = new ArrayList<>();

    /** 前置删除文件列表（可能为`null`）。 */
    @Nullable private List<DeletionFile> beforeDeletionFiles;

    /** 数据文件列表。 */
    private List<DataFileMeta> dataFiles;

    /** 数据删除文件列表（可能为`null`）。 */
    @Nullable private List<DeletionFile> dataDeletionFiles;

    /** 是否为流式数据。 */
    private boolean isStreaming = false;

    /** 是否可转换为原始文件格式。 */
    private boolean rawConvertible;

    /** 构造函数。 */
    public DataSplit() {}

    /** 获取快照ID。 */
    public long snapshotId() {
        return snapshotId;
    }

    /** 获取分区信息。 */
    public BinaryRow partition() {
        return partition;
    }

    /** 获取桶编号。 */
    public int bucket() {
        return bucket;
    }

    /** 获取桶路径。 */
    public String bucketPath() {
        return bucketPath;
    }

    /** 获取前置数据文件列表。 */
    public List<DataFileMeta> beforeFiles() {
        return beforeFiles;
    }

    /** 获取前置删除文件列表（作为`Optional`返回）。 */
    public Optional<List<DeletionFile>> beforeDeletionFiles() {
        return Optional.ofNullable(beforeDeletionFiles);
    }

    /** 获取数据文件列表。 */
    public List<DataFileMeta> dataFiles() {
        return dataFiles;
    }

    /** 获取数据删除文件列表（作为`Optional`返回）。 */
    @Override
    public Optional<List<DeletionFile>> deletionFiles() {
        return Optional.ofNullable(dataDeletionFiles);
    }

    /** 是否为流式数据。 */
    public boolean isStreaming() {
        return isStreaming;
    }

    /** 是否可转换为原始文件格式。 */
    public boolean rawConvertible() {
        return rawConvertible;
    }

    /**
     * 获取数据文件的最新创建时间戳（以毫秒为单位）。
     * 使用流操作从`dataFiles`列表中提取文件的创建时间戳，并返回最大值。
     */
    public OptionalLong latestFileCreationEpochMillis() {
        return this.dataFiles.stream().mapToLong(DataFileMeta::creationTimeEpochMillis).max();
    }

    /**
     * 获取数据文件的最早创建时间戳（以毫秒为单位）。
     * 使用流操作从`dataFiles`列表中提取文件的创建时间戳，并返回最小值。
     */
    public OptionalLong earliestFileCreationEpochMillis() {
        return this.dataFiles.stream().mapToLong(DataFileMeta::creationTimeEpochMillis).min();
    }

    /**
     * 获取数据文件的总行数。
     * 遍历`dataFiles`列表，累加每个文件的行数。
     */
    @Override
    public long rowCount() {
        long rowCount = 0;
        for (DataFileMeta file : dataFiles) {
            rowCount += file.rowCount();
        }
        return rowCount;
    }

    /**
     * 将数据文件转换为原始文件格式。
     * 如果`rawConvertible`为`true`，则将`dataFiles`中的每个文件转换为`RawFile`对象并返回列表。
     * 否则，返回空`Optional`。
     */
    @Override
    public Optional<List<RawFile>> convertToRawFiles() {
        if (rawConvertible) {
            return Optional.of(
                    dataFiles.stream()
                            .map(f -> makeRawTableFile(bucketPath, f))
                            .collect(Collectors.toList()));
        } else {
            return Optional.empty();
        }
    }

    /**
     * 将数据文件路径和元数据转换为`RawFile`对象。
     */
    private RawFile makeRawTableFile(String bucketPath, DataFileMeta file) {
        return new RawFile(
                bucketPath + "/" + file.fileName(),
                file.fileSize(),
                0,
                file.fileSize(),
                file.fileFormat(),
                file.schemaId(),
                file.rowCount());
    }

    /**
     * 获取数据文件的索引文件信息。
     * 遍历`dataFiles`列表，查找每个文件的索引文件（文件名以`INDEX_PATH_SUFFIX`结尾）。
     * 如果找到索引文件，则创建`IndexFile`对象并返回列表。
     * 如果未找到，则返回空`Optional`。
     */
    @Override
    @Nullable
    public Optional<List<IndexFile>> indexFiles() {
        List<IndexFile> indexFiles = new ArrayList<>();
        boolean hasIndexFile = false;
        for (DataFileMeta file : dataFiles) {
            List<String> exFiles =
                    file.extraFiles().stream()
                            .filter(s -> s.endsWith(INDEX_PATH_SUFFIX))
                            .collect(Collectors.toList());
            if (exFiles.isEmpty()) {
                indexFiles.add(null);
            } else if (exFiles.size() == 1) {
                hasIndexFile = true;
                indexFiles.add(new IndexFile(bucketPath + "/" + exFiles.get(0)));
            } else {
                throw new RuntimeException(
                        "Wrong number of file index for file "
                                + file.fileName()
                                + " index files: "
                                + String.join(",", exFiles));
            }
        }

        return hasIndexFile ? Optional.of(indexFiles) : Optional.empty();
    }

    /**
     * 对象的equals方法实现。
     * 比较两个`DataSplit`对象的各个字段是否相等。
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataSplit dataSplit = (DataSplit) o;
        return snapshotId == dataSplit.snapshotId
                && bucket == dataSplit.bucket
                && isStreaming == dataSplit.isStreaming
                && rawConvertible == dataSplit.rawConvertible
                && Objects.equals(partition, dataSplit.partition)
                && Objects.equals(bucketPath, dataSplit.bucketPath)
                && Objects.equals(beforeFiles, dataSplit.beforeFiles)
                && Objects.equals(beforeDeletionFiles, dataSplit.beforeDeletionFiles)
                && Objects.equals(dataFiles, dataSplit.dataFiles)
                && Objects.equals(dataDeletionFiles, dataSplit.dataDeletionFiles);
    }

    /**
     * 对象的hashCode方法实现。
     * 根据各字段的哈希值生成对象的哈希码。
     */
    @Override
    public int hashCode() {
        return Objects.hash(
                snapshotId,
                partition,
                bucket,
                bucketPath,
                beforeFiles,
                beforeDeletionFiles,
                dataFiles,
                dataDeletionFiles,
                isStreaming,
                rawConvertible);
    }

    /**
     * 自定义对象的序列化方法。
     * 使用`DataOutputView`进行序列化。
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    /**
     * 自定义对象的反序列化方法。
     * 使用`DataInputView`进行反序列化。
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        assign(deserialize(new DataInputViewStreamWrapper(in)));
    }

    /**
     * 将另一个`DataSplit`对象的字段复制到当前对象。
     */
    private void assign(DataSplit other) {
        this.snapshotId = other.snapshotId;
        this.partition = other.partition;
        this.bucket = other.bucket;
        this.bucketPath = other.bucketPath;
        this.beforeFiles = other.beforeFiles;
        this.beforeDeletionFiles = other.beforeDeletionFiles;
        this.dataFiles = other.dataFiles;
        this.dataDeletionFiles = other.dataDeletionFiles;
        this.isStreaming = other.isStreaming;
        this.rawConvertible = other.rawConvertible;
    }

    /**
     * 序列化方法，将对象数据写入`DataOutputView`。
     */
    public void serialize(DataOutputView out) throws IOException {
        out.writeLong(MAGIC);
        out.writeInt(VERSION);
        out.writeLong(snapshotId);
        SerializationUtils.serializeBinaryRow(partition, out);
        out.writeInt(bucket);
        out.writeUTF(bucketPath);

        DataFileMetaSerializer dataFileSer = new DataFileMetaSerializer();
        out.writeInt(beforeFiles.size());
        for (DataFileMeta file : beforeFiles) {
            dataFileSer.serialize(file, out);
        }

        DeletionFile.serializeList(out, beforeDeletionFiles);

        out.writeInt(dataFiles.size());
        for (DataFileMeta file : dataFiles) {
            dataFileSer.serialize(file, out);
        }

        DeletionFile.serializeList(out, dataDeletionFiles);

        out.writeBoolean(isStreaming);

        out.writeBoolean(rawConvertible);
    }

    /**
     * 反序列化方法，从`DataInputView`读取数据并生成`DataSplit`对象。
     */
    public static DataSplit deserialize(DataInputView in) throws IOException {
        long magic = in.readLong();
        int version = magic == MAGIC ? in.readInt() : 1;
        // 如果版本号为1，第一部分为快照ID，否则使用魔数和版本号
        long snapshotId = version == 1 ? magic : in.readLong();
        BinaryRow partition = SerializationUtils.deserializeBinaryRow(in);
        int bucket = in.readInt();
        String bucketPath = in.readUTF();

        FunctionWithIOException<DataInputView, DataFileMeta> dataFileSer =
                getFileMetaSerde(version);
        int beforeNumber = in.readInt();
        List<DataFileMeta> beforeFiles = new ArrayList<>(beforeNumber);
        for (int i = 0; i < beforeNumber; i++) {
            beforeFiles.add(dataFileSer.apply(in));
        }

        List<DeletionFile> beforeDeletionFiles = DeletionFile.deserializeList(in);

        int fileNumber = in.readInt();
        List<DataFileMeta> dataFiles = new ArrayList<>(fileNumber);
        for (int i = 0; i < fileNumber; i++) {
            dataFiles.add(dataFileSer.apply(in));
        }

        List<DeletionFile> dataDeletionFiles = DeletionFile.deserializeList(in);

        boolean isStreaming = in.readBoolean();
        boolean rawConvertible = in.readBoolean();

        DataSplit.Builder builder =
                builder()
                        .withSnapshot(snapshotId)
                        .withPartition(partition)
                        .withBucket(bucket)
                        .withBucketPath(bucketPath)
                        .withBeforeFiles(beforeFiles)
                        .withDataFiles(dataFiles)
                        .isStreaming(isStreaming)
                        .rawConvertible(rawConvertible);

        if (beforeDeletionFiles != null) {
            builder.withBeforeDeletionFiles(beforeDeletionFiles);
        }
        if (dataDeletionFiles != null) {
            builder.withDataDeletionFiles(dataDeletionFiles);
        }
        return builder.build();
    }

    /** 根据版本号选择对应的数据文件元数据序列化和反序列化工具。 */
    private static FunctionWithIOException<DataInputView, DataFileMeta> getFileMetaSerde(
            int version) {
        if (version == 1) {
            DataFileMeta08Serializer serializer = new DataFileMeta08Serializer();
            return serializer::deserialize;
        } else if (version == 2) {
            DataFileMetaSerializer serializer = new DataFileMetaSerializer();
            return serializer::deserialize;
        } else {
            throw new UnsupportedOperationException(
                    "Expecting DataSplit version to be smaller or equal than "
                            + VERSION
                            + ", but found "
                            + version
                            + ".");
        }
    }

    /** 构造`DataSplit`对象的构建器（Builder）方法。 */
    public static Builder builder() {
        return new Builder();
    }

    /** `DataSplit`的构建器实现。 */
    public static class Builder {

        private final DataSplit split = new DataSplit();

        /** 设置快照ID。 */
        public Builder withSnapshot(long snapshot) {
            this.split.snapshotId = snapshot;
            return this;
        }

        /** 设置分区信息。 */
        public Builder withPartition(BinaryRow partition) {
            this.split.partition = partition;
            return this;
        }

        /** 设置桶编号。 */
        public Builder withBucket(int bucket) {
            this.split.bucket = bucket;
            return this;
        }

        /** 设置桶路径。 */
        public Builder withBucketPath(String bucketPath) {
            this.split.bucketPath = bucketPath;
            return this;
        }

        /** 设置前置数据文件列表。 */
        public Builder withBeforeFiles(List<DataFileMeta> beforeFiles) {
            this.split.beforeFiles = new ArrayList<>(beforeFiles);
            return this;
        }

        /** 设置前置删除文件列表。 */
        public Builder withBeforeDeletionFiles(List<DeletionFile> beforeDeletionFiles) {
            this.split.beforeDeletionFiles = new ArrayList<>(beforeDeletionFiles);
            return this;
        }

        /** 设置数据文件列表。 */
        public Builder withDataFiles(List<DataFileMeta> dataFiles) {
            this.split.dataFiles = new ArrayList<>(dataFiles);
            return this;
        }

        /** 设置数据删除文件列表。 */
        public Builder withDataDeletionFiles(List<DeletionFile> dataDeletionFiles) {
            this.split.dataDeletionFiles = new ArrayList<>(dataDeletionFiles);
            return this;
        }

        /** 设置是否为流式数据。 */
        public Builder isStreaming(boolean isStreaming) {
            this.split.isStreaming = isStreaming;
            return this;
        }

        /** 设置是否可转换为原始文件格式。 */
        public Builder rawConvertible(boolean rawConvertible) {
            this.split.rawConvertible = rawConvertible;
            return this;
        }

        /** 构建`DataSplit`对象。 */
        public DataSplit build() {
            checkArgument(split.partition != null);
            checkArgument(split.bucket != -1);
            checkArgument(split.bucketPath != null);
            checkArgument(split.dataFiles != null);

            DataSplit split = new DataSplit();
            split.assign(this.split);
            return split;
        }
    }
}
