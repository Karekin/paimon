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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/**
 * DataFileMeta 表示一个数据文件的元数据信息，用于在 Paimon 中描述文件的各种属性。
 *
 * <p>主要包含以下信息：
 * <ul>
 *     <li>文件名、文件大小、行数</li>
 *     <li>文件的键区间（minKey 和 maxKey）</li>
 *     <li>文件中存储的 keyStats 和 valueStats 等统计信息</li>
 *     <li>文件写入时的最小/最大序列号（minSequenceNumber 和 maxSequenceNumber）</li>
 *     <li>文件所属的 schemaId、level（用于多层合并）等</li>
 *     <li>一些可选属性，如额外文件列表 extraFiles、内嵌索引 embeddedIndex、文件来源 fileSource 等</li>
 * </ul>
 *
 * @since 0.9.0
 */
@Public
public class DataFileMeta {

    /**
     * 数据文件的字段模式，用于描述该文件的元数据结构，比如文件名、大小、最小/最大Key等。
     */
    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_FILE_NAME", newStringType(false)),
                            new DataField(1, "_FILE_SIZE", new BigIntType(false)),
                            new DataField(2, "_ROW_COUNT", new BigIntType(false)),
                            new DataField(3, "_MIN_KEY", newBytesType(false)),
                            new DataField(4, "_MAX_KEY", newBytesType(false)),
                            new DataField(5, "_KEY_STATS", SimpleStats.SCHEMA),
                            new DataField(6, "_VALUE_STATS", SimpleStats.SCHEMA),
                            new DataField(7, "_MIN_SEQUENCE_NUMBER", new BigIntType(false)),
                            new DataField(8, "_MAX_SEQUENCE_NUMBER", new BigIntType(false)),
                            new DataField(9, "_SCHEMA_ID", new BigIntType(false)),
                            new DataField(10, "_LEVEL", new IntType(false)),
                            new DataField(
                                    11, "_EXTRA_FILES", new ArrayType(false, newStringType(false))),
                            new DataField(12, "_CREATION_TIME", DataTypes.TIMESTAMP_MILLIS()),
                            new DataField(13, "_DELETE_ROW_COUNT", new BigIntType(true)),
                            new DataField(14, "_EMBEDDED_FILE_INDEX", newBytesType(true)),
                            new DataField(15, "_FILE_SOURCE", new TinyIntType(true))));

    /** 空的 minKey、maxKey。表示该文件没有显式的键区间（或用于append场景等）。 */
    public static final BinaryRow EMPTY_MIN_KEY = EMPTY_ROW;
    public static final BinaryRow EMPTY_MAX_KEY = EMPTY_ROW;

    /** 文件的初始层级（level），默认为 0。 */
    public static final int DUMMY_LEVEL = 0;

    // ================================
    //  以下为 DataFileMeta 的成员变量
    // ================================

    /** 文件名。通常是实际存储在文件系统中的名称，如 "data-xxx-1.parquet"。 */
    private final String fileName;

    /** 文件大小，单位为字节（bytes）。 */
    private final long fileSize;

    /**
     * 该文件的总行数，包含新增和删除行（如果有）。
     * 对于事务表/增量表而言，可能存在 "delete" 行。
     */
    private final long rowCount;

    /** 文件键区间的最小值（minKey）。 */
    private final BinaryRow minKey;

    /** 文件键区间的最大值（maxKey）。 */
    private final BinaryRow maxKey;

    /** keyStats 用于描述键相关的统计信息（如某些列的最小值、最大值、空值计数等）。 */
    private final SimpleStats keyStats;

    /** valueStats 用于描述值相关的统计信息。 */
    private final SimpleStats valueStats;

    /** 写入该文件时的最小序列号（通常用来区分增量、版本等）。 */
    private final long minSequenceNumber;

    /** 写入该文件时的最大序列号。 */
    private final long maxSequenceNumber;

    /** 文件所属的 schemaId，用于识别文件对应的表结构版本。 */
    private final long schemaId;

    /**
     * 文件层级（level）。
     * 在多层合并存储（如 MergeTree）中，文件会有不同的 level 表示是否已被合并过。
     */
    private final int level;

    /**
     * 额外文件列表。
     * 在某些场景下（例如旧版本的 changelog 文件、索引文件等），可能会引用额外文件。
     */
    private final List<String> extraFiles;

    /** 文件的创建时间（存储为 Timestamp），表示写入该文件的时刻。 */
    private final Timestamp creationTime;

    /**
     * 文件中的删除行数（可选）。
     * 因为之前的版本只存储 rowCount，所以为了兼容性这里用 @Nullable。
     * 如果非空，则 rowCount = addRowCount + deleteRowCount。
     */
    private final @Nullable Long deleteRowCount;

    /**
     * 内嵌的索引数据（可选）。如果索引体量较小，可以直接存储在这个字节数组里。
     */
    private final @Nullable byte[] embeddedIndex;

    /**
     * 文件来源（可选）。
     * 可能用于标识该文件是由何种来源生成的（不同 Source、不同写入方式等）。
     */
    private final @Nullable FileSource fileSource;

    // ================================
    //  以下为静态方法，用于快速创建或加载 DataFileMeta
    // ================================

    /**
     * 用于追加场景（append），创建一个 DataFileMeta。
     * 其中 minKey, maxKey 默认为 EMPTY_ROW，
     * keyStats 默认为 EMPTY_STATS，level 默认为 0。
     *
     * @param fileName           文件名
     * @param fileSize           文件大小
     * @param rowCount           行数
     * @param rowStats           行相关的统计信息（通常作为 valueStats）
     * @param minSequenceNumber  最小序列号
     * @param maxSequenceNumber  最大序列号
     * @param schemaId           schema ID
     * @param fileSource         文件来源（可选）
     */
    public static DataFileMeta forAppend(
            String fileName,
            long fileSize,
            long rowCount,
            SimpleStats rowStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            @Nullable FileSource fileSource) {
        return forAppend(
                fileName,
                fileSize,
                rowCount,
                rowStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                Collections.emptyList(),
                null,
                fileSource);
    }

    /**
     * 同上，但可额外指定 extraFiles、embeddedIndex。
     *
     * @param fileName           文件名
     * @param fileSize           文件大小
     * @param rowCount           行数
     * @param rowStats           行相关统计信息
     * @param minSequenceNumber  最小序列号
     * @param maxSequenceNumber  最大序列号
     * @param schemaId           schema ID
     * @param extraFiles         额外文件列表
     * @param embeddedIndex      内嵌索引数据
     * @param fileSource         文件来源
     */
    public static DataFileMeta forAppend(
            String fileName,
            long fileSize,
            long rowCount,
            SimpleStats rowStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            List<String> extraFiles,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource) {
        return new DataFileMeta(
                fileName,
                fileSize,
                rowCount,
                EMPTY_MIN_KEY,
                EMPTY_MAX_KEY,
                EMPTY_STATS,
                rowStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                DUMMY_LEVEL,
                extraFiles,
                Timestamp.fromLocalDateTime(LocalDateTime.now()).toMillisTimestamp(),
                0L,
                embeddedIndex,
                fileSource);
    }

    /**
     * 构造函数：可自定义字段较多。
     *
     * @param fileName           文件名
     * @param fileSize           文件大小
     * @param rowCount           行数
     * @param minKey             最小键
     * @param maxKey             最大键
     * @param keyStats           key 的统计信息
     * @param valueStats         value 的统计信息
     * @param minSequenceNumber  最小序列号
     * @param maxSequenceNumber  最大序列号
     * @param schemaId           schema ID
     * @param level              文件层级
     * @param deleteRowCount     删除行数（可空）
     * @param embeddedIndex      内嵌索引
     * @param fileSource         文件来源
     */
    public DataFileMeta(
            String fileName,
            long fileSize,
            long rowCount,
            BinaryRow minKey,
            BinaryRow maxKey,
            SimpleStats keyStats,
            SimpleStats valueStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            int level,
            @Nullable Long deleteRowCount,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource) {
        this(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                Collections.emptyList(),
                // 默认 creationTime 为当前时间
                Timestamp.fromLocalDateTime(LocalDateTime.now()).toMillisTimestamp(),
                deleteRowCount,
                embeddedIndex,
                fileSource);
    }

    /**
     * 完整构造函数，可指定 extraFiles、creationTime 等所有字段。
     *
     * @param fileName           文件名
     * @param fileSize           文件大小
     * @param rowCount           行数
     * @param minKey             最小键
     * @param maxKey             最大键
     * @param keyStats           key 的统计信息
     * @param valueStats         value 的统计信息
     * @param minSequenceNumber  最小序列号
     * @param maxSequenceNumber  最大序列号
     * @param schemaId           schema ID
     * @param level              文件层级
     * @param extraFiles         额外文件列表
     * @param creationTime       创建时间
     * @param deleteRowCount     删除行数（可空）
     * @param embeddedIndex      内嵌索引
     * @param fileSource         文件来源
     */
    public DataFileMeta(
            String fileName,
            long fileSize,
            long rowCount,
            BinaryRow minKey,
            BinaryRow maxKey,
            SimpleStats keyStats,
            SimpleStats valueStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            int level,
            List<String> extraFiles,
            Timestamp creationTime,
            @Nullable Long deleteRowCount,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.rowCount = rowCount;
        this.embeddedIndex = embeddedIndex;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyStats = keyStats;
        this.valueStats = valueStats;

        this.minSequenceNumber = minSequenceNumber;
        this.maxSequenceNumber = maxSequenceNumber;
        this.level = level;
        this.schemaId = schemaId;
        // 不可变的 extraFiles 列表
        this.extraFiles = Collections.unmodifiableList(extraFiles);
        this.creationTime = creationTime;
        this.deleteRowCount = deleteRowCount;
        this.fileSource = fileSource;
    }

    // ==============
    // Getter 方法
    // ==============

    /** 获取文件名。 */
    public String fileName() {
        return fileName;
    }

    /** 获取文件大小（单位：字节）。 */
    public long fileSize() {
        return fileSize;
    }

    /** 获取该文件包含的行数（rowCount = addRowCount + deleteRowCount）。 */
    public long rowCount() {
        return rowCount;
    }

    /**
     * 获取该文件中的 "新增行" 行数（可选）。
     * 如果 deleteRowCount 存在，则 addRowCount = rowCount - deleteRowCount。
     */
    public Optional<Long> addRowCount() {
        return Optional.ofNullable(deleteRowCount).map(c -> rowCount - c);
    }

    /** 获取该文件中的 "删除行" 行数（可选）。 */
    public Optional<Long> deleteRowCount() {
        return Optional.ofNullable(deleteRowCount);
    }

    /** 获取内嵌索引。 */
    public byte[] embeddedIndex() {
        return embeddedIndex;
    }

    /** 获取 minKey。 */
    public BinaryRow minKey() {
        return minKey;
    }

    /** 获取 maxKey。 */
    public BinaryRow maxKey() {
        return maxKey;
    }

    /** 获取 keyStats。 */
    public SimpleStats keyStats() {
        return keyStats;
    }

    /** 获取 valueStats。 */
    public SimpleStats valueStats() {
        return valueStats;
    }

    /** 获取最小序列号。 */
    public long minSequenceNumber() {
        return minSequenceNumber;
    }

    /** 获取最大序列号。 */
    public long maxSequenceNumber() {
        return maxSequenceNumber;
    }

    /** 获取文件所属的 schemaId。 */
    public long schemaId() {
        return schemaId;
    }

    /** 获取文件层级。 */
    public int level() {
        return level;
    }

    /**
     * 获取额外文件列表。
     * <p>在 Paimon 0.2 之前版本中，用于存储 {@link CoreOptions.ChangelogProducer#INPUT} 生成的 changelog 文件；
     * 后续在 0.3 中移到了 {@link DataIncrement}。
     */
    public List<String> extraFiles() {
        return extraFiles;
    }

    /** 获取创建时间（Timestamp）。 */
    public Timestamp creationTime() {
        return creationTime;
    }

    /**
     * 获取创建时间对应的 epoch 毫秒值（long）。
     */
    public long creationTimeEpochMillis() {
        return creationTime
                .toLocalDateTime()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    /**
     * 尝试从文件名中解析出文件格式，例如 "parquet" 等。
     *
     * @return 返回文件名最后一个 '.' 之后的字符串
     */
    public String fileFormat() {
        String[] split = fileName.split("\\.");
        if (split.length == 1) {
            throw new RuntimeException("Can't find format from file: " + fileName());
        }
        return split[split.length - 1];
    }

    /** 获取文件来源（可选）。 */
    public Optional<FileSource> fileSource() {
        return Optional.ofNullable(fileSource);
    }

    // ==============
    // 其他操作方法
    // ==============

    /**
     * 将当前文件升级到更高 level。通常在合并后，level 会增加。
     *
     * @param newLevel 新的文件层级
     * @return 返回升级后的 DataFileMeta
     */
    public DataFileMeta upgrade(int newLevel) {
        checkArgument(newLevel > this.level);
        return new DataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                newLevel,
                extraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource);
    }

    /**
     * 重命名文件（但保持其他元数据信息不变）。
     *
     * @param newFileName 新的文件名
     * @return 返回重命名后的 DataFileMeta
     */
    public DataFileMeta rename(String newFileName) {
        return new DataFileMeta(
                newFileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource);
    }

    /**
     * 根据该元数据收集所有实际存在的文件路径（包含主文件和 extraFiles）。
     *
     * @param pathFactory 用于将文件名转换成 Path 的工厂
     * @return 文件路径列表
     */
    public List<Path> collectFiles(DataFilePathFactory pathFactory) {
        List<Path> paths = new ArrayList<>();
        paths.add(pathFactory.toPath(fileName));
        extraFiles.forEach(f -> paths.add(pathFactory.toPath(f)));
        return paths;
    }

    /**
     * 拷贝当前对象，并替换 extraFiles。
     *
     * @param newExtraFiles 新的 extraFiles
     * @return 拷贝后的 DataFileMeta
     */
    public DataFileMeta copy(List<String> newExtraFiles) {
        return new DataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                newExtraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource);
    }

    /**
     * 拷贝当前对象，并替换 embeddedIndex。
     *
     * @param newEmbeddedIndex 新的内嵌索引字节数组
     * @return 拷贝后的 DataFileMeta
     */
    public DataFileMeta copy(byte[] newEmbeddedIndex) {
        return new DataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                newEmbeddedIndex,
                fileSource);
    }

    // ==============
    // equals / hashCode / toString
    // ==============

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DataFileMeta)) {
            return false;
        }
        DataFileMeta that = (DataFileMeta) o;
        return Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && rowCount == that.rowCount
                && Arrays.equals(embeddedIndex, that.embeddedIndex)
                && Objects.equals(minKey, that.minKey)
                && Objects.equals(maxKey, that.maxKey)
                && Objects.equals(keyStats, that.keyStats)
                && Objects.equals(valueStats, that.valueStats)
                && minSequenceNumber == that.minSequenceNumber
                && maxSequenceNumber == that.maxSequenceNumber
                && schemaId == that.schemaId
                && level == that.level
                && Objects.equals(extraFiles, that.extraFiles)
                && Objects.equals(creationTime, that.creationTime)
                && Objects.equals(deleteRowCount, that.deleteRowCount)
                && Objects.equals(fileSource, that.fileSource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName,
                fileSize,
                rowCount,
                Arrays.hashCode(embeddedIndex),
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                fileSource);
    }

    @Override
    public String toString() {
        return String.format(
                "{fileName: %s, fileSize: %d, rowCount: %d, embeddedIndex: %s, "
                        + "minKey: %s, maxKey: %s, keyStats: %s, valueStats: %s, "
                        + "minSequenceNumber: %d, maxSequenceNumber: %d, "
                        + "schemaId: %d, level: %d, extraFiles: %s, creationTime: %s, "
                        + "deleteRowCount: %d, fileSource: %s}",
                fileName,
                fileSize,
                rowCount,
                Arrays.toString(embeddedIndex),
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                fileSource);
    }

    /**
     * 获取一组 DataFileMeta 列表中最大（最新）的 maxSequenceNumber。
     *
     * @param fileMetas 文件元数据列表
     * @return 该列表中最大的 maxSequenceNumber
     */
    public static long getMaxSequenceNumber(List<DataFileMeta> fileMetas) {
        return fileMetas.stream()
                .map(DataFileMeta::maxSequenceNumber)
                .max(Long::compare)
                .orElse(-1L);
    }
}

