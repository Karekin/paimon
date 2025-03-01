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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.apache.paimon.CoreOptions.SCAN_BOUNDED_WATERMARK;
import static org.apache.paimon.CoreOptions.STREAM_SCAN_MODE;
import static org.apache.paimon.CoreOptions.StreamScanMode.FILE_MONITOR;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

// -------------------------
//  文件变更监测表实现类
// -------------------------

/**
 * 文件变更监测表实现类，用于生成文件变更的快照。
 */
@Experimental
public class FileMonitorTable implements DataTable, ReadonlyTable {

    private static final long serialVersionUID = 1L;

    // 包装的文件存储表对象
    private final FileStoreTable wrapped;

    // 定义行类型，包含快照 ID、分区、桶、变更前文件和数据文件信息
    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                            new BigIntType(false),
                            newBytesType(false),
                            new IntType(false),
                            newBytesType(false),
                            newBytesType(false)
                    },
                    new String[] {
                            "_SNAPSHOT_ID", "_PARTITION", "_BUCKET", "_BEFORE_FILES", "_DATA_FILES"
                    });

    public FileMonitorTable(FileStoreTable wrapped) {
        // 初始化包装的文件存储表
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(STREAM_SCAN_MODE.key(), FILE_MONITOR.getValue());
        dynamicOptions.put(SCAN_BOUNDED_WATERMARK.key(), null);
        this.wrapped = wrapped.copy(dynamicOptions);
    }

    @Override
    public OptionalLong latestSnapshotId() {
        // 获取最新的快照 ID
        return wrapped.latestSnapshotId();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        // 获取指定快照 ID 的快照
        return wrapped.snapshot(snapshotId);
    }

    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        // 获取清单文件元数据列表的读取器
        return wrapped.manifestListReader();
    }

    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        // 获取清单文件入口的读取器
        return wrapped.manifestFileReader();
    }

    @Override
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        // 获取索引清单文件入口的读取器
        return wrapped.indexManifestFileReader();
    }

    @Override
    public Path location() {
        // 获取文件存储表的位置
        return wrapped.location();
    }

    @Override
    public SnapshotManager snapshotManager() {
        // 获取快照管理器
        return wrapped.snapshotManager();
    }

    @Override
    public TagManager tagManager() {
        // 获取标签管理器
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        // 获取分支管理器
        return wrapped.branchManager();
    }

    @Override
    public DataTable switchToBranch(String branchName) {
        // 切换到指定分支的表
        return new FileMonitorTable(wrapped.switchToBranch(branchName));
    }

    @Override
    public String name() {
        // 获取表的名称
        return "__internal_file_monitor_" + wrapped.location().getName();
    }

    @Override
    public RowType rowType() {
        // 获取表的行类型
        return ROW_TYPE;
    }

    @Override
    public Map<String, String> options() {
        // 获取表的选项
        return wrapped.options();
    }

    @Override
    public List<String> primaryKeys() {
        // 当前表无主键
        return Collections.emptyList();
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        // 创建新的快照读取器
        return wrapped.newSnapshotReader();
    }

    @Override
    public DataTableScan newScan() {
        // 创建新的数据表扫描器
        return wrapped.newScan();
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        // 创建新的流式数据表扫描器
        return wrapped.newStreamScan();
    }

    @Override
    public CoreOptions coreOptions() {
        // 获取核心选项
        return wrapped.coreOptions();
    }

    @Override
    public InnerTableRead newRead() {
        // 创建新的内部表读取器
        return new BucketsRead();
    }

    @Override
    public FileMonitorTable copy(Map<String, String> dynamicOptions) {
        // 复制文件监测表
        return new FileMonitorTable(wrapped.copy(dynamicOptions));
    }

    @Override
    public FileIO fileIO() {
        // 获取文件输入输出对象
        return wrapped.fileIO();
    }

    public static RowType getRowType() {
        // 获取行类型
        return ROW_TYPE;
    }

    private static class BucketsRead implements InnerTableRead {

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            // 过滤操作由扫描器完成，这里直接返回当前实例
            return this;
        }

        @Override
        public InnerTableRead withProjection(int[][] projection) {
            // 不支持投影操作
            throw new UnsupportedOperationException("BucketsRead does not support projection");
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            // 使用当前实例作为表读取器
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            // 根据分片创建记录读取器
            if (!(split instanceof DataSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }

            DataSplit dataSplit = (DataSplit) split;

            // 创建文件变更对象
            FileChange change =
                    new FileChange(
                            dataSplit.snapshotId(),
                            dataSplit.partition(),
                            dataSplit.bucket(),
                            dataSplit.beforeFiles(),
                            dataSplit.dataFiles());

            // 返回包含文件变更记录的记录读取器
            return new IteratorRecordReader<>(
                    Collections.singletonList(toRow(change)).iterator());
        }
    }

    public static InternalRow toRow(FileChange change) throws IOException {
        // 将文件变更对象转换为内部行对象
        DataFileMetaSerializer fileSerializer = new DataFileMetaSerializer();
        return GenericRow.of(
                change.snapshotId(),
                serializeBinaryRow(change.partition()),
                change.bucket(),
                fileSerializer.serializeList(change.beforeFiles()),
                fileSerializer.serializeList(change.dataFiles()));
    }

    public static FileChange toFileChange(InternalRow row) throws IOException {
        // 将内部行对象转换为文件变更对象
        DataFileMetaSerializer fileSerializer = new DataFileMetaSerializer();
        return new FileChange(
                row.getLong(0),
                deserializeBinaryRow(row.getBinary(1)),
                row.getInt(2),
                fileSerializer.deserializeList(row.getBinary(3)),
                fileSerializer.deserializeList(row.getBinary(4)));
    }

    /** 文件变更记录的 POJO 类。 */
    public static class FileChange {

        private final long snapshotId; // 快照 ID
        private final BinaryRow partition; // 分区
        private final int bucket; // 桶编号
        private final List<DataFileMeta> beforeFiles; // 变更前的文件元数据列表
        private final List<DataFileMeta> dataFiles; // 数据文件元数据列表

        public FileChange(
                long snapshotId,
                BinaryRow partition,
                int bucket,
                List<DataFileMeta> beforeFiles,
                List<DataFileMeta> dataFiles) {
            this.snapshotId = snapshotId;
            this.partition = partition;
            this.bucket = bucket;
            this.beforeFiles = beforeFiles;
            this.dataFiles = dataFiles;
        }

        public long snapshotId() {
            return snapshotId;
        }

        public BinaryRow partition() {
            return partition;
        }

        public int bucket() {
            return bucket;
        }

        public List<DataFileMeta> beforeFiles() {
            return beforeFiles;
        }

        public List<DataFileMeta> dataFiles() {
            return dataFiles;
        }

        @Override
        public String toString() {
            return "FileChange{"
                    + "snapshotId="
                    + snapshotId
                    + ", partition="
                    + partition
                    + ", bucket="
                    + bucket
                    + ", beforeFiles="
                    + beforeFiles
                    + ", dataFiles="
                    + dataFiles
                    + '}';
        }
    }
}
