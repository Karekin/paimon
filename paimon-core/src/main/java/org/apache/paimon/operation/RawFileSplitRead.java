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

package org.apache.paimon.operation;

import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.ApplyDeletionVectorReader;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.FileIndexSkipper;
import org.apache.paimon.io.FileRecordReader;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.EmptyRecordReader;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BulkFormatMapping;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOExceptionSupplier;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.predicate.PredicateBuilder.excludePredicateWithFields;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;


/**
 * 这是一个实现 {@link SplitRead} 接口的类，用于直接从 {@link DataSplit} 中读取原始文件。
 */
public class RawFileSplitRead implements SplitRead<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(RawFileSplitRead.class);

    // 文件 I/O 组件，用于文件的读写操作
    private final FileIO fileIO;

    // Schema 管理器，用于管理表的 Schema
    private final SchemaManager schemaManager;

    // 表的 Schema，包含表的结构信息
    private final TableSchema schema;

    // 文件格式发现器，用于根据文件格式选择相应的读取策略
    private final FileFormatDiscover formatDiscover;

    // 文件存储路径生成器
    private final FileStorePathFactory pathFactory;

    // 存储不同文件格式对应的批量读取映射
    private final Map<FormatKey, RawFileBulkFormatMapping> bulkFormatMappings;

    // 是否启用文件索引读取
    private final boolean fileIndexReadEnabled;

    // 数据投影，用于指定读取数据时的字段范围
    private int[][] projection;

    // 过滤条件，用于过滤读取的数据
    @Nullable
    private List<Predicate> filters;

    // 构造函数，初始化 RawFileSplitRead 对象
    public RawFileSplitRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType rowType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            boolean fileIndexReadEnabled) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.formatDiscover = formatDiscover;
        this.pathFactory = pathFactory;
        this.bulkFormatMappings = new HashMap<>();
        this.fileIndexReadEnabled = fileIndexReadEnabled;

        // 初始化投影为所有字段
        this.projection = Projection.range(0, rowType.getFieldCount()).toNestedIndexes();
    }

    /**
     * 强制保留删除标记的读取器。当前实现直接返回自身。
     *
     * @return 当前对象。
     */
    @Override
    public SplitRead<InternalRow> forceKeepDelete() {
        return this;
    }

    /**
     * 设置 IO 管理器。当前实现直接返回自身。
     *
     * @param ioManager IO 管理器。
     * @return 当前对象。
     */
    @Override
    public SplitRead<InternalRow> withIOManager(@Nullable IOManager ioManager) {
        return this;
    }

    /**
     * 设置读取时的字段投影。
     *
     * @param projectedFields 字段投影。
     * @return 当前对象。
     */
    @Override
    public RawFileSplitRead withProjection(int[][] projectedFields) {
        if (projectedFields != null) {
            projection = projectedFields; // 设置字段投影
        }
        return this;
    }

    /**
     * 设置读取时的过滤条件。
     *
     * @param predicate 过滤条件。
     * @return 当前对象。
     */
    @Override
    public RawFileSplitRead withFilter(Predicate predicate) {
        if (predicate != null) {
            this.filters = splitAnd(predicate); // 分解过滤条件
        }
        return this;
    }

    /**
     * 创建记录读取器，用于从数据分片中读取数据。
     *
     * @param split 数据分片。
     * @return 记录读取器。
     * @throws IOException 如果发生 IO 错误。
     */
    @Override
    public RecordReader<InternalRow> createReader(DataSplit split) throws IOException {
        if (split.beforeFiles().size() > 0) {
            LOG.info("Ignore split before files: {}", split.beforeFiles()); // 忽略前文件信息
        }

        // 获取数据文件和删除向量工厂
        List<DataFileMeta> files = split.dataFiles();
        DeletionVector.Factory dvFactory =
                DeletionVector.factory(fileIO, files, split.deletionFiles().orElse(null));
        List<IOExceptionSupplier<DeletionVector>> dvFactories = new ArrayList<>();
        for (DataFileMeta file : files) {
            dvFactories.add(() -> dvFactory.create(file.fileName()).orElse(null));
        }

        // 创建记录读取器
        return createReader(split.partition(), split.bucket(), split.dataFiles(), dvFactories);
    }

    /**
     * 创建记录读取器，用于从指定的分区、桶和文件列表中读取数据。
     *
     * @param partition   分区信息。
     * @param bucket      桶编号。
     * @param files       数据文件列表。
     * @param dvFactories 删除向量工厂列表。
     * @return 记录读取器。
     * @throws IOException 如果发生 IO 错误。
     */
    public RecordReader<InternalRow> createReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<IOExceptionSupplier<DeletionVector>> dvFactories)
            throws IOException {
        // 创建数据文件路径生成器
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(partition, bucket);
        List<ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();

        for (int i = 0; i < files.size(); i++) {
            DataFileMeta file = files.get(i);
            String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName()); // 获取文件格式标识符
            RawFileBulkFormatMapping bulkFormatMapping =
                    bulkFormatMappings.computeIfAbsent(
                            new FormatKey(file.schemaId(), formatIdentifier), // 创建或获取格式映射
                            this::createBulkFormatMapping);

            IOExceptionSupplier<DeletionVector> dvFactory =
                    dvFactories == null ? null : dvFactories.get(i); // 获取删除向量工厂
            suppliers.add(
                    () ->
                            createFileReader(
                                    partition,
                                    file,
                                    dataFilePathFactory,
                                    bulkFormatMapping,
                                    dvFactory));
        }

        // 返回合并后的记录读取器
        return ConcatRecordReader.create(suppliers);
    }

    /**
     * 创建格式映射对象，用于映射不同的文件格式和 Schema。
     *
     * @param key 格式键，包含 Schema ID 和格式标识符。
     * @return 格式映射对象。
     */
    private RawFileBulkFormatMapping createBulkFormatMapping(FormatKey key) {
        TableSchema tableSchema = schema;
        TableSchema dataSchema = // 获取数据 Schema
                key.schemaId == schema.id() ? schema : schemaManager.schema(key.schemaId);

        // 创建数据投影
        int[][] dataProjection =
                SchemaEvolutionUtil.createDataProjection(
                        tableSchema.fields(), dataSchema.fields(), projection);

        // 创建索引转换映射
        IndexCastMapping indexCastMapping =
                SchemaEvolutionUtil.createIndexCastMapping(
                        Projection.of(projection).toTopLevelIndexes(),
                        tableSchema.fields(),
                        Projection.of(dataProjection).toTopLevelIndexes(),
                        dataSchema.fields());

        // 创建数据过滤条件
        List<Predicate> dataFilters =
                this.schema.id() == key.schemaId
                        ? filters
                        : SchemaEvolutionUtil.createDataFilters(
                        tableSchema.fields(), dataSchema.fields(), filters);
        List<Predicate> nonPartitionFilters =
                excludePredicateWithFields(dataFilters, new HashSet<>(dataSchema.partitionKeys()));

        // 处理分区字段的投影
        Pair<int[], RowType> partitionPair = null;
        if (!dataSchema.partitionKeys().isEmpty()) {
            Pair<int[], int[][]> partitionMapping =
                    PartitionUtils.constructPartitionMapping(dataSchema, dataProjection);
            if (partitionMapping != null) {
                dataProjection = partitionMapping.getRight();
                partitionPair =
                        Pair.of(
                                partitionMapping.getLeft(),
                                dataSchema.projectedLogicalRowType(dataSchema.partitionKeys()));
            }
        }

        // 创建投影后的行类型
        RowType projectedRowType =
                Projection.of(dataProjection).project(dataSchema.logicalRowType());

        // 返回格式映射对象
        return new RawFileBulkFormatMapping(
                indexCastMapping.getIndexMapping(),
                indexCastMapping.getCastMapping(),
                partitionPair,
                formatDiscover.discover(key.format).createReaderFactory(projectedRowType, nonPartitionFilters),
                dataSchema,
                dataFilters);
    }

    /**
     * 创建文件读取器，用于从指定的文件中读取数据。
     *
     * @param partition           分区信息。
     * @param file                数据文件。
     * @param dataFilePathFactory 数据文件路径生成器。
     * @param bulkFormatMapping   格式映射对象。
     * @param dvFactory           删除向量工厂。
     * @return 文件读取器。
     * @throws IOException 如果发生 IO 错误。
     */
    private RecordReader<InternalRow> createFileReader(
            BinaryRow partition,
            DataFileMeta file,
            DataFilePathFactory dataFilePathFactory,
            RawFileBulkFormatMapping bulkFormatMapping,
            IOExceptionSupplier<DeletionVector> dvFactory)
            throws IOException {
        if (fileIndexReadEnabled) {
            boolean skip =
                    FileIndexSkipper.skip( // 判断是否跳过文件
                            fileIO,
                            bulkFormatMapping.getDataSchema(),
                            bulkFormatMapping.getDataFilters(),
                            dataFilePathFactory,
                            file);
            if (skip) {
                return new EmptyRecordReader<>(); // 如果跳过，返回空读取器
            }
        }

        // 创建文件记录读取器
        FileRecordReader fileRecordReader =
                new FileRecordReader(
                        bulkFormatMapping.getReaderFactory(),
                        new FormatReaderContext(
                                fileIO,
                                dataFilePathFactory.toPath(file.fileName()),
                                file.fileSize()),
                        bulkFormatMapping.getIndexMapping(),
                        bulkFormatMapping.getCastMapping(),
                        PartitionUtils.create(bulkFormatMapping.getPartitionPair(), partition));

        DeletionVector deletionVector = dvFactory == null ? null : dvFactory.get(); // 获取删除向量
        if (deletionVector != null && !deletionVector.isEmpty()) {
            return new ApplyDeletionVectorReader(fileRecordReader, deletionVector); // 使用删除向量读取器
        }
        return fileRecordReader; // 直接返回文件记录读取器
    }

    /**
     * 批量格式映射，包含数据 Schema 和过滤条件。
     */
    private static class RawFileBulkFormatMapping extends BulkFormatMapping {

        private final TableSchema dataSchema;
        private final List<Predicate> dataFilters;

        public RawFileBulkFormatMapping(
                int[] indexMapping,
                CastFieldGetter[] castMapping,
                Pair<int[], RowType> partitionPair,
                FormatReaderFactory bulkFormat,
                TableSchema dataSchema,
                List<Predicate> dataFilters) {
            super(indexMapping, castMapping, partitionPair, bulkFormat);
            this.dataSchema = dataSchema;
            this.dataFilters = dataFilters;
        }

        public TableSchema getDataSchema() {
            return dataSchema;
        }

        public List<Predicate> getDataFilters() {
            return dataFilters;
        }
    }
}
