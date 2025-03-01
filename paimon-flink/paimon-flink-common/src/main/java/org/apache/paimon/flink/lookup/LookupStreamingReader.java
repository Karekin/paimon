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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.SplitsParallelReadUtil;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FunctionWithIOException;
import org.apache.paimon.utils.TypeUtils;

import org.apache.paimon.shade.guava30.com.google.common.primitives.Ints;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_BOOTSTRAP_PARALLELISM;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/**
 * 流式读取器，用于将数据加载到查找表中。
 * <p>
 * 该类实现了流式读取器，能够从文件存储表中读取数据并将其加载到查找表中。
 */
public class LookupStreamingReader {

    /** 文件存储表实例，用于与文件存储系统交互 */
    private final Table table;

    /** 数据投影，定义了需要读取的列及其顺序 */
    private final int[] projection;

    /** 读取构建器，用于构建读取操作 */
    private final ReadBuilder readBuilder;

    /** 投影谓词，用于过滤投影后的数据 */
    @Nullable private final Predicate projectedPredicate;

    /** 流扫描实例，用于执行流式扫描操作 */
    private final StreamTableScan scan;

    /** 时间旅行相关的配置项，包括快照 ID、时间戳、标签等 */
    private static final List<ConfigOption<?>> TIME_TRAVEL_OPTIONS =
            Arrays.asList(
                    CoreOptions.SCAN_TIMESTAMP_MILLIS,
                    CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
                    CoreOptions.SCAN_SNAPSHOT_ID,
                    CoreOptions.SCAN_TAG_NAME,
                    CoreOptions.SCAN_VERSION);

    /**
     * 构造函数，初始化LookupStreamingReader对象。
     * <p>
     * 该构造函数接收以下几个参数：
     * - table: 文件存储表实例
     * - projection: 数据投影
     * - predicate: 数据过滤谓词
     * - requireCachedBucketIds: 需要缓存的桶 ID 列表
     *
     * @param table 文件存储表
     * @param projection 数据投影
     * @param predicate 数据过滤谓词
     * @param requireCachedBucketIds 需要缓存的桶 ID
     */
    public LookupStreamingReader(
            Table table,
            int[] projection,
            @Nullable Predicate predicate,
            Set<Integer> requireCachedBucketIds) {
        this.table = unsetTimeTravelOptions(table); // 移除时间旅行相关的配置项
        this.projection = projection; // 设置数据投影
        this.readBuilder =
                this.table
                        .newReadBuilder() // 创建读取构建器
                        .withProjection(projection) // 设置投影
                        .withFilter(predicate) // 设置过滤谓词
                        .withBucketFilter(
                                requireCachedBucketIds == null
                                        ? null
                                        : requireCachedBucketIds::contains // 设置桶过滤器
                        );
        scan = readBuilder.newStreamScan(); // 创建流扫描实例

        if (predicate != null) { // 如果存在过滤谓词
            List<String> fieldNames = table.rowType().getFieldNames(); // 获取字段名称列表
            List<String> primaryKeys = table.primaryKeys(); // 获取主键列表

            // 对于主键表，只过滤主键字段，确保数据流是 UPSERT 模式，而非 CHANGELOG 模式
            // 对于非主键表，过滤所有字段
            IntUnaryOperator operator =
                    i -> {
                        int index = Ints.indexOf(projection, i); // 获取字段在投影中的索引
                        boolean safeFilter =
                                primaryKeys.isEmpty() // 如果没有主键
                                        || primaryKeys.contains(fieldNames.get(i)); // 或者字段是主键
                        return safeFilter ? index : -1; // 如果安全过滤，则返回索引，否则返回 -1
                    };

            int[] fieldIdxToProjectionIdx =
                    IntStream.range(0, table.rowType().getFieldCount())
                            .map(operator)
                            .toArray(); // 创建字段索引到投影索引的映射数组

            this.projectedPredicate =
                    transformFieldMapping(predicate, fieldIdxToProjectionIdx).orElse(null); // 转换谓词
        } else {
            this.projectedPredicate = null; // 如果没有过滤谓词，置为 null
        }
    }

    /**
     * 移除文件存储表实例中的时间旅行相关配置。
     * <p>
     * 该方法会创建一个新的表实例，去除其中的所有时间旅行相关配置项。
     *
     * @param origin 原始表实例
     * @return 新的表实例，不包含时间旅行相关配置
     */
    private Table unsetTimeTravelOptions(Table origin) {
        FileStoreTable fileStoreTable = (FileStoreTable) origin; // 获取文件存储表实例
        Map<String, String> newOptions = new HashMap<>(fileStoreTable.options()); // 复制选项配置
        // 移除时间旅行相关的配置项
        TIME_TRAVEL_OPTIONS.stream().map(ConfigOption::key).forEach(newOptions::remove);

        CoreOptions.StartupMode startupMode = CoreOptions.fromMap(newOptions).startupMode(); // 获取启动模式
        if (startupMode != CoreOptions.StartupMode.COMPACTED_FULL) {
            startupMode = CoreOptions.StartupMode.LATEST_FULL; // 设置为最新全量模式
        }
        newOptions.put(CoreOptions.SCAN_MODE.key(), startupMode.toString()); // 更新扫描模式选项

        TableSchema newSchema = fileStoreTable.schema().copy(newOptions); // 创建新的表模式
        return fileStoreTable.copy(newSchema); // 返回新的表实例
    }

    /**
     * 获取下一批数据的记录读取器。
     * <p>
     * 该方法会根据分片情况，创建并返回记录读取器。
     * 如果启用并行度，则使用并行执行方式创建读取器；否则，使用串联方式创建读取器。
     *
     * @param useParallelism 是否启用并行度
     * @return 记录读取器
     * @throws Exception 如果操作过程中出现异常
     */
    public RecordReader<InternalRow> nextBatch(boolean useParallelism) throws Exception {
        List<Split> splits = scan.plan().splits(); // 获取分片列表
        CoreOptions options = CoreOptions.fromMap(table.options()); // 获取核心选项
        FunctionWithIOException<Split, RecordReader<InternalRow>> readerSupplier =
                split -> readBuilder.newRead().createReader(split); // 创建读取器的供应商

        RowType readType = TypeUtils.project(table.rowType(), projection); // 获取读取的行类型

        RecordReader<InternalRow> reader;
        if (useParallelism) { // 如果启用并行度
            reader =
                    SplitsParallelReadUtil.parallelExecute( // 并行执行读取
                            readType,
                            readerSupplier,
                            splits,
                            options.pageSize(), // 页面大小
                            new Options(table.options()).get(LOOKUP_BOOTSTRAP_PARALLELISM) // 并行度
                    );
        } else {
            List<ReaderSupplier<InternalRow>> readers = new ArrayList<>(); // 读取器列表
            for (Split split : splits) { // 遍历分片
                readers.add(() -> readerSupplier.apply(split)); // 添加读取器供应商
            }
            reader = ConcatRecordReader.create(readers); // 创建串联读取器
        }

        if (projectedPredicate != null) { // 如果存在投影谓词
            reader = reader.filter(projectedPredicate::test); // 过滤数据
        }
        return reader; // 返回记录读取器
    }

    /**
     * 获取下一个快照 ID。
     *
     * @return 下一个快照 ID，如果不存在则返回 null
     */
    @Nullable
    public Long nextSnapshotId() {
        return scan.checkpoint(); // 返回下一个检查点（快照 ID）
    }
}
