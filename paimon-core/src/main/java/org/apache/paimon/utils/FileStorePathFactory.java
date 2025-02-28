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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.types.RowType;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 文件存储路径工厂类。
 * 用于生成 {@link Path} 对象，这些路径用于存储文件存储系统中的各种文件。
 * 该工厂类是线程安全的，可以安全地在多线程环境中使用。
 */
@ThreadSafe
public class FileStorePathFactory {

    // 表示桶路径前缀。
    public static final String BUCKET_PATH_PREFIX = "bucket-";

    // 文件存储系统的根路径。
    // 所有生成的路径都是基于这个根路径。
    private final Path root;

    // 文件存储系统的唯一标识符。
    // 用于确保生成的路径是唯一的。
    private final String uuid;

    // 分区计算器。
    // 用于计算分区的字符串表示形式。
    private final InternalRowPartitionComputer partitionComputer;

    // 文件格式标识符。
    // 用于表示文件存储系统的文件格式。
    private final String formatIdentifier;

    // 表示已经生成的日志文件的数量。
    // 用于为日志文件生成唯一的名称。
    private final AtomicInteger manifestFileCount;

    // 表示已经生成的日志列表的数量。
    // 用于为日志列表文件生成唯一的名称。
    private final AtomicInteger manifestListCount;

    // 表示已经生成的索引日志的数量。
    // 用于为索引日志文件生成唯一的名称。
    private final AtomicInteger indexManifestCount;

    // 表示已经生成的索引文件的数量。
    // 用于为索引文件生成唯一的名称。
    private final AtomicInteger indexFileCount;

    // 表示已经生成的统计信息文件的数量。
    // 用于为统计信息文件生成唯一的名称。
    private final AtomicInteger statsFileCount;

    // 构造函数，初始化文件存储路径工厂。
    // 根据分区类型和默认分区值创建分区计算器。
    public FileStorePathFactory(
            Path root, RowType partitionType, String defaultPartValue, String formatIdentifier) {
        this.root = root; // 设置根路径
        this.uuid = UUID.randomUUID().toString(); // 生成唯一标识符

        // 根据分区类型和默认分区值创建分区计算器
        this.partitionComputer = getPartitionComputer(partitionType, defaultPartValue);
        this.formatIdentifier = formatIdentifier; // 设置文件格式标识符

        // 初始化计数器
        this.manifestFileCount = new AtomicInteger(0);
        this.manifestListCount = new AtomicInteger(0);
        this.indexManifestCount = new AtomicInteger(0);
        this.indexFileCount = new AtomicInteger(0);
        this.statsFileCount = new AtomicInteger(0);
    }

    // 获取文件存储系统的根路径。
    // 返回值是一个 {@link Path} 对象，表示文件存储系统的根路径。
    public Path root() {
        return root; // 返回根路径
    }

    // 获取分区计算器。
    // 这个方法是为测试目的而公开的，通常不建议外部调用。
    @VisibleForTesting
    public static InternalRowPartitionComputer getPartitionComputer(
            RowType partitionType, String defaultPartValue) {
        String[] partitionColumns = partitionType.getFieldNames().toArray(new String[0]); // 获取分区列名
        return new InternalRowPartitionComputer(defaultPartValue, partitionType, partitionColumns); // 创建分区计算器
    }

    // 生成一个新的日志文件路径。
    // 返回值是一个 {@link Path} 对象，表示日志文件的路径。
    public Path newManifestFile() {
        return new Path(
                root + "/manifest/manifest-" + uuid + "-" + manifestFileCount.getAndIncrement()); // 生成路径
    }

    // 生成一个新的日志列表文件路径。
    // 返回值是一个 {@link Path} 对象，表示日志列表文件的路径。
    public Path newManifestList() {
        return new Path(
                root
                        + "/manifest/manifest-list-"
                        + uuid
                        + "-"
                        + manifestListCount.getAndIncrement()); // 生成路径
    }

    // 根据日志文件名生成完整的路径。
    // 返回值是一个 {@link Path} 对象，表示日志文件的路径。
    public Path toManifestFilePath(String manifestFileName) {
        return new Path(root + "/manifest/" + manifestFileName); // 生成路径
    }

    // 根据日志列表文件名生成完整的路径。
    // 返回值是一个 {@link Path} 对象，表示日志列表文件的路径。
    public Path toManifestListPath(String manifestListName) {
        return new Path(root + "/manifest/" + manifestListName); // 生成路径
    }

    // 生成数据文件路径工厂。
    // 返回值是一个 {@link DataFilePathFactory} 对象，用于生成数据文件路径。
    public DataFilePathFactory createDataFilePathFactory(BinaryRow partition, int bucket) {
        return new DataFilePathFactory(bucketPath(partition, bucket), formatIdentifier); // 创建路径工厂
    }

    // 根据分区和桶生成桶路径。
    // 返回值是一个 {@link Path} 对象，表示桶路径。
    public Path bucketPath(BinaryRow partition, int bucket) {
        return new Path(root + "/" + relativePartitionAndBucketPath(partition, bucket)); // 生成路径
    }

    // 根据分区和桶生成相对路径。
    // 返回值是一个 {@link Path} 对象，表示相对路径。
    public Path relativePartitionAndBucketPath(BinaryRow partition, int bucket) {
        String partitionPath = getPartitionString(partition); // 获取分区字符串
        String fullPath =
                partitionPath.isEmpty()
                        ? BUCKET_PATH_PREFIX + bucket
                        : partitionPath + "/" + BUCKET_PATH_PREFIX + bucket; // 拼接路径
        return new Path(fullPath); // 返回路径
    }

    /**
     * 获取分区字符串。
     * 注意：这个方法不是线程安全的。
     * 返回值是一个字符串，表示分区的路径。
     */
    public String getPartitionString(BinaryRow partition) {
        return PartitionPathUtils.generatePartitionPath(
                partitionComputer.generatePartValues(
                        Preconditions.checkNotNull(
                                partition, "Partition row data is null. This is unexpected."))); // 生成分区路径
    }

    // 根据分区生成分层分区路径。
    // 返回值是一个列表，包含多个 {@link Path} 对象，表示分层分区路径。
    public List<Path> getHierarchicalPartitionPath(BinaryRow partition) {
        return PartitionPathUtils.generateHierarchicalPartitionPaths(
                        partitionComputer.generatePartValues(
                                Preconditions.checkNotNull(
                                        partition,
                                        "Partition binary row is null. This is unexpected.")))
                .stream()
                .map(p -> new Path(root + "/" + p))
                .collect(Collectors.toList()); // 生成分层路径
    }

    // 获取唯一标识符。
    // 这个方法是为测试目的而公开的，通常不建议外部调用。
    @VisibleForTesting
    public String uuid() {
        return uuid; // 返回唯一标识符
    }

    // 创建日志文件路径工厂。
    // 返回值是一个 {@link PathFactory} 对象，用于生成日志文件路径。
    public PathFactory manifestFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestFile(); // 生成新的日志文件路径
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestFilePath(fileName); // 根据文件名生成日志文件路径
            }
        };
    }

    // 创建日志列表文件路径工厂。
    // 返回值是一个 {@link PathFactory} 对象，用于生成日志列表文件路径。
    public PathFactory manifestListFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestList(); // 生成新的日志列表文件路径
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestListPath(fileName); // 根据文件名生成日志列表文件路径
            }
        };
    }

    // 创建索引日志文件路径工厂。
    // 返回值是一个 {@link PathFactory} 对象，用于生成索引日志文件路径。
    public PathFactory indexManifestFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return new Path(
                        root
                                + "/manifest/index-manifest-"
                                + uuid
                                + "-"
                                + indexManifestCount.getAndIncrement()); // 生成新的索引日志文件路径
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(root + "/manifest/" + fileName); // 根据文件名生成索引日志文件路径
            }
        };
    }

    // 创建索引文件路径工厂。
    // 返回值是一个 {@link PathFactory} 对象，用于生成索引文件路径。
    public PathFactory indexFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return new Path(
                        root + "/index/index-" + uuid + "-" + indexFileCount.getAndIncrement()); // 生成新的索引文件路径
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(root + "/index/" + fileName); // 根据文件名生成索引文件路径
            }
        };
    }

    // 创建统计信息文件路径工厂。
    // 返回值是一个 {@link PathFactory} 对象，用于生成统计信息文件路径。
    public PathFactory statsFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return new Path(
                        root
                                + "/statistics/stats-"
                                + uuid
                                + "-"
                                + statsFileCount.getAndIncrement()); // 生成新的统计信息文件路径
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(root + "/statistics/" + fileName); // 根据文件名生成统计信息文件路径
            }
        };
    }
}
