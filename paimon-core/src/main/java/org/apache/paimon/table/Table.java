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

package org.apache.paimon.table;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SimpleFileReader;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * A table provides basic abstraction for table type and table scan and table read.
 *
 * @since 0.4.0
 */
/**
* 
* 
* 
* 表为表类型、表扫描和表读取提供了基本的抽象。
*/
@Public
public interface Table extends Serializable {

    // ================== Table Metadata =====================

    /** A name to identify this table. */
    /**
    * 
    * 
    * 
    * 用于标识此表的名称。
    */
    String name();

    default String fullName() {
        return name();
    }

    /** Returns the row type of this table. */
    /**
     * 
     * 
     * 
     * 返回此表的行类型。
     */
    RowType rowType();

    /** Partition keys of this table. */
    /**
     * 
     * 
     * 
     * 此表的分区键。
     */
    List<String> partitionKeys();

    /** Primary keys of this table. */
    /**
     * 
     * 
     * 
     * 此表的主键。
     */
    List<String> primaryKeys();

    /** Options of this table. */
    /**
     * 
     * 
     * 
     * 此表的Options。
     */
    Map<String, String> options();

    /** Optional comment of this table. */
    /**
     * 
     * 
     * 
     * 表的comment
     */
    Optional<String> comment();

    /** Optional statistics of this table. */
    /**
     * 
     * 
     * 
     * 此表的可选统计信息。
     */
    @Experimental
    Optional<Statistics> statistics();

    // ================= Table Operations ====================

    /** Copy this table with adding dynamic options. */
    /**
     * 
     * 
     * 
     * 复制此表并添加动态选项。
     */
    Table copy(Map<String, String> dynamicOptions);

    /** Get the latest snapshot id for this table, or empty if there are no snapshots. */
    /**
     * 
     * 
     * 
     * 获取此表的最新快照id，如果没有快照，则为空。
     */
    @Experimental
    OptionalLong latestSnapshotId();

    /** Get the {@link Snapshot} from snapshot id. */
    /**
     * 
     * 
     * 
     * 从快照id获取｛@link Snapshot｝。
     */
    @Experimental
    Snapshot snapshot(long snapshotId);

    /** Reader to read manifest file meta from manifest list file. */
    /**
     * 
     * 
     * 
     * 读取器从清单列表文件读取清单文件元。
     */
    @Experimental
    SimpleFileReader<ManifestFileMeta> manifestListReader();

    /** Reader to read manifest entry from manifest file. */
    /**
     * 
     * 
     * 
     * 读取器从清单文件读取清单条目。
     */
    @Experimental
    SimpleFileReader<ManifestEntry> manifestFileReader();

    /** Reader to read index manifest entry from index manifest file. */
    /**
     * 
     * 
     * 
     * 读取器从索引清单文件读取索引清单条目。
     */
    @Experimental
    SimpleFileReader<IndexManifestEntry> indexManifestFileReader();

    /** Rollback table's state to a specific snapshot. */
    @Experimental
    void rollbackTo(long snapshotId);

    /** Create a tag from given snapshot. */
    @Experimental
    void createTag(String tagName, long fromSnapshotId);

    @Experimental
    void createTag(String tagName, long fromSnapshotId, Duration timeRetained);

    /** Create a tag from latest snapshot. */
    @Experimental
    void createTag(String tagName);

    @Experimental
    void createTag(String tagName, Duration timeRetained);

    /** Delete a tag by name. */
    @Experimental
    void deleteTag(String tagName);

    /** Delete tags, tags are separated by commas. */
    @Experimental
    default void deleteTags(String tagNames) {
        for (String tagName : tagNames.split(",")) {
            deleteTag(tagName);
        }
    }

    /** Rollback table's state to a specific tag. */
    @Experimental
    void rollbackTo(String tagName);

    /** Create an empty branch. */
    @Experimental
    void createBranch(String branchName);

    /** Create a branch from given tag. */
    @Experimental
    void createBranch(String branchName, String tagName);

    /** Delete a branch by branchName. */
    @Experimental
    void deleteBranch(String branchName);

    /** Delete branches, branches are separated by commas. */
    @Experimental
    default void deleteBranches(String branchNames) {
        for (String branch : branchNames.split(",")) {
            deleteBranch(branch);
        }
    }

    /** Merge a branch to main branch. */
    @Experimental
    void fastForward(String branchName);

    /** Manually expire snapshots, parameters can be controlled independently of table options. */
    @Experimental
    ExpireSnapshots newExpireSnapshots();

    @Experimental
    ExpireSnapshots newExpireChangelog();

    // =============== Read & Write Operations ==================

    /** Returns a new read builder. */
    ReadBuilder newReadBuilder();

    /** Returns a new batch write builder. */
    BatchWriteBuilder newBatchWriteBuilder();

    /** Returns a new stream write builder. */
    StreamWriteBuilder newStreamWriteBuilder();
}
