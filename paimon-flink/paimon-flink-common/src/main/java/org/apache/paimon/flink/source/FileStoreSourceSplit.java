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

package org.apache.paimon.flink.source;

import org.apache.paimon.table.source.Split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Objects;

/**
 * 文件存储的 {@link SourceSplit}。
 */
public class FileStoreSourceSplit implements SourceSplit {

    /** 切片的唯一 ID。在这个数据源的范围内是唯一的。 */
    private final String id;

    /** 切片的内容。 */
    private final Split split;

    /** 需要跳过的记录数。 */
    private final long recordsToSkip;

    /**
     * 构造函数，创建一个文件存储切片。
     *
     * @param id 切片的唯一 ID
     * @param split 切片的内容
     */
    public FileStoreSourceSplit(String id, Split split) {
        this(id, split, 0); // 默认跳过 0 条记录
    }

    /**
     * 构造函数，创建一个文件存储切片，并指定需要跳过的记录数。
     *
     * @param id 切片的唯一 ID
     * @param split 切片的内容
     * @param recordsToSkip 需要跳过的记录数
     */
    public FileStoreSourceSplit(String id, Split split, long recordsToSkip) {
        this.id = id;
        this.split = split;
        this.recordsToSkip = recordsToSkip;
    }

    /**
     * 获取切片的内容。
     *
     * @return 切片的内容
     */
    public Split split() {
        return split;
    }

    /**
     * 获取需要跳过的记录数。
     *
     * @return 需要跳过的记录数
     */
    public long recordsToSkip() {
        return recordsToSkip;
    }

    @Override
    public String splitId() {
        return id;
    }

    /**
     * 创建一个新的切片对象，带有更新后的需要跳过的记录数。
     *
     * @param recordsToSkip 需要跳过的记录数
     * @return 新的切片对象
     */
    public FileStoreSourceSplit updateWithRecordsToSkip(long recordsToSkip) {
        return new FileStoreSourceSplit(id, split, recordsToSkip);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileStoreSourceSplit other = (FileStoreSourceSplit) o;
        return Objects.equals(id, other.id)
                && Objects.equals(this.split, other.split)
                && recordsToSkip == other.recordsToSkip;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, split, recordsToSkip);
    }
}
