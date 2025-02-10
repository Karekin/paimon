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

import org.apache.paimon.io.DataFileMeta;

import java.util.Collection;
import java.util.List;

/**
 * 记录写入器（RecordWriter）接口，负责数据写入和处理中间文件（未提交的数据）。
 * 当数据准备提交时，调用 {@link #prepareCommit(boolean)} 方法获取增量文件。
 *
 * @param <T> 要写入的数据类型。
 */
public interface RecordWriter<T> {

    /**
     * 向写入器中添加一条数据记录。
     *
     * @param record 要写入的记录
     * @throws Exception 写入过程中可能抛出的异常
     */
    void write(T record) throws Exception;

    /**
     * 对当前写入器关联的文件执行压缩（compaction）。
     * 需要注意的是，该方法仅提交压缩任务，调用返回时，压缩任务可能尚未完成。
     *
     * @param fullCompaction 是否执行全量压缩（true：执行全量压缩；false：仅执行常规压缩）
     * @throws Exception 压缩过程中可能抛出的异常
     */
    void compact(boolean fullCompaction) throws Exception;

    /**
     * 将新的数据文件添加到内部的 {@link org.apache.paimon.compact.CompactManager} 进行管理。
     *
     * @param files 需要添加的文件列表
     */
    void addNewFiles(List<DataFileMeta> files);

    /**
     * 获取该写入器管理的所有数据文件。
     *
     * @return 当前维护的所有数据文件集合
     */
    Collection<DataFileMeta> dataFiles();

    /**
     * 获取该写入器写入的最大序列号（Sequence Number）。
     *
     * @return 记录的最大序列号
     */
    long maxSequenceNumber();

    /**
     * 准备提交数据，获取当前快照周期内的增量文件。
     *
     * @param waitCompaction 是否等待当前压缩任务完成后再提交
     * @return 本次提交所包含的增量文件
     * @throws Exception 提交准备过程中可能抛出的异常
     */
    CommitIncrement prepareCommit(boolean waitCompaction) throws Exception;

    /**
     * 检查是否有正在进行的压缩任务，或者是否有尚未获取的压缩结果。
     *
     * @return 如果存在正在进行的压缩任务或未获取的压缩结果，则返回 true，否则返回 false。
     */
    boolean isCompacting();

    /**
     * 同步写入器。
     * 文件读写相关的结构是非线程安全的，写入器内部可能存在异步线程，在读取数据前应进行同步。
     *
     * @throws Exception 同步过程中可能抛出的异常
     */
    void sync() throws Exception;

    /**
     * 当数据的 "仅插入" 状态发生变化时调用此方法。
     *
     * @param insertOnly 如果为 true，则所有后续记录都是 {@link org.apache.paimon.types.RowKind#INSERT}，
     *                   并且不会有相同主键的两条记录。
     */
    void withInsertOnly(boolean insertOnly);

    /**
     * 关闭写入器，清理资源。
     * 关闭时会删除所有尚未提交的新生成文件，以保证数据一致性。
     *
     * @throws Exception 关闭过程中可能抛出的异常
     */
    void close() throws Exception;
}

