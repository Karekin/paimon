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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 表读取的抽象层，用于提供从切片中读取 {@link InternalRow} 记录的功能。
 *
 * @since 0.4.0
 */
@Public
public interface TableRead {

    /**
     * 执行过滤操作。
     *
     * @return 返回执行过滤后的表读取对象
     */
    TableRead executeFilter();

    /**
     * 设置 I/O 管理器。
     *
     * @param ioManager I/O 管理器
     * @return 返回带有 I/O 管理器的表读取对象
     */
    TableRead withIOManager(IOManager ioManager);

    /**
     * 创建一个记录读取器，用于读取指定切片中的记录。
     *
     * @param split 切片
     * @return 返回记录读取器
     * @throws IOException 如果发生 I/O 错误
     */
    RecordReader<InternalRow> createReader(Split split) throws IOException;

    /**
     * 创建一个记录读取器，用于读取多个切片中的记录。
     *
     * @param splits 切片列表
     * @return 返回记录读取器
     * @throws IOException 如果发生 I/O 错误
     */
    default RecordReader<InternalRow> createReader(List<Split> splits) throws IOException {
        List<ReaderSupplier<InternalRow>> readers = new ArrayList<>();
        for (Split split : splits) {
            readers.add(() -> createReader(split)); // 为每个切片创建读取器
        }
        return ConcatRecordReader.create(readers); // 将多个读取器合并为一个
    }

    /**
     * 创建一个记录读取器，根据表扫描的计划创建读取器。
     *
     * @param plan 表扫描计划
     * @return 返回记录读取器
     * @throws IOException 如果发生 I/O 错误
     */
    default RecordReader<InternalRow> createReader(TableScan.Plan plan) throws IOException {
        return createReader(plan.splits()); // 根据计划中的切片创建读取器
    }
}
