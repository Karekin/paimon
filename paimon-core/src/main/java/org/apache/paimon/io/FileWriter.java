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

import org.apache.paimon.utils.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * 文件写入器，用于接受一条记录或记录的一个分支，并在关闭后生成元数据。
 *
 * @param <T> 记录类型。
 * @param <R> 文件结果类型。
 */
public interface FileWriter<T, R> extends Closeable {

    /**
     * 向此文件写入器中写入一条记录。
     *
     * <p>注意：如果在写入过程中发生异常，写入器应该为用户清理无用的文件。
     *
     * @param record 要写入的记录。
     * @throws IOException 如果遇到任何 I/O 错误。
     */
    void write(T record) throws IOException;

    /**
     * 从 {@link Iterator} 中向此文件写入器写入记录。
     *
     * <p>注意：如果在写入过程中发生异常，写入器应该为用户清理无用的文件。
     *
     * @param records 要写入的记录。
     * @throws IOException 如果遇到任何 I/O 错误。
     */
    default void write(Iterator<T> records) throws Exception {
        while (records.hasNext()) {
            write(records.next());
        }
    }

    /**
     * 从 {@link CloseableIterator} 中向此文件写入器写入记录。
     *
     * <p>注意：如果在写入过程中发生异常，写入器应该为用户清理无用的文件。
     *
     * @param records 要写入的记录。
     * @throws IOException 如果遇到任何 I/O 错误。
     */
    default void write(CloseableIterator<T> records) throws Exception {
        try {
            while (records.hasNext()) {
                write(records.next());
            }
        } finally {
            records.close();
        }
    }

    /**
     * 从 {@link Iterable} 中向文件写入器写入记录。
     *
     * <p>注意：如果在写入过程中发生异常，写入器应该为用户清理无用的文件。
     *
     * @param records 要写入的记录。
     * @throws IOException 如果遇到任何 I/O 错误。
     */
    default void write(Iterable<T> records) throws IOException {
        for (T record : records) {
            write(record);
        }
    }

    /**
     * 已写入的记录总数。
     *
     * @return 记录数量。
     */
    long recordCount();

    /**
     * 中止写入以清理孤立文件，如果遇到任何错误。
     *
     * <p>注意：此实现必须是可重入的。
     */
    void abort();

    /** @return 返回此已关闭文件写入器的结果。 */
    R result() throws IOException;
}
