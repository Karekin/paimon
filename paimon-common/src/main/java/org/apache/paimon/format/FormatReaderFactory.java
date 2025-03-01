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

package org.apache.paimon.format;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;

import java.io.IOException;

/**
 * 一个工厂接口，用于创建文件的 {@link RecordReader}。
 */
public interface FormatReaderFactory {

    /**
     * 创建一个记录读取器。
     *
     * @param context 上下文对象，包含创建读取器所需的信息
     * @return 返回一个 {@link RecordReader} 对象，用于读取文件中的记录
     * @throws IOException 如果在创建读取器过程中发生 I/O 异常
     */
    RecordReader<InternalRow> createReader(Context context) throws IOException;

    /**
     * 创建读取器的上下文接口。
     */
    interface Context {

        /**
         * 获取文件输入输出对象。
         *
         * @return 返回一个 {@link FileIO} 对象，用于文件的输入输出操作
         */
        FileIO fileIO();

        /**
         * 获取文件路径。
         *
         * @return 返回一个 {@link Path} 对象，表示文件的路径
         */
        Path filePath();

        /**
         * 获取文件大小。
         *
         * @return 返回文件的大小（以字节为单位）
         */
        long fileSize();
    }
}
