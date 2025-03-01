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

import org.apache.paimon.fs.PositionOutputStream;

import java.io.IOException;

/**
 * 一个工厂接口，用于创建 {@link FormatWriter}。
 */
public interface FormatWriterFactory {

    /**
     * 创建一个写入器，将数据写入到指定的输出流中。
     *
     * @param out 输出流，用于写入编码后的数据。
     * @param compression 压缩方式。
     * @return 返回一个 {@link FormatWriter} 对象，用于写入数据。
     * @throws IOException 如果写入器无法打开，或者输出流抛出异常。
     */
    FormatWriter create(PositionOutputStream out, String compression) throws IOException;
}
