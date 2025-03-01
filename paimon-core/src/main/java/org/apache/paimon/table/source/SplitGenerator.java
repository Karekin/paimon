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

import org.apache.paimon.io.DataFileMeta;

import java.util.List;

/** 从 {@link DataFileMeta} 对象中生成数据分割块。 */
public interface SplitGenerator {

    /**
     * 判断是否总是可以转换为原始格式。
     * @return 如果总是可以转换为原始格式，则返回 true；否则返回 false。
     */
    boolean alwaysRawConvertible();

    /**
     * 为批处理模式生成数据分割组。
     * @param files 数据文件元数据列表
     * @return 数据分割组列表
     */
    List<SplitGroup> splitForBatch(List<DataFileMeta> files);

    /**
     * 为流处理模式生成数据分割组。
     * @param files 数据文件元数据列表
     * @return 数据分割组列表
     */
    List<SplitGroup> splitForStreaming(List<DataFileMeta> files);

    /**
     * 数据分割组。
     */
    class SplitGroup {

        public final List<DataFileMeta> files; // 数据文件元数据列表
        public final boolean rawConvertible; // 是否可以转换为原始格式

        private SplitGroup(List<DataFileMeta> files, boolean rawConvertible) {
            this.files = files;
            this.rawConvertible = rawConvertible;
        }

        /**
         * 创建一个可以转换为原始格式的数据分割组。
         * @param files 数据文件元数据列表
         * @return 数据分割组
         */
        public static SplitGroup rawConvertibleGroup(List<DataFileMeta> files) {
            return new SplitGroup(files, true);
        }

        /**
         * 创建一个不可以转换为原始格式的数据分割组。
         * @param files 数据文件元数据列表
         * @return 数据分割组
         */
        public static SplitGroup nonRawConvertibleGroup(List<DataFileMeta> files) {
            return new SplitGroup(files, false);
        }
    }
}
