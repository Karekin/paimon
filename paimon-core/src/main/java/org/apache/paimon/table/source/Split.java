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

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * 用于读取的输入切片。
 *
 * @since 0.4.0
 */
@Public
public interface Split extends Serializable {

    /**
     * 获取切片中的行数。
     *
     * @return 切片中的行数
     */
    long rowCount();

    /**
     * 如果此切片中的所有文件都可以在不进行合并的情况下直接读取，则返回一个包含要读取的原始文件列表的 {@link Optional}。
     * 否则，返回 {@link Optional#empty()}。
     *
     * @return 原始文件列表
     */
    default Optional<List<RawFile>> convertToRawFiles() {
        return Optional.empty();
    }

    /**
     * 返回数据文件对应的删除文件，表示数据文件中的哪些行被删除。
     * 如果没有对应的删除文件，则返回 null。
     *
     * @return 删除文件列表
     */
    default Optional<List<DeletionFile>> deletionFiles() {
        return Optional.empty();
    }

    /**
     * 返回数据文件的索引文件，例如布隆过滤器索引。
     * 所有类型的索引和列都将存储在一个单一的索引文件中。
     * 如果没有对应的索引文件，则返回 null。
     *
     * @return 索引文件列表
     */
    default Optional<List<IndexFile>> indexFiles() {
        return Optional.empty();
    }
}
