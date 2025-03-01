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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * 查找表接口，提供获取数据和刷新数据的功能。
 * <p>
 * 该接口定义了查找表的基本功能，包括：
 * - 设置特定分区过滤器
 * - 初始化查找表
 * - 根据键获取数据
 * - 刷新查找表数据
 */
public interface LookupTable extends Closeable {

    /**
     * 设置特定分区过滤器。
     * <p>
     * 该方法允许为查找表设置特定的分区过滤器，以过滤特定分区的数据。
     *
     * @param filter 过滤器，用于过滤数据行
     */
    void specificPartitionFilter(Predicate filter);

    /**
     * 初始化查找表。
     * <p>
     * 该方法用于初始化查找表相关的资源和状态。
     *
     * @throws Exception 如果初始化过程中出现异常
     */
    void open() throws Exception;

    /**
     * 根据给定的键获取数据。
     * <p>
     * 该方法将根据指定的键从查找表中获取匹配的数据行。
     *
     * @param key 查询键
     * @return 匹配的数据行列表
     * @throws IOException 如果获取过程中出现IO异常
     */
    List<InternalRow> get(InternalRow key) throws IOException;

    /**
     * 刷新查找表中的数据。
     * <p>
     * 该方法将刷新查找表中的数据，确保数据是最新的。
     *
     * @throws Exception 如果刷新过程中出现异常
     */
    void refresh() throws Exception;
}
