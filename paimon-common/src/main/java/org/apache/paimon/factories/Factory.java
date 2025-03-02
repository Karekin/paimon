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

package org.apache.paimon.factories;

/**
 * 该接口是所有类型的工厂（Factory）的基础接口，
 * 这些工厂用于从 Paimon 的 catalog（目录）和 lineage（数据血缘）中的键值对列表创建对象实例。
 *
 * <p>工厂的唯一标识由 {@link Class} 和 {@link #identifier()} 组成。
 *
 * <p>工厂的列表是通过 Java 的服务提供者接口（Service Provider Interfaces，SPI）机制进行发现的。
 * 实现该接口的类可以被添加到 JAR 文件中的
 * {@code META_INF/services/org.apache.paimon.factories.Factory} 目录下，
 * 这样就可以通过 SPI 机制进行动态加载。
 */
public interface Factory {

    /**
     * 返回该工厂在相同类型的工厂接口中唯一的标识符。
     *
     * <p>为了保持一致性，标识符应该是一个小写的单词（例如 {@code kafka}）。
     * 如果存在不同版本的多个工厂，则应使用 "-" 追加版本号（例如 {@code elasticsearch-7}）。
     *
     * @return 该工厂的唯一标识符
     */
    String identifier();
}
