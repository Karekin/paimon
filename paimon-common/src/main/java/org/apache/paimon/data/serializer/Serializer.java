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

package org.apache.paimon.data.serializer;

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

/**
 * 序列化器接口，用于序列化对象结构。
 * <p>
 * 该接口定义了序列化器的基本功能，包括创建对象副本、序列化和反序列化等。
 */
public interface Serializer<T> extends Serializable {

    /**
     * 如果序列化器是需要状态的，那么创建该序列化器的一个深层副本。
     * 如果序列化器不需要状态，则直接返回当前对象。
     * <p>
     * 这是因为序列化器可能会被多个线程使用。无状态的序列化器是线程安全的，
     * 而有状态的序列化器则可能不是。通过创建副本，可以确保每个线程都有自己的序列化器实例。
     *
     * @return 序列化器的副本
     */
    Serializer<T> duplicate();

    /**
     * 创建一个给定元素的深度副本。
     *
     * @param from 要复制的元素
     * @return 深度副本元素
     */
    T copy(T from);

    /**
     * 将给定的记录序列化到目标输出视图。
     *
     * @param record 要序列化的记录
     * @param target 目标输出视图
     * @throws IOException 如果序列化过程中出现I/O相关错误
     */
    void serialize(T record, DataOutputView target) throws IOException;

    /**
     * 从给定的源输入视图反序列化记录。
     *
     * @param source 源输入视图
     * @return 反序列化的元素
     * @throws IOException 如果反序列化过程中出现I/O相关错误
     */
    T deserialize(DataInputView source) throws IOException;

    /**
     * 将给定的记录序列化为字符串。
     *
     * @param record 要序列化的记录
     * @return 序列化后的字符串
     */
    default String serializeToString(T record) {
        throw new UnsupportedOperationException(
                String.format("不支持将 %s 序列化为字符串", record));
    }

    /**
     * 从字符串反序列化记录。
     *
     * @param s 要反序列化的字符串
     * @return 反序列化的元素
     */
    default T deserializeFromString(String s) {
        throw new UnsupportedOperationException(
                String.format("不支持从字符串 %s 反序列化", s));
    }
}