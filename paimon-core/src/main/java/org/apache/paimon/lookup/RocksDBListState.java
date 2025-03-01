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

package org.apache.paimon.lookup;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.utils.ListDelimitedSerializer;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 基于RocksDB的键值列表状态管理类。
 * <p>
 * 该类继承自RocksDBState，用于管理键到值列表的映射关系。
 * 它支持键值对的添加、获取操作，并提供序列化和反序列化功能。
 */
public class RocksDBListState<K, V> extends RocksDBState<K, V, List<V>> {

    // 列表序列化器，用于序列化和反序列化列表数据
    private final ListDelimitedSerializer listSerializer = new ListDelimitedSerializer();

    /**
     * 构造函数，初始化RocksDBListState对象。
     * <p>
     * 该构造函数接收以下几个参数：
     * - stateFactory: RocksDB状态工厂，用于创建和管理状态存储。
     * - columnFamily: 列族句柄，用于指定操作的列族。
     * - keySerializer: 键的序列化器，用于将键序列化为字节数组。
     * - valueSerializer: 值的序列化器，用于将值序列化为字节数组。
     * - lruCacheSize: 缓存大小，用于指定缓存中的元素数量限制。
     *
     * @param stateFactory 状态工厂
     * @param columnFamily 列族句柄
     * @param keySerializer 键的序列化器
     * @param valueSerializer 值的序列化器
     * @param lruCacheSize 缓存大小
     */
    public RocksDBListState(
            RocksDBStateFactory stateFactory,
            ColumnFamilyHandle columnFamily,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        // 调用父类的构造函数
        super(stateFactory, columnFamily, keySerializer, valueSerializer, lruCacheSize);
    }

    /**
     * 向指定键对应的列表中添加一个值。
     * <p>
     * 该方法会将键和值序列化为字节数组，并使用RocksDB的merge操作追加值到键对应的列表中。
     * 同时，会从缓存中清除对应键的值，以确保数据一致性。
     *
     * @param key 键
     * @param value 值
     * @throws IOException 如果操作过程中出现IO异常
     */
    public void add(K key, V value) throws IOException {
        byte[] keyBytes = serializeKey(key); // 序列化键为字节数组
        byte[] valueBytes = serializeValue(value); // 序列化值为字节数组

        try {
            // 使用RocksDB的merge操作追加值到键对应的列表中
            db.merge(columnFamily, writeOptions, keyBytes, valueBytes);
        } catch (RocksDBException e) {
            throw new IOException(e); // 捕获RocksDB异常并抛出IO异常
        }

        // 从缓存中清除对应键的值，确保一致性
        cache.invalidate(wrap(keyBytes));
    }

    /**
     * 根据指定的键获取对应的值列表。
     * <p>
     * 该方法会从缓存或RocksDB中读取数据，并反序列化为列表。
     * 如果缓存中没有数据，会从RocksDB中读取并反序列化为列表，然后存储到缓存中。
     *
     * @param key 键
     * @return 对应的值列表，如果数据不存在则返回空列表
     * @throws IOException 如果操作过程中出现IO异常
     */
    public List<V> get(K key) throws IOException {
        byte[] keyBytes = serializeKey(key); // 序列化键为字节数组

        // 从缓存中获取数据或读取并反序列化RocksDB中的数据
        return cache.get(
                wrap(keyBytes),
                k -> {
                    byte[] valueBytes;
                    try {
                        // 从RocksDB中读取数据
                        valueBytes = db.get(columnFamily, keyBytes);
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e); // 捕获RocksDB异常并抛出运行时异常
                    }

                    // 反序列化字节数组为值列表
                    List<V> rows = listSerializer.deserializeList(valueBytes, valueSerializer);
                    if (rows == null) {
                        return Collections.emptyList(); // 返回空列表
                    }
                    return rows;
                });
    }

    /**
     * 序列化单个值为字节数组。
     * <p>
     * 该方法会清除输出视图缓冲区，将值序列化为字节数组，并返回字节数组的副本。
     *
     * @param value 待序列化的值
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程中出现异常
     */
    public byte[] serializeValue(V value) throws IOException {
        valueOutputView.clear(); // 清除输出视图缓冲区
        // 序列化值到输出视图
        valueSerializer.serialize(value, valueOutputView);
        return valueOutputView.getCopyOfBuffer(); // 返回字节数组的副本
    }

    /**
     * 序列化列表为字节数组。
     * <p>
     * 该方法会将列表中的每个元素序列化为字节数组，并将所有字节数组合并为一个字节数组。
     * 注意：这里会直接返回{@link ListDelimitedSerializer}的序列化结果，可能无法正确反序列化。
     *
     * @param valueList 待序列化的值列表
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程中出现异常
     */
    public byte[] serializeList(List<byte[]> valueList) throws IOException {
        return listSerializer.serializeList(valueList); // 序列化列表为字节数组
    }
}
