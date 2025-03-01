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

import org.rocksdb.ColumnFamilyHandle;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * RocksDB键值状态管理类，用于管理单个值的键值对。
 * <p>
 * 该类继承自RocksDBState，用于存储和管理键值对数据。
 * 它提供了读取、写入、删除键值对的功能，并且支持缓存功能。
 */
public class RocksDBValueState<K, V> extends RocksDBState<K, V, RocksDBState.Reference> {

    /**
     * 构造函数，初始化RocksDBValueState对象。
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
    public RocksDBValueState(
            RocksDBStateFactory stateFactory,
            ColumnFamilyHandle columnFamily,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        super(stateFactory, columnFamily, keySerializer, valueSerializer, lruCacheSize);
    }

    /**
     * 根据指定的键获取对应的值。
     * <p>
     * 该方法会从缓存或RocksDB中读取数据，并反序列化为值。
     * 如果数据不存在，返回null。
     *
     * @param key 键
     * @return 对应的值，如果数据不存在则返回null
     * @throws IOException 如果获取过程中出现IO异常
     */
    @Nullable
    public V get(K key) throws IOException {
        try {
            Reference valueRef = get(wrap(serializeKey(key))); // 获取值引用
            return valueRef.isPresent() ? deserializeValue(valueRef.bytes) : null; // 反序列化值
        } catch (Exception e) {
            throw new IOException(e); // 捕获异常并抛出IO异常
        }
    }

    /**
     * 根据键字节数组获取值引用。
     * <p>
     * 该方法会从缓存或RocksDB中读取数据。
     *
     * @param keyBytes 键字节数组
     * @return 值引用
     * @throws Exception 如果获取过程中出现异常
     */
    private Reference get(ByteArray keyBytes) throws Exception {
        Reference valueRef = cache.getIfPresent(keyBytes); // 从缓存中获取值引用
        if (valueRef == null) { // 如果缓存中没有数据
            valueRef = ref(db.get(columnFamily, keyBytes.bytes)); // 从RocksDB中读取数据
            cache.put(keyBytes, valueRef); // 将数据存入缓存
        }
        return valueRef; // 返回值引用
    }

    /**
     * 将指定的键值对写入状态存储。
     * <p>
     * 该方法会将键和值序列化为字节数组，并写入到RocksDB中。
     * 同时，会更新缓存中的数据。
     *
     * @param key 键
     * @param value 值
     * @throws IOException 如果写入过程中出现IO异常
     */
    public void put(K key, V value) throws IOException {
        checkArgument(value != null); // 检查值不能为空

        try {
            byte[] keyBytes = serializeKey(key); // 序列化键为字节数组
            byte[] valueBytes = serializeValue(value); // 序列化值为字节数组
            db.put(columnFamily, writeOptions, keyBytes, valueBytes); // 将数据写入RocksDB
            cache.put(wrap(keyBytes), ref(valueBytes)); // 更新缓存
        } catch (Exception e) {
            throw new IOException(e); // 捕获异常并抛出IO异常
        }
    }

    /**
     * 根据指定的键删除对应的值。
     * <p>
     * 该方法会从RocksDB和缓存中删除指定键对应的值。
     *
     * @param key 键
     * @throws IOException 如果删除过程中出现IO异常
     */
    public void delete(K key) throws IOException {
        try {
            byte[] keyBytes = serializeKey(key); // 序列化键为字节数组
            ByteArray keyByteArray = wrap(keyBytes); // 包装键字节数组
            Reference valueRef = get(keyByteArray); // 获取值引用
            if (valueRef.isPresent()) { // 如果值存在
                db.delete(columnFamily, writeOptions, keyBytes); // 从RocksDB中删除数据
                cache.put(keyByteArray, ref(null)); // 将缓存中的值设置为null
            }
        } catch (Exception e) {
            throw new IOException(e); // 捕获异常并抛出IO异常
        }
    }

    /**
     * 将字节数组反序列化为值。
     * <p>
     * 该方法会将字节数组反序列化为对应的值对象。
     *
     * @param valueBytes 值字节数组
     * @return 反序列化后的值
     * @throws IOException 如果反序列化过程中出现异常
     */
    public V deserializeValue(byte[] valueBytes) throws IOException {
        valueInputView.setBuffer(valueBytes); // 设置输入视图的缓冲区
        return valueSerializer.deserialize(valueInputView); // 反序列化为值对象
    }

    /**
     * 将值序列化为字节数组。
     * <p>
     * 该方法会将值序列化为字节数组。
     *
     * @param value 值
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程中出现异常
     */
    public byte[] serializeValue(V value) throws IOException {
        valueOutputView.clear(); // 清除输出视图缓冲区
        valueSerializer.serialize(value, valueOutputView); // 序列化值到输出视图
        return valueOutputView.getCopyOfBuffer(); // 返回字节数组的副本
    }
}