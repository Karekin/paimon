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
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * RocksDB状态管理类，用于管理键到集合值的映射。
 * <p>
 * 该类继承自RocksDBState，用于存储和管理键到集合值的映射关系。
 * 它提供了获取、添加和删除集合元素的功能，并支持缓存机制。
 */
public class RocksDBSetState<K, V> extends RocksDBState<K, V, List<byte[]>> {

    /** 空字节数组，用于表示空值 */
    private static final byte[] EMPTY = new byte[0];

    /**
     * 构造函数，初始化RocksDBSetState对象。
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
    public RocksDBSetState(
            RocksDBStateFactory stateFactory,
            ColumnFamilyHandle columnFamily,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            long lruCacheSize) {
        super(stateFactory, columnFamily, keySerializer, valueSerializer, lruCacheSize);
    }

    /**
     * 根据指定的键获取对应的值集合。
     * <p>
     * 该方法会从缓存或RocksDB中读取数据，并反序列化为值集合。
     * 如果缓存中没有数据，会从RocksDB中读取并存储到缓存中。
     *
     * @param key 键
     * @return 对应的值集合
     * @throws IOException 如果获取过程中出现IO异常
     */
    public List<V> get(K key) throws IOException {
        ByteArray keyBytes = wrap(serializeKey(key)); // 序列化键并包装为ByteArray
        List<byte[]> valueBytes = cache.getIfPresent(keyBytes); // 从缓存中获取值字节数组列表

        if (valueBytes == null) { // 如果缓存中没有数据
            valueBytes = new ArrayList<>(); // 创建新的值字节数组列表

            try (RocksIterator iterator = db.newIterator(columnFamily)) { // 创建RocksDB迭代器
                iterator.seek(keyBytes.bytes); // 寻找键的起始位置

                while (iterator.isValid() && startWithKeyPrefix(keyBytes.bytes, iterator.key())) { // 遍历匹配的键
                    byte[] rawKeyBytes = iterator.key(); // 获取当前键的字节数组
                    byte[] value = Arrays.copyOfRange(rawKeyBytes, keyBytes.bytes.length, rawKeyBytes.length); // 提取值部分
                    valueBytes.add(value); // 将值添加到列表中
                    iterator.next(); // 移动到下一个键
                }
            }

            cache.put(keyBytes, valueBytes); // 将值字节数组列表存入缓存
        }

        List<V> values = new ArrayList<>(valueBytes.size()); // 创建值对象列表
        for (byte[] value : valueBytes) { // 遍历值字节数组列表
            valueInputView.setBuffer(value); // 设置输入视图的缓冲区
            values.add(valueSerializer.deserialize(valueInputView)); // 反序列化为值对象并添加到列表中
        }
        return values; // 返回值对象列表
    }

    /**
     * 从集合中移除指定的值。
     * <p>
     * 该方法会根据键和值构造一个键值对字节数组，并从RocksDB中删除该键值对。
     *
     * @param key 键
     * @param value 值
     * @throws IOException 如果移除过程中出现IO异常
     */
    public void retract(K key, V value) throws IOException {
        try {
            byte[] bytes = invalidKeyAndGetKVBytes(key, value); // 构造键值对字节数组
            if (db.get(columnFamily, bytes) != null) { // 如果键值对存在
                db.delete(columnFamily, writeOptions, bytes); // 删除键值对
            }
        } catch (RocksDBException e) { // 捕获RocksDB异常
            throw new IOException(e); // 抛出IO异常
        }
    }

    /**
     * 向集合中添加指定的值。
     * <p>
     * 该方法会根据键和值构造一个键值对字节数组，并将该键值对写入RocksDB。
     *
     * @param key 键
     * @param value 值
     * @throws IOException 如果添加过程中出现IO异常
     */
    public void add(K key, V value) throws IOException {
        try {
            byte[] bytes = invalidKeyAndGetKVBytes(key, value); // 构造键值对字节数组
            db.put(columnFamily, writeOptions, bytes, EMPTY); // 将键值对写入RocksDB
        } catch (RocksDBException e) { // 捕获RocksDB异常
            throw new IOException(e); // 抛出IO异常
        }
    }

    /**
     * 构造键值对字节数组，并使缓存失效。
     * <p>
     * 该方法会将键和值序列化为字节数组，并构造一个键值对字节数组。
     * 同时，会使缓存中对应的键失效。
     *
     * @param key 键
     * @param value 值
     * @return 构造的键值对字节数组
     * @throws IOException 如果序列化过程中出现IO异常
     */
    private byte[] invalidKeyAndGetKVBytes(K key, V value) throws IOException {
        checkArgument(value != null); // 检查值不能为空

        keyOutView.clear(); // 清除输出视图缓冲区
        keySerializer.serialize(key, keyOutView); // 序列化键到输出视图

        // 使缓存中对应的键失效
        cache.invalidate(wrap(keyOutView.getCopyOfBuffer()));

        valueSerializer.serialize(value, keyOutView); // 序列化值到输出视图
        return keyOutView.getCopyOfBuffer(); // 返回构造的键值对字节数组
    }

    /**
     * 检查字节数组是否以指定的前缀开头。
     * <p>
     * 该方法会逐字节比较两个字节数组，判断一个字节数组是否以另一个字节数组为前缀。
     *
     * @param keyPrefixBytes 前缀字节数组
     * @param rawKeyBytes 待检查的字节数组
     * @return 如果字节数组以指定的前缀开头，返回true；否则返回false
     */
    private boolean startWithKeyPrefix(byte[] keyPrefixBytes, byte[] rawKeyBytes) {
        if (rawKeyBytes.length < keyPrefixBytes.length) { // 如果待检查的字节数组长度小于前缀字节数组长度
            return false; // 返回false
        }

        for (int i = keyPrefixBytes.length; --i >= 0; ) { // 逐字节比较
            if (rawKeyBytes[i] != keyPrefixBytes[i]) { // 如果字节不相等
                return false; // 返回false
            }
        }

        return true; // 返回true
    }
}
