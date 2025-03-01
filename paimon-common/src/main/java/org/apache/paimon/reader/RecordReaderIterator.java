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

package org.apache.paimon.reader;

import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.IOUtils;

import java.io.IOException;

/** 将 RecordReader 包装为 CloseableIterator。 */
public class RecordReaderIterator<T> implements CloseableIterator<T> {

    // 被包装的 RecordReader，用于读取数据
    private final RecordReader<T> reader;
    // 当前的记录迭代器，用于读取批处理数据
    private RecordReader.RecordIterator<T> currentIterator;
    // 标记是否已经执行过 advance 操作
    private boolean advanced;
    // 当前的记录结果
    private T currentResult;

    /**
     * 构造方法，初始化 RecordReaderIterator。
     * @param reader 待包装的 RecordReader
     */
    public RecordReaderIterator(RecordReader<T> reader) {
        this.reader = reader;
        try {
            this.currentIterator = reader.readBatch(); // 读取初始批处理数据
        } catch (Exception e) {
            // 如果读取失败，关闭资源并抛出异常
            IOUtils.closeQuietly(reader);
            throw new RuntimeException(e);
        }
        this.advanced = false; // 初始未执行 advance 操作
        this.currentResult = null; // 初始无记录结果
    }

    /**
     * 检查是否有下一个元素。
     * <b>重要</b>: 在调用此方法前，确保前一个返回的键值对已不再使用！
     */
    @Override
    public boolean hasNext() {
        if (currentIterator == null) {
            return false; // 当前迭代器为空，没有更多元素
        }
        advanceIfNeeded(); // 根据需要向前移动迭代器
        return currentResult != null; // 存在非空的当前结果
    }

    /**
     * 获取下一个元素。
     * @return 下一个元素，或 null 如果没有更多元素
     */
    @Override
    public T next() {
        if (!hasNext()) {
            return null; // 没有更多元素，返回 null
        }
        advanced = false; // 重置 advance 标志
        return currentResult; // 返回当前结果
    }

    /**
     * 根据需要向前移动迭代器。
     */
    private void advanceIfNeeded() {
        if (advanced) { // 如果已经执行过 advance，直接返回
            return;
        }
        advanced = true; // 标记为已执行 advance

        try {
            while (true) {
                currentResult = currentIterator.next(); // 获取下一个记录
                if (currentResult != null) {
                    break; // 找到非空记录，退出循环
                } else {
                    // 当前批处理已耗尽，释放资源并读取下一个批处理
                    currentIterator.releaseBatch();
                    currentIterator = null;
                    currentIterator = reader.readBatch();
                    if (currentIterator == null) {
                        break; // 没有更多批处理，退出循环
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // 捕获 I/O 异常并抛出运行时异常
        }
    }

    /**
     * 关闭资源。
     */
    @Override
    public void close() throws Exception {
        try {
            if (currentIterator != null) {
                currentIterator.releaseBatch(); // 释放当前批处理资源
            }
        } finally {
            reader.close(); // 关闭 RecordReader
        }
    }
}
