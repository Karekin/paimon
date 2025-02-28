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

package org.apache.paimon.flink.log;

import org.apache.paimon.flink.sink.LogSinkFunction.WriteCallback;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAccumulator;

/** {@link WriteCallback} 接口的实现类。 */
public class LogWriteCallback implements WriteCallback {

    // 用于存储每个桶（bucket）对应的偏移量累加器的并发哈希表
    private final ConcurrentHashMap<Integer, LongAccumulator> offsetMap = new ConcurrentHashMap<>();

    @Override
    public void onCompletion(int bucket, long offset) {
        LongAccumulator acc = offsetMap.get(bucket); // 获取当前桶对应的偏移量累加器

        if (acc == null) {
            // 如果当前桶的累加器不存在，则使用 computeIfAbsent 方法原子地创建并初始化
            // computeIfAbsent 在访问键时会锁定该键，确保线程安全
            acc = offsetMap.computeIfAbsent(bucket, k -> new LongAccumulator(Long::max, 0));
        } // 否则，访问已有的累加器无需加锁

        // 累加器保存的是下一次写入的起始偏移量
        // 对于混合读取操作，需要提供下一次事务的起始偏移量
        acc.accumulate(offset + 1); // 将当前完成写入的偏移量加一后累加到该桶的累加器中
    }

    /**
     * 获取所有桶对应的偏移量。
     *
     * @return 一个包含所有桶及其对应最后一个写入偏移量的映射
     */
    public Map<Integer, Long> offsets() {
        Map<Integer, Long> offsets = new HashMap<>(); // 创建一个临时的哈希映射来存储结果
        // 遍历 offsetMap 中的所有键值对，并将当前桶的偏移量值存入 offsets
        offsetMap.forEach((k, v) -> offsets.put(k, v.longValue()));
        return offsets; // 返回所有桶的偏移量映射
    }
}
