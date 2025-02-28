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

package org.apache.paimon.flink.sink;

import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/** 带有 {@link WriteCallback} 的日志 {@link SinkFunction}。 */
public interface LogSinkFunction extends SinkFunction<SinkRecord> {

    /**
     * 设置写入回调函数。
     * <p>
     * 通过此方法，可以将一个 {@link WriteCallback} 实例绑定到当前的 LogSinkFunction。
     * 当有写入操作完成时，回调函数中的 {@link WriteCallback#onCompletion} 方法会被触发。
     *
     * @param writeCallback 要设置的写入回调函数
     */
    void setWriteCallback(WriteCallback writeCallback);

    /**
     * 刷新待处理的记录。
     * <p>
     * 在某些情况下，记录可能被缓存或异步写入。调用此方法会确保所有待处理的记录被立即写入目标存储。
     *
     * @throws Exception 如果在刷新过程中发生任何错误
     */
    void flush() throws Exception;

    /**
     * 一个回调接口，用户可以实现它来了解当请求完成时 bucket 的偏移量。
     */
    interface WriteCallback {

        /**
         * 一个回调方法，用户可以实现它来提供请求完成的异步处理。
         * <p>
         * 当向服务器发送的记录被确认后，此方法将被调用。
         *
         * @param bucket 桶号，标识写入操作所归属的桶
         * @param offset 写入操作完成时的偏移量
         */
        void onCompletion(int bucket, long offset);
    }
}