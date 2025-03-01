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

import org.apache.paimon.utils.ExecutorThreadFactory;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 使用同步函数包装实现的异步查找函数。
 * 此类用于将同步的查找函数封装为异步的，通过线程池实现并发执行。
 */
public class AsyncLookupFunctionWrapper extends AsyncLookupFunction {

    /** 被包装的同步查找函数。 */
    private final NewLookupFunction function;

    /** 线程池的线程数量。 */
    private final int threadNumber;

    /** 线程池，采用懒加载方式初始化。 */
    private transient ExecutorService lazyExecutor;

    /**
     * 构造函数，初始化被包装的同步函数和线程池线程数量。
     * @param function 被包装的同步查找函数。
     * @param threadNumber 线程池的线程数量。
     */
    public AsyncLookupFunctionWrapper(NewLookupFunction function, int threadNumber) {
        this.function = function;
        this.threadNumber = threadNumber;
    }

    /**
     * 初始化函数。
     * 调用被包装的同步函数的初始化方法。
     * @throws 异常。
     */
    @Override
    public void open(FunctionContext context) throws Exception {
        function.open(context);
    }

    /**
     * 同步查找方法，在线程池中运行。
     * 此方法确保线程上下文类加载器的正确设置，并返回查找结果。
     * @param keyRow 查找的键。
     * @return 查找结果集合。
     */
    private Collection<RowData> lookup(RowData keyRow) {
        // 保存当前线程的类加载器
        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        // 设置线程上下文类加载器为当前类的类加载器
        Thread.currentThread()
                .setContextClassLoader(AsyncLookupFunctionWrapper.class.getClassLoader());

        try {
            // 同步调用被包装的同步函数的查找方法
            synchronized (function) {
                return function.lookup(keyRow);
            }
        } catch (IOException e) {
            // 如果抛出I/O异常，将其包装为非检查异常继续抛出
            throw new UncheckedIOException(e);
        } finally {
            // 恢复原始的线程上下文类加载器
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    /**
     * 异步查找方法，返回一个包含查找结果的CompletableFuture。
     * 使用线程池中的线程异步执行同步查找任务。
     * @param keyRow 查找的键。
     * @return 包含查找结果的CompletableFuture。
     */
    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        // 使用线程池异步执行同步查找任务，并返回CompletableFuture
        return CompletableFuture.supplyAsync(() -> lookup(keyRow), executor());
    }

    /**
     * 关闭函数，释放资源。
     * 关闭被包装的同步函数，并关闭线程池。
     * @throws 异常。
     */
    @Override
    public void close() throws Exception {
        function.close(); // 关闭被包装的同步函数
        if (lazyExecutor != null) {
            lazyExecutor.shutdownNow(); // 强制关闭线程池
            lazyExecutor = null; // 释放引用
        }
    }

    /**
     * 获取线程池。
     * 如果线程池尚未初始化，则创建一个新的固定大小线程池。
     * @return 线程池。
     */
    private ExecutorService executor() {
        // 懒加载初始化线程池
        if (lazyExecutor == null) {
            lazyExecutor =
                    Executors.newFixedThreadPool(
                            threadNumber,
                            new ExecutorThreadFactory(Thread.currentThread().getName() + "-async"));
        }
        return lazyExecutor;
    }
}
