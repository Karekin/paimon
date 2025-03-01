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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.Filter;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 封装 {@link MergeFunction}，通过查找第一行生成变更日志。
 */
public class FirstRowMergeFunctionWrapper implements MergeFunctionWrapper<ChangelogResult> {

    private final Filter<InternalRow> contains; // 用于过滤行的条件
    private final FirstRowMergeFunction mergeFunction; // 用于合并第一行的函数
    private final ChangelogResult reusedResult = new ChangelogResult(); // 重用的变更日志结果对象

    public FirstRowMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory, Filter<InternalRow> contains) {
        this.contains = contains;
        MergeFunction<KeyValue> mergeFunction = mergeFunctionFactory.create(); // 创建合并函数
        // 确保合并函数是 FirstRowMergeFunction 类型
        checkArgument(
                mergeFunction instanceof FirstRowMergeFunction,
                "Merge function should be a FirstRowMergeFunction, but is %s, there is a bug.",
                mergeFunction.getClass().getName());
        this.mergeFunction = (FirstRowMergeFunction) mergeFunction;
    }

    @Override
    public void reset() {
        mergeFunction.reset(); // 重置合并函数
    }

    @Override
    public void add(KeyValue kv) {
        mergeFunction.add(kv); // 添加 KeyValue 到合并函数
    }

    @Override
    public ChangelogResult getResult() {
        reusedResult.reset(); // 重置变更结果对象

        KeyValue result = mergeFunction.getResult(); // 获取合并后的结果
        if (mergeFunction.containsHighLevel) {
            // 如果包含高级别数据
            reusedResult.setResult(result); // 设置结果
            return reusedResult;
        }

        if (contains.test(result.key())) {
            // 如果键满足过滤条件，返回空结果
            return reusedResult;
        }

        reusedResult.setResult(result); // 设置结果
        if (result.level() == 0) {
            // 如果层级为 0，表示新记录，添加变更日志
            return reusedResult.addChangelog(result);
        }
        return reusedResult;
    }
}
