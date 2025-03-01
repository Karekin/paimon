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

package org.apache.paimon.predicate;

import org.apache.paimon.types.DataType;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.predicate.CompareUtils.compareLiteral;

/**
 * 一种 {@link LeafFunction} 的实现，用于评估“IN”条件。
 */
public class In extends LeafFunction {

    private static final long serialVersionUID = 1L; // 用于序列化版本控制

    public static final In INSTANCE = new In(); // 单例实例，确保只有一个 In 对象

    private In() {} // 私有构造函数，防止外部实例化

    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        // 判断字段是否在 literals 列表中
        if (field == null) {
            return false; // 如果字段为空，直接返回 false
        }
        for (Object literal : literals) { // 遍历 literals 列表
            if (literal != null && compareLiteral(type, literal, field) == 0) {
                return true; // 如果匹配，返回 true
            }
        }
        return false; // 未匹配，返回 false
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        // 判断在统计信息下的字段是否在 literals 列表中
        if (nullCount != null && rowCount == nullCount) {
            return false; // 如果所有行都是 null，返回 false
        }
        for (Object literal : literals) { // 遍历 literals 列表
            if (literal != null
                    && compareLiteral(type, literal, min) >= 0
                    && compareLiteral(type, literal, max) <= 0) {
                return true; // 如果在范围内的匹配，返回 true
            }
        }
        return false; // 未匹配，返回 false
    }

    @Override
    public Optional<LeafFunction> negate() {
        // 返回“NOT IN”的相反条件
        return Optional.of(NotIn.INSTANCE);
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        // 访问“IN”条件
        return visitor.visitIn(fieldRef, literals); // 调用访问者的“IN”方法
    }
}