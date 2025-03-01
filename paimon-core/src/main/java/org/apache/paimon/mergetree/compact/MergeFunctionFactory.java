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

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * 创建{@link MergeFunction} 的工厂接口。
 * <p>
 * 此接口用于生成不同类型的合并函数。
 * MergeFunction 通常用于数据合并场景，如流处理中的聚合操作或数据库中的多表合并。
 * 通过实现此接口，可以灵活地创建各种自定义的合并函数，满足不同的业务需求。
 */
@FunctionalInterface
public interface MergeFunctionFactory<T> extends Serializable {

    /**
     * 无参数创建合并函数。
     * <p>
     * 该方法是一个默认方法，提供一个无参数的创建方式。
     * 调用时会自动调用带有投影参数的 create 方法，并传入 null 作为投影参数。
     * 当不需要使用投影功能时，可以通过此方法快速创建合并函数。
     * <p>
     * 使用示例：
     * <pre>
     * MergeFunctionFactory&lt;Data&gt; factory = ...;
     * MergeFunction&lt;Data&gt; function = factory.create();
     * </pre>
     *
     * @return 返回一个合并函数实例
     */
    default MergeFunction<T> create() {
        return create(null);
    }

    /**
     * 创建带有投影参数的合并函数。
     * <p>
     * 该方法是创建合并函数的核心方法。
     * 投影参数（projection）用于指定数据的投影规则。
     * 投影规则可以定义如何将数据映射到目标结构，例如在数据合并时只保留特定字段或进行数据转换。
     * 通过传入不同的投影参数，可以生成具有不同投影规则的合并函数。
     * <p>
     * 参数示例：
     * <pre>
     * int[][] projection = {{0, 1}, {2, 3}};
     * MergeFunction&lt;Data&gt; function = factory.create(projection);
     * </pre>
     *
     * @param projection 投影参数，可以为 null
     * @return 返回一个合并函数实例
     */
    MergeFunction<T> create(@Nullable int[][] projection);

    /**
     * 调整投影参数。
     * <p>
     * 该方法是一个默认方法，用于根据传入的投影参数生成调整后的投影结果。
     * 调整后的投影结果包含下推投影和外围投影两个部分。
     * - 下推投影（pushdownProjection）用于将投影规则下推到数据源或中间处理阶段，提高处理效率。
     * - 外围投影（outerProjection）用于在合并函数的外围处理中应用投影规则。
     * <p>
     * 返回示例：
     * <pre>
     * AdjustedProjection adjusted = factory.adjustProjection(projection);
     * int[][] pushdown = adjusted.pushdownProjection();
     * int[][] outer = adjusted.outerProjection();
     * </pre>
     *
     * @param projection 投影参数
     * @return 返回调整后的投影结果
     */
    default AdjustedProjection adjustProjection(@Nullable int[][] projection) {
        return new AdjustedProjection(projection, null);
    }

    /**
     * 调整后的投影结果。
     * <p>
     * 该类用于封装调整后的投影结果，包括下推投影和外围投影。
     */
    class AdjustedProjection {

        // 下推投影
        @Nullable public final int[][] pushdownProjection;

        // 外围投影
        @Nullable public final int[][] outerProjection;

        /**
         * 构造方法，初始化调整后的投影结果。
         * <p>
         * 示例：
         * <pre>
         * int[][] pushdown = {{0, 1}};
         * int[][] outer = {{2, 3}};
         * AdjustedProjection adjusted = new AdjustedProjection(pushdown, outer);
         * </pre>
         *
         * @param pushdownProjection 下推投影
         * @param outerProjection 外围投影
         */
        public AdjustedProjection(int[][] pushdownProjection, int[][] outerProjection) {
            this.pushdownProjection = pushdownProjection;
            this.outerProjection = outerProjection;
        }
    }
}
