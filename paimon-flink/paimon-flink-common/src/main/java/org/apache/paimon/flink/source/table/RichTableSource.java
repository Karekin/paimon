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

package org.apache.paimon.flink.source.table;

import org.apache.paimon.flink.source.FlinkTableSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.connector.source.abilities.SupportsStatisticReport;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.plan.stats.TableStats;

import java.util.List;

/**
 * 包含 `lookup`、`watermark`、`statistic` 和 `dynamic filtering` 功能的 {@link BaseTableSource}。
 * <p>该类继承了 {@link BaseTableSource}，并实现了多个接口，以提供对不同类型功能的支持。</p>
 */
public class RichTableSource extends BaseTableSource
        implements LookupTableSource,
        SupportsWatermarkPushDown,
        SupportsStatisticReport,
        SupportsDynamicFiltering {

    private final FlinkTableSource source; // 基础 Flink 数据源

    public RichTableSource(FlinkTableSource source) {
        super(source); // 调用父类构造函数
        this.source = source; // 初始化基础 Flink 数据源
    }

    @Override
    public RichTableSource copy() {
        return new RichTableSource(source.copy()); // 创建当前对象的副本
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // 获取 `lookup` 运行时提供程序
        return source.getLookupRuntimeProvider(context);
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        source.pushWatermark(watermarkStrategy); // 应用水印策略
    }

    @Override
    public TableStats reportStatistics() {
        // 返回表的统计信息
        return source.reportStatistics();
    }

    @Override
    public List<String> listAcceptedFilterFields() {
        // 列出可以动态过滤的字段
        return source.listAcceptedFilterFields();
    }

    @Override
    public void applyDynamicFiltering(List<String> candidateFilterFields) {
        // 应用动态过滤
        source.applyDynamicFiltering(candidateFilterFields);
    }
}
