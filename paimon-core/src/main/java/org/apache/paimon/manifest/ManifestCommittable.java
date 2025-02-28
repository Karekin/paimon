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

package org.apache.paimon.manifest;

import org.apache.paimon.table.sink.CommitMessage;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 表示提交消息，用于描述需要进行提交的相关信息。
 */
public class ManifestCommittable {

    // 提交的唯一标识符
    private final long identifier;

    // 水位线，可能是一个时间戳或其他指示特定提交点的值
    @Nullable private final Long watermark;

    // 日志偏移量，以桶（bucket）为键，对应的偏移量为值
    private final Map<Integer, Long> logOffsets;

    // 提交消息列表，包含需要提交的具体消息
    private final List<CommitMessage> commitMessages;

    // 构造函数，初始化提交标识符和水位线，其他字段为空
    public ManifestCommittable(long identifier) {
        this(identifier, null);
    }

    // 构造函数，初始化提交标识符和水位线，其他字段为空
    public ManifestCommittable(long identifier, @Nullable Long watermark) {
        this.identifier = identifier;
        this.watermark = watermark;
        this.logOffsets = new HashMap<>();
        this.commitMessages = new ArrayList<>();
    }

    // 构造函数，支持初始化所有字段
    public ManifestCommittable(
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            List<CommitMessage> commitMessages) {
        this.identifier = identifier;
        this.watermark = watermark;
        this.logOffsets = logOffsets;
        this.commitMessages = commitMessages;
    }

    // 添加提交消息
    public void addFileCommittable(CommitMessage commitMessage) {
        commitMessages.add(commitMessage); // 将提交消息添加到列表中
    }

    // 添加日志偏移量
    public void addLogOffset(int bucket, long offset) {
        if (logOffsets.containsKey(bucket)) {
            throw new RuntimeException(
                    String.format(
                            "桶 %d 出现多次，这是不可能的。", bucket)); // 如果桶已经存在，抛出异常
        }
        logOffsets.put(bucket, offset); // 将桶和对应的偏移量添加到日志偏移量中
    }

    // 获取提交的唯一标识符
    public long identifier() {
        return identifier;
    }

    // 获取水位线
    @Nullable
    public Long watermark() {
        return watermark;
    }

    // 获取日志偏移量
    public Map<Integer, Long> logOffsets() {
        return logOffsets;
    }

    // 获取提交消息列表
    public List<CommitMessage> fileCommittables() {
        return commitMessages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true; // 同一个对象，返回 true
        }
        if (o == null || getClass() != o.getClass()) {
            return false; // 如果对象为 null 或类型不同，返回 false
        }
        ManifestCommittable that = (ManifestCommittable) o;
        // 比较各个字段是否相等
        return Objects.equals(identifier, that.identifier)
                && Objects.equals(watermark, that.watermark)
                && Objects.equals(logOffsets, that.logOffsets)
                && Objects.equals(commitMessages, that.commitMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, watermark, logOffsets, commitMessages); // 生成哈希码
    }

    @Override
    public String toString() {
        return String.format( // 返回类的字符串表示形式
                "ManifestCommittable {"
                        + "identifier = %s, "
                        + "watermark = %s, "
                        + "logOffsets = %s, "
                        + "commitMessages = %s",
                identifier, watermark, logOffsets, commitMessages);
    }
}
