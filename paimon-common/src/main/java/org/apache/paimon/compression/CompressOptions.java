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

package org.apache.paimon.compression;

import java.io.Serializable;
import java.util.Objects;

/**
 * 压缩选项类，用于配置压缩参数。
 */
public class CompressOptions implements Serializable {

    private static final long serialVersionUID = 1L; // 用于支持 Java 序列化，确保版本兼容性

    // 压缩算法类型
    private final String compress;
    // 压缩级别（特定于 Zstandard 压缩算法）
    private final int zstdLevel;

    /**
     * 构造方法，初始化压缩选项。
     * @param compress 压缩算法类型，例如 "zstd"
     * @param zstdLevel 压缩级别，用于 Zstandard 压缩算法
     */
    public CompressOptions(String compress, int zstdLevel) {
        this.compress = compress; // 初始化压缩算法类型
        this.zstdLevel = zstdLevel; // 初始化 Zstandard 压缩级别
    }

    // 获取压缩算法类型
    public String compress() {
        return compress;
    }

    // 获取 Zstandard 压缩级别
    public int zstdLevel() {
        return zstdLevel;
    }

    /**
     * 重写 equals 方法，用于比较两个 CompressOptions 对象是否相等。
     * @param o 要比较的对象
     * @return 若对象相等，返回 true；否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) { // 检查是否为同一对象
            return true;
        }
        if (o == null || getClass() != o.getClass()) { // 检查对象类型是否一致
            return false;
        }
        CompressOptions that = (CompressOptions) o;
        // 检查压缩算法类型和压缩级别是否相同
        return zstdLevel == that.zstdLevel && Objects.equals(compress, that.compress);
    }

    /**
     * 重写 hashCode 方法，用于生成对象的哈希码。
     * @return 对象的哈希码
     */
    @Override
    public int hashCode() {
        // 根据压缩算法类型和压缩级别生成哈希码
        return Objects.hash(compress, zstdLevel);
    }

    /**
     * 重写 toString 方法，返回对象的字符串表示形式。
     * @return 对象的字符串表示
     */
    @Override
    public String toString() {
        return "CompressOptions{" +
                "compress='" + compress + '\'' + // 压缩算法类型
                ", zstdLevel=" + zstdLevel + // Zstandard 压缩级别
                '}';
    }

    /**
     * 返回默认的压缩选项。
     * @return 默认的 CompressOptions 对象
     */
    public static CompressOptions defaultOptions() {
        return new CompressOptions("zstd", 1); // 默认使用 Zstandard 压缩，级别为 1
    }
}
