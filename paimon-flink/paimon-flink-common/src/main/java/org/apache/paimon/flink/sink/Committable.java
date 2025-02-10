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

/**
 * 由 {@link PrepareCommitOperator} 生成的 Committable（可提交记录）。
 * 该类用于封装待提交的数据，包括检查点 ID、提交类型（文件或日志偏移量）、以及提交数据对象。
 */
public class Committable {

    /** 对应的 Flink 检查点 ID */
    private final long checkpointId;

    /** 提交记录的类型（文件或日志偏移量） */
    private final Kind kind;

    /** 具体的可提交对象，可能是文件信息或日志偏移量 */
    private final Object wrappedCommittable;

    /**
     * 构造方法，初始化 Committable 对象。
     *
     * @param checkpointId       关联的检查点 ID
     * @param kind               可提交记录的类型（文件或日志偏移）
     * @param wrappedCommittable 具体的可提交对象
     */
    public Committable(long checkpointId, Kind kind, Object wrappedCommittable) {
        this.checkpointId = checkpointId;
        this.kind = kind;
        this.wrappedCommittable = wrappedCommittable;
    }

    /**
     * 获取当前 Committable 关联的检查点 ID。
     *
     * @return 检查点 ID
     */
    public long checkpointId() {
        return checkpointId;
    }

    /**
     * 获取当前 Committable 的类型（文件或日志偏移）。
     *
     * @return Committable 类型
     */
    public Kind kind() {
        return kind;
    }

    /**
     * 获取封装的可提交对象，具体内容取决于 kind 类型。
     *
     * @return 具体的可提交对象（文件或日志偏移量）
     */
    public Object wrappedCommittable() {
        return wrappedCommittable;
    }

    /**
     * 返回 Committable 对象的字符串表示，用于日志和调试。
     *
     * @return Committable 的字符串描述
     */
    @Override
    public String toString() {
        return "Committable{"
                + "checkpointId=" + checkpointId
                + ", kind=" + kind
                + ", wrappedCommittable=" + wrappedCommittable
                + '}';
    }

    /**
     * Committable 的类型枚举，表示不同的提交对象类型。
     */
    public enum Kind {
        /** 提交文件 */
        FILE((byte) 0),

        /** 提交日志偏移量 */
        LOG_OFFSET((byte) 1);

        /** 类型对应的字节值 */
        private final byte value;

        /**
         * 构造方法，初始化 Kind 类型。
         *
         * @param value 该类型的字节值
         */
        Kind(byte value) {
            this.value = value;
        }

        /**
         * 将 Kind 类型转换为对应的字节值。
         *
         * @return Kind 对应的字节值
         */
        public byte toByteValue() {
            return value;
        }

        /**
         * 根据字节值解析 Kind 类型。
         *
         * @param value 需要解析的字节值
         * @return 对应的 Kind 类型
         * @throws UnsupportedOperationException 如果字节值不在支持范围内
         */
        public static Kind fromByteValue(byte value) {
            switch (value) {
                case 0:
                    return FILE;
                case 1:
                    return LOG_OFFSET;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported byte value '" + value + "' for value kind.");
            }
        }
    }
}

