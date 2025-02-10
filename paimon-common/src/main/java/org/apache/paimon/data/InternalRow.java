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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.apache.paimon.types.DataTypeChecks.getScale;

/* 该文件基于 Apache Flink 项目 (https://flink.apache.org/) 的源代码，
 * 由 Apache 软件基金会 (ASF) 在 Apache 许可证 2.0 版本下授权使用。
 * 详情请参阅随本工作分发的 NOTICE 文件，以获取有关版权归属的其他信息。
 */

/**
 * 内部数据结构的基本接口，表示 {@link RowType} 类型的数据。
 *
 * <p>SQL 数据类型到内部数据结构的映射如下：
 *
 * <pre>
 * +--------------------------------+-----------------------------------------+
 * | SQL 数据类型                   | 内部数据结构                             |
 * +--------------------------------+-----------------------------------------+
 * | BOOLEAN                        | boolean                                 |
 * +--------------------------------+-----------------------------------------+
 * | CHAR / VARCHAR / STRING        | {@link BinaryString}                    |
 * +--------------------------------+-----------------------------------------+
 * | BINARY / VARBINARY / BYTES     | byte[]                                  |
 * +--------------------------------+-----------------------------------------+
 * | DECIMAL                        | {@link Decimal}                         |
 * +--------------------------------+-----------------------------------------+
 * | TINYINT                        | byte                                    |
 * +--------------------------------+-----------------------------------------+
 * | SMALLINT                       | short                                   |
 * +--------------------------------+-----------------------------------------+
 * | INT                            | int                                     |
 * +--------------------------------+-----------------------------------------+
 * | BIGINT                         | long                                    |
 * +--------------------------------+-----------------------------------------+
 * | FLOAT                          | float                                   |
 * +--------------------------------+-----------------------------------------+
 * | DOUBLE                         | double                                  |
 * +--------------------------------+-----------------------------------------+
 * | DATE                           | int（自 Epoch 起的天数）                 |
 * +--------------------------------+-----------------------------------------+
 * | TIME                           | int（一天中的毫秒数）                     |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP                      | {@link Timestamp}                       |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP WITH LOCAL TIME ZONE | {@link Timestamp}                       |
 * +--------------------------------+-----------------------------------------+
 * | ROW                            | {@link InternalRow}                     |
 * +--------------------------------+-----------------------------------------+
 * | ARRAY                          | {@link InternalArray}                   |
 * +--------------------------------+-----------------------------------------+
 * | MAP / MULTISET                 | {@link InternalMap}                     |
 * +--------------------------------+-----------------------------------------+
 * </pre>
 *
 * <p>可空性（nullability）始终由容器数据结构处理。
 *
 * @see GenericRow
 * @see JoinedRow
 * @since 0.4.0
 */
@Public
public interface InternalRow extends DataGetters {

    /**
     * 返回此行中的字段数。
     *
     * <p>此数量不包括 {@link RowKind}，后者单独存储。
     *
     * @return 字段数量
     */
    int getFieldCount();

    /**
     * 返回此行在变更日志（changelog）中的变更类型。
     *
     * @see RowKind
     * @return 变更类型
     */
    RowKind getRowKind();

    /**
     * 设置此行在变更日志中的变更类型。
     *
     * @see RowKind
     * @param kind 变更类型
     */
    void setRowKind(RowKind kind);

// ------------------------------------------------------------------------------------------
// 访问工具方法（Access Utilities）
// ------------------------------------------------------------------------------------------

    /**
     * 根据给定的 {@link DataType} 返回对应的 Java 数据类型类。
     *
     * @param type  数据类型
     * @return      该数据类型对应的 Java 类
     */
    static Class<?> getDataClass(DataType type) {
        // 按照数据类型的根类型（TypeRoot）进行分类处理
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return BinaryString.class; // 字符串类型映射为 BinaryString
            case BOOLEAN:
                return Boolean.class; // 布尔类型映射为 Boolean
            case BINARY:
            case VARBINARY:
                return byte[].class; // 二进制数据类型映射为字节数组
            case DECIMAL:
                return Decimal.class; // 小数类型映射为 Decimal
            case TINYINT:
                return Byte.class; // 8 位整数类型映射为 Byte
            case SMALLINT:
                return Short.class; // 16 位整数类型映射为 Short
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return Integer.class; // 32 位整数类型映射为 Integer（包括日期和时间）
            case BIGINT:
                return Long.class; // 64 位整数类型映射为 Long
            case FLOAT:
                return Float.class; // 32 位浮点数类型映射为 Float
            case DOUBLE:
                return Double.class; // 64 位浮点数类型映射为 Double
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.class; // 时间戳类型映射为 Timestamp
            case ARRAY:
                return InternalArray.class; // 数组类型映射为 InternalArray
            case MULTISET:
            case MAP:
                return InternalMap.class; // 映射类型映射为 InternalMap
            case ROW:
                return InternalRow.class; // 结构化行类型映射为 InternalRow
            default:
                throw new IllegalArgumentException("非法数据类型：" + type);
        }
    }

    /**
     * 创建一个访问器，用于在运行时获取 {@link InternalRow} 数据结构中指定位置的元素。
     *
     * @param fieldType 字段的数据类型
     * @param fieldPos  字段在行中的索引位置
     * @return          该字段的访问器
     */
    static FieldGetter createFieldGetter(DataType fieldType, int fieldPos) {
        final FieldGetter fieldGetter;
        // 按照数据类型的根类型（TypeRoot）进行分类处理
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter = row -> row.getString(fieldPos); // 访问字符串字段
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos); // 访问布尔字段
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = row -> row.getBinary(fieldPos); // 访问二进制字段
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale); // 访问 Decimal 字段
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos); // 访问 8 位整数字段
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos); // 访问 16 位整数字段
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = row -> row.getInt(fieldPos); // 访问 32 位整数字段
                break;
            case BIGINT:
                fieldGetter = row -> row.getLong(fieldPos); // 访问 64 位整数字段
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos); // 访问 32 位浮点数字段
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos); // 访问 64 位浮点数字段
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestamp(fieldPos, timestampPrecision); // 访问时间戳字段
                break;
            case ARRAY:
                fieldGetter = row -> row.getArray(fieldPos); // 访问数组字段
                break;
            case MULTISET:
            case MAP:
                fieldGetter = row -> row.getMap(fieldPos); // 访问映射字段
                break;
            case ROW:
                final int rowFieldCount = DataTypeChecks.getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount); // 访问结构化行字段
                break;
            default:
                String msg =
                        String.format(
                                "不支持的类型 %s 在 %s 中",
                                fieldType.getTypeRoot().toString(), InternalRow.class.getName());
                throw new IllegalArgumentException(msg);
        }

        // 如果字段允许为空，则需进行额外的空值检查
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null; // 若该字段为空，则返回 null
            }
            return fieldGetter.getFieldOrNull(row); // 否则返回字段值
        };
    }

    /**
     * 用于在运行时获取行中字段的访问器接口。
     */
    interface FieldGetter extends Serializable {
        /**
         * 获取指定行中的字段值，如果字段为空，则返回 null。
         *
         * @param row 目标行
         * @return 该字段的值或 null
         */
        @Nullable
        Object getFieldOrNull(InternalRow row);
    }

}
