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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.BytesColumnVector.Bytes;

import java.io.Serializable;

/**
 * VectorizedColumnBatch 是一个矢量化列批次的表示形式，其中的每一列都以列向量的形式组织。
 *
 * <p>这是查询执行的基本单位，设计目标是最小化每行的处理成本。
 *
 * <p>{@code VectorizedColumnBatch} 的设计受 Apache Hive 的 VectorizedRowBatch 启发。
 */
public class VectorizedColumnBatch implements Serializable {

    private static final long serialVersionUID = 8180323238728166155L;

    /**
     * 默认批次大小，经过精心选择以最小化开销，通常可以使一个 VectorizedColumnBatch 完全加载到缓存中。
     */
    public static final int DEFAULT_SIZE = 2048;

    // 当前批次的行数
    private int numRows;

    // 存储各列的列向量数组
    public final ColumnVector[] columns;

    /**
     * 构造函数，用于初始化矢量化列批次。
     *
     * @param vectors 列向量数组，每个列对应一个向量。
     */
    public VectorizedColumnBatch(ColumnVector[] vectors) {
        this.columns = vectors;
    }

    /**
     * 设置批次的行数。
     *
     * @param numRows 批次的行数。
     */
    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    /**
     * 获取批次的行数。
     *
     * @return 批次的行数。
     */
    public int getNumRows() {
        return numRows;
    }

    /**
     * 获取列的数量（批次的列数）。
     *
     * @return 列数量。
     */
    public int getArity() {
        return columns.length;
    }

    /**
     * 检查指定行和列的值是否为 null。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return 如果值为 null 返回 true，否则返回 false。
     */
    public boolean isNullAt(int rowId, int colId) {
        return columns[colId].isNullAt(rowId);
    }

    /**
     * 获取指定行和列的布尔值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return 布尔值。
     */
    public boolean getBoolean(int rowId, int colId) {
        return ((BooleanColumnVector) columns[colId]).getBoolean(rowId);
    }

    /**
     * 获取指定行和列的字节值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return 字节值。
     */
    public byte getByte(int rowId, int colId) {
        return ((ByteColumnVector) columns[colId]).getByte(rowId);
    }

    /**
     * 获取指定行和列的短整型值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return 短整型值。
     */
    public short getShort(int rowId, int colId) {
        return ((ShortColumnVector) columns[colId]).getShort(rowId);
    }

    /**
     * 获取指定行和列的整型值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return 整型值。
     */
    public int getInt(int rowId, int colId) {
        return ((IntColumnVector) columns[colId]).getInt(rowId);
    }

    /**
     * 获取指定行和列的长整型值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return 长整型值。
     */
    public long getLong(int rowId, int colId) {
        return ((LongColumnVector) columns[colId]).getLong(rowId);
    }

    /**
     * 获取指定行和列的浮点值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return 浮点值。
     */
    public float getFloat(int rowId, int colId) {
        return ((FloatColumnVector) columns[colId]).getFloat(rowId);
    }

    /**
     * 获取指定行和列的双精度浮点值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return 双精度浮点值。
     */
    public double getDouble(int rowId, int colId) {
        return ((DoubleColumnVector) columns[colId]).getDouble(rowId);
    }

    /**
     * 获取指定行和列的字符串值。
     *
     * @param rowId 行索引。
     * @param pos 列索引。
     * @return 字符串值。
     */
    public BinaryString getString(int rowId, int pos) {
        Bytes byteArray = getByteArray(rowId, pos);
        return BinaryString.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
    }

    /**
     * 获取指定行和列的二进制数据。
     *
     * @param rowId 行索引。
     * @param pos 列索引。
     * @return 二进制数据。
     */
    public byte[] getBinary(int rowId, int pos) {
        Bytes byteArray = getByteArray(rowId, pos);
        if (byteArray.len == byteArray.data.length) {
            return byteArray.data;
        } else {
            byte[] ret = new byte[byteArray.len];
            System.arraycopy(byteArray.data, byteArray.offset, ret, 0, byteArray.len);
            return ret;
        }
    }

    /**
     * 获取指定行和列的字节数组。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return 字节数组对象。
     */
    public Bytes getByteArray(int rowId, int colId) {
        return ((BytesColumnVector) columns[colId]).getBytes(rowId);
    }

    /**
     * 获取指定行和列的十进制值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @param precision 精度。
     * @param scale 小数位数。
     * @return 十进制值。
     */
    public Decimal getDecimal(int rowId, int colId, int precision, int scale) {
        return ((DecimalColumnVector) (columns[colId])).getDecimal(rowId, precision, scale);
    }

    /**
     * 获取指定行和列的时间戳值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @param precision 时间精度。
     * @return 时间戳值。
     */
    public Timestamp getTimestamp(int rowId, int colId, int precision) {
        return ((TimestampColumnVector) (columns[colId])).getTimestamp(rowId, precision);
    }

    /**
     * 获取指定行和列的数组值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return 数组值。
     */
    public InternalArray getArray(int rowId, int colId) {
        return ((ArrayColumnVector) columns[colId]).getArray(rowId);
    }

    /**
     * 获取指定行和列的行值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return 行值。
     */
    public InternalRow getRow(int rowId, int colId) {
        return ((RowColumnVector) columns[colId]).getRow(rowId);
    }

    /**
     * 获取指定行和列的 Map 值。
     *
     * @param rowId 行索引。
     * @param colId 列索引。
     * @return Map 值。
     */
    public InternalMap getMap(int rowId, int colId) {
        return ((MapColumnVector) columns[colId]).getMap(rowId);
    }

    /**
     * 复制当前 VectorizedColumnBatch。
     *
     * @param vectors 新的列向量数组。
     * @return 复制的 VectorizedColumnBatch。
     */
    public VectorizedColumnBatch copy(ColumnVector[] vectors) {
        VectorizedColumnBatch vectorizedColumnBatch = new VectorizedColumnBatch(vectors);
        vectorizedColumnBatch.setNumRows(numRows);
        return vectorizedColumnBatch;
    }
}

