/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.bitset.BitSetMethods;

/**
 * @author huldarchen
 * @version 1.0
 * @date 2022/10/17 19:43
 */
public class MyUnsafeRowWriter extends UnsafeWriter{
    private final UnsafeRow row;

    private final int nullBitsSize;

    private final int fixedSize;

    public MyUnsafeRowWriter(int numFields) {
        this(new UnsafeRow(numFields));
    }

    public MyUnsafeRowWriter(int numFields, int initialBufferSize) {
        this(new UnsafeRow(numFields), initialBufferSize);
    }

    public MyUnsafeRowWriter(UnsafeWriter writer, int numFields) {
        this(null, writer.holder, numFields);
    }


    private MyUnsafeRowWriter(UnsafeRow row) {
        this(row, new BufferHolder(row), row.numFields());
    }

    private MyUnsafeRowWriter(UnsafeRow row, int initialBufferSize) {
        this(row, new BufferHolder(row, initialBufferSize), row.numFields());
    }

    private MyUnsafeRowWriter(UnsafeRow row, BufferHolder holder, int numFields) {
        super(holder);
        this.row = row;
        this.nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields);
        this.fixedSize = nullBitsSize + 8 * numFields;
        this.startingOffset = cursor();
    }

    public UnsafeRow getRow() {
        row.setTotalSize(totalSize());
        return row;
    }

    public void resetRowWriter() {
        this.startingOffset = cursor();

        grow(fixedSize);
        increaseCursor(fixedSize);
        zeroOutNullBytes();
    }

    public void zeroOutNullBytes() {
        for (int i = 0; i < nullBitsSize; i += 8) {
            Platform.putLong(getBuffer(), startingOffset + i, 0L);
        }
    }

    public boolean isNullAt(int ordinal) {
        return BitSetMethods.isSet(getBuffer(), startingOffset, ordinal);
    }

    public void setNullAt(int ordinal) {
        BitSetMethods.set(getBuffer(), startingOffset, ordinal);
        write(ordinal, 0L);
    }

    @Override
    public void setNull1Bytes(int ordinal) {
        setNullAt(ordinal);
    }

    @Override
    public void setNull2Bytes(int ordinal) {
        setNullAt(ordinal);
    }

    @Override
    public void setNull4Bytes(int ordinal) {
        setNullAt(ordinal);
    }

    @Override
    public void setNull8Bytes(int ordinal) {
        setNullAt(ordinal);
    }

    public long getFieldOffset(int ordinal) {
        return startingOffset + nullBitsSize + 8L * ordinal;
    }


    @Override
    public void write(int ordinal, boolean value) {
        final long offset = getFieldOffset(ordinal);
        writeLong(offset, 0);
        writeBoolean(offset, value);
    }

    @Override
    public void write(int ordinal, byte value) {
        final long offset = getFieldOffset(ordinal);
        writeLong(offset, 0);
        writeByte(offset, value);
    }

    @Override
    public void write(int ordinal, short value) {
        final long offset = getFieldOffset(ordinal);
        writeLong(offset, 0);
        writeShort(offset, value);
    }

    @Override
    public void write(int ordinal, int value) {
        final long offset = getFieldOffset(ordinal);
        writeLong(offset, 0);
        writeInt(offset, value);
    }

    @Override
    public void write(int ordinal, long value) {
        writeLong(getFieldOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, float value) {
        final long offset = getFieldOffset(ordinal);
        writeLong(offset, 0);
        writeFloat(offset, value);
    }

    @Override
    public void write(int ordinal, double value) {
        writeDouble(getFieldOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, Decimal input, int precision, int scale) {
        if (precision <= Decimal.MAX_LONG_DIGITS()) {
            if (input != null && input.changePrecision(precision, scale)) {
                write(ordinal, input.toUnscaledLong());
            } else {
                setNullAt(ordinal);
            }
        } else {
            holder.grow(16);

            Platform.putLong(getBuffer(), cursor(), 0L);
            Platform.putLong(getBuffer(), cursor() + 8, 0L);

            if (input == null || !input.changePrecision(precision, scale)) {
                BitSetMethods.set(getBuffer(), startingOffset, ordinal);
                // keep the offset for future update
                setOffsetAndSize(ordinal, 0);
            } else {
                final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
                final int numBytes = bytes.length;
                assert numBytes <= 16;

                // Write the bytes to the variable length portion.
                Platform.copyMemory(
                        bytes, Platform.BYTE_ARRAY_OFFSET, getBuffer(), cursor(), numBytes);
                setOffsetAndSize(ordinal, bytes.length);
            }
            increaseCursor(16);
        }
    }
}
