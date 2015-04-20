/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions;


import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import static org.apache.spark.sql.types.DataTypes.*;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UTF8String;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.string.UTF8StringMethods;
import scala.collection.Map;
import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;


// TODO: pick a better name for this class, since this is potentially confusing.
// Maybe call it UnsafeMutableRow?

/**
 * An Unsafe implementation of Row which is backed by raw memory instead of Java objets.
 *
 * Each tuple has three parts: [null bit set] [values] [variable length portion]
 *
 * The bit set is used for null tracking and is aligned to 8-byte word boundaries.  It stores
 * one bit per field.
 *
 * In the `values` region, we store one 8-byte word per field. For fields that hold fixed-length
 * primitive types, such as long, double, or int, we store the value directly in the word. For
 * fields with non-primitive or variable-length values, we store a relative offset (w.r.t. the
 * base address of the row) that points to the beginning of the variable-length field.
 */
public final class UnsafeRow implements MutableRow {

  private Object baseObject;
  private long baseOffset;
  private int numFields;
  /** The width of the null tracking bit set, in bytes */
  private int bitSetWidthInBytes;
  @Nullable
  private StructType schema;

  private long getFieldOffset(int ordinal) {
   return baseOffset + bitSetWidthInBytes + ordinal * 8;
  }

  public static int calculateBitSetWidthInBytes(int numFields) {
    return ((numFields / 64) + ((numFields % 64 == 0 ? 0 : 1))) * 8;
  }

  public UnsafeRow() { }

  public void set(Object baseObject, long baseOffset, int numFields, StructType schema) {
    assert numFields >= 0 : "numFields should >= 0";
    assert schema == null || schema.fields().length == numFields;
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.numFields = numFields;
    this.schema = schema;
  }

  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < numFields : "index (" + index + ") should <= " + numFields;
  }

  @Override
  public void setNullAt(int i) {
    assertIndexIsValid(i);
    BitSetMethods.set(baseObject, baseOffset, i);
  }

  private void setNotNullAt(int i) {
    assertIndexIsValid(i);
    BitSetMethods.unset(baseObject, baseOffset, i);
  }

  @Override
  public void update(int ordinal, Object value) {
    assert schema != null : "schema cannot be null when calling the generic update()";
    final DataType type = schema.fields()[ordinal].dataType();
    // TODO: match based on the type, then set.  This will be slow.
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putInt(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setLong(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putLong(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setDouble(int ordinal, double value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putDouble(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setBoolean(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putBoolean(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setShort(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putShort(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setByte(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putByte(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setFloat(int ordinal, float value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    PlatformDependent.UNSAFE.putFloat(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setString(int ordinal, String value) {
    // TODO: need to ensure that array has been suitably sized.
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return numFields;
  }

  @Override
  public int length() {
    return size();
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Object apply(int i) {
    return get(i);
  }

  @Override
  public Object get(int i) {
    assertIndexIsValid(i);
    final DataType dataType = schema.fields()[i].dataType();
    // The ordering of these `if` statements is intentional: internally, it looks like this only
    // gets invoked in JoinedRow when trying to access UTF8String columns. It's extremely unlikely
    // that internal code will call this on non-string-typed columns, but we support that anyways
    // just for the sake of completeness.
    // TODO: complete this for the remaining types?
    if (isNullAt(i)) {
      return null;
    } else if (dataType == StringType) {
      return getUTF8String(i);
    } else if (dataType == IntegerType) {
      return getInt(i);
    } else if (dataType == LongType) {
      return getLong(i);
    } else if (dataType == DoubleType) {
      return getDouble(i);
    } else if (dataType == FloatType) {
      return getFloat(i);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public boolean isNullAt(int i) {
    assertIndexIsValid(i);
    return BitSetMethods.isSet(baseObject, baseOffset, i);
  }

  @Override
  public boolean getBoolean(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getBoolean(baseObject, getFieldOffset(i));
  }

  @Override
  public byte getByte(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getByte(baseObject, getFieldOffset(i));
  }

  @Override
  public short getShort(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getShort(baseObject, getFieldOffset(i));
  }

  @Override
  public int getInt(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getInt(baseObject, getFieldOffset(i));
  }

  @Override
  public long getLong(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getLong(baseObject, getFieldOffset(i));
  }

  @Override
  public float getFloat(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getFloat(baseObject, getFieldOffset(i));
  }

  @Override
  public double getDouble(int i) {
    assertIndexIsValid(i);
    return PlatformDependent.UNSAFE.getDouble(baseObject, getFieldOffset(i));
  }

  public UTF8String getUTF8String(int i) {
    // TODO: this is inefficient; just doing this to make some tests pass for now; will fix later
    assertIndexIsValid(i);
    return UTF8String.apply(getString(i));
  }

  @Override
  public String getString(int i) {
    assertIndexIsValid(i);
    final long offsetToStringSize = getLong(i);
    final long stringSizeInBytes =
      PlatformDependent.UNSAFE.getLong(baseObject, baseOffset + offsetToStringSize);
    // TODO: ugly cast; figure out whether we'll support mega long strings
    return UTF8StringMethods.toJavaString(baseObject, baseOffset + offsetToStringSize + 8, (int) stringSizeInBytes);
  }

  @Override
  public BigDecimal getDecimal(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public Date getDate(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> Seq<T> getSeq(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<T> getList(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> Map<K, V> getMap(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V> java.util.Map<K, V> getJavaMap(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public Row getStruct(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getAs(int i) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public Row copy() {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean anyNull() {
    return BitSetMethods.anySet(baseObject, baseOffset, bitSetWidthInBytes);
  }

  @Override
  public Seq<Object> toSeq() {
    final ArraySeq<Object> values = new ArraySeq<Object>(numFields);
    for (int fieldNumber = 0; fieldNumber < numFields; fieldNumber++) {
      values.update(fieldNumber, get(fieldNumber));
    }
    return values;
  }

  @Override
  public String toString() {
    return mkString("[", ",", "]");
  }

  @Override
  public String mkString() {
    return toSeq().mkString();
  }

  @Override
  public String mkString(String sep) {
    return toSeq().mkString(sep);
  }

  @Override
  public String mkString(String start, String sep, String end) {
    return toSeq().mkString(start, sep, end);
  }
}