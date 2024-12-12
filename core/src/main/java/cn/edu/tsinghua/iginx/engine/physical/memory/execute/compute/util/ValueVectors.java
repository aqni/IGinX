/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunctions;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic.And;
import javax.annotation.Nullable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

public class ValueVectors {

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T slice(
      BufferAllocator allocator, @Nullable T source, int startIndex, int valueCount, Field field) {
    if (source == null) {
      return null;
    }
    if (source.getValueCount() == 0) {
      return (T) source.getField().createVector(allocator);
    }
    TransferPair transferPair = source.getTransferPair(field, allocator);
    transferPair.splitAndTransfer(startIndex, valueCount);
    return (T) transferPair.getTo();
  }

  public static <T extends ValueVector> T slice(
      BufferAllocator allocator, @Nullable T source, int startIndex, int valueCount, String ref) {
    if (source == null) {
      return null;
    }
    return slice(
        allocator, source, startIndex, valueCount, Schemas.fieldWithName(source.getField(), ref));
  }

  public static <T extends ValueVector> T slice(
      BufferAllocator allocator, @Nullable T source, boolean nullable) {
    if (source == null) {
      return null;
    }
    return slice(
        allocator,
        source,
        0,
        source.getValueCount(),
        Schemas.fieldWithNullable(source.getField(), nullable));
  }

  public static <T extends ValueVector> T slice(
      BufferAllocator allocator, @Nullable T source, int startIndex, int valueCount) {
    if (source == null) {
      return null;
    }
    return slice(allocator, source, startIndex, valueCount, source.getName());
  }

  public static <T extends ValueVector> T slice(
      BufferAllocator allocator, T source, int valueCount, String ref) {
    return slice(allocator, source, 0, valueCount, ref);
  }

  public static <T extends ValueVector> T slice(BufferAllocator allocator, T source, String ref) {
    if (source == null) {
      return null;
    }
    return slice(allocator, source, source.getValueCount(), ref);
  }

  public static <T extends ValueVector> T slice(
      BufferAllocator allocator, @Nullable T source, int valueCount) {
    return slice(allocator, source, 0, valueCount);
  }

  public static <T extends ValueVector> T slice(BufferAllocator allocator, @Nullable T source) {
    if (source == null) {
      return null;
    }
    return slice(allocator, source, source.getValueCount());
  }

  @SuppressWarnings("unchecked")
  public static <T extends BaseIntVector> T slice(
      BufferAllocator allocator, T indices, Dictionary dictionary) {
    Field field =
        Schemas.fieldWithName(indices.getField(), dictionary.getVector().getField().getName());
    Field fieldWithDictionaryEncoding =
        Schemas.fieldWithDictionary(field, dictionary.getEncoding());
    TransferPair transferPair = indices.getTransferPair(fieldWithDictionaryEncoding, allocator);
    transferPair.splitAndTransfer(0, indices.getValueCount());
    return (T) transferPair.getTo();
  }

  public static FieldVector create(BufferAllocator allocator, Types.MinorType returnType) {
    return returnType.getNewVector(Schemas.defaultField(returnType), allocator, null);
  }

  public static FieldVector create(BufferAllocator allocator, Field field) {
    return field.createVector(allocator);
  }

  public static FieldVector create(
      BufferAllocator allocator, Types.MinorType minorType, int rowCount) {
    FieldVector ret = create(allocator, minorType);
    ret.setInitialCapacity(rowCount);
    ret.setValueCount(rowCount);
    return ret;
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T likeOnlyField(BufferAllocator allocator, T left) {
    return (T) create(allocator, left.getField());
  }

  public static <T extends ValueVector> T like(BufferAllocator allocator, T left) {
    T ret = likeOnlyField(allocator, left);
    ret.setInitialCapacity(left.getValueCount());
    ret.setValueCount(left.getValueCount());
    return ret;
  }

  public static ValueVector createWithBothValidity(
      BufferAllocator allocator, ValueVector left, ValueVector right, Types.MinorType type) {
    ValueVector ret = create(allocator, type);

    int valueCount = Math.min(left.getValueCount(), right.getValueCount());
    ret.setInitialCapacity(valueCount);
    ret.setValueCount(valueCount);

    ArrowBuf retValidityBuffer = ret.getValidityBuffer();
    ArrowBuf firstValidityBuffer = left.getValidityBuffer();
    ArrowBuf secondValidityBuffer = right.getValidityBuffer();

    new And()
        .evaluate(
            retValidityBuffer,
            firstValidityBuffer,
            secondValidityBuffer,
            BitVectorHelper.getValidityBufferSize(valueCount));
    return ret;
  }

  public static ValueVector createWithValidity(
      BufferAllocator allocator, ValueVector input, Types.MinorType type) {
    ValueVector ret = create(allocator, type);

    int valueCount = input.getValueCount();
    ret.setInitialCapacity(valueCount);
    ret.setValueCount(valueCount);

    ArrowBuf retValidityBuffer = ret.getValidityBuffer();
    ArrowBuf inputValidityBuffer = input.getValidityBuffer();
    long capacity = Math.min(retValidityBuffer.capacity(), inputValidityBuffer.capacity());
    MemoryUtil.UNSAFE.copyMemory(
        retValidityBuffer.memoryAddress(), inputValidityBuffer.memoryAddress(), capacity);

    return ret;
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T likeWithBothValidity(
      BufferAllocator allocator, T first, T second) {
    if (first.getMinorType() != second.getMinorType()) {
      throw new IllegalArgumentException("Cannot create intersection vector for different types");
    }
    return (T) createWithBothValidity(allocator, first, second, first.getMinorType());
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T likeWithValidity(BufferAllocator allocator, T vector) {
    return (T) createWithValidity(allocator, vector, vector.getMinorType());
  }

  @SuppressWarnings("unchecked")
  public static <T extends ValueVector> T transfer(
      BufferAllocator allocator, T result, String ref) {
    TransferPair transferPair = result.getTransferPair(ref, allocator);
    transferPair.transfer();
    return (T) transferPair.getTo();
  }

  public static <T extends ValueVector> T transfer(BufferAllocator allocator, T vector) {
    return transfer(allocator, vector, vector.getName());
  }

  public static IntVector ofNonnull(BufferAllocator allocator, String name, Integer[] values) {
    return of(allocator, name, false, values);
  }

  public static IntVector of(
      BufferAllocator allocator, String name, boolean nullable, Integer[] values) {
    Field field = Schemas.field(name, nullable, Types.MinorType.INT);
    IntVector ret = (IntVector) field.createVector(allocator);
    ret.allocateNew(values.length);
    for (int i = 0; i < values.length; i++) {
      if (nullable && values[i] == null) {
        ret.setNull(i);
      } else {
        ret.set(i, values[i]);
      }
    }
    ret.setValueCount(values.length);
    return ret;
  }

  public static <T extends ValueVector> Object[] getObjects(T[] columns, int position) {
    Object[] ret = new Object[columns.length];
    for (int i = 0; i < columns.length; i++) {
      ret[i] = columns[i].getObject(position);
    }
    return ret;
  }

  public static FieldVector flatten(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      FieldVector vector,
      @Nullable BaseIntVector selection) {
    DictionaryEncoding dictionaryEncoding = vector.getField().getDictionary();
    if (dictionaryEncoding == null) {
      if (selection != null) {
        return PhysicalFunctions.take(allocator, selection, vector);
      } else {
        return slice(allocator, vector);
      }
    }

    Dictionary dictionary = dictionaryProvider.lookup(dictionaryEncoding.getId());
    FieldVector dictionaryVector = dictionary.getVector();
    BaseIntVector indices = (BaseIntVector) vector;
    int destCount = selection == null ? vector.getValueCount() : selection.getValueCount();

    try (FieldVector dest = dictionaryVector.getField().createVector(allocator)) {
      FixedWidthVector fixedWidthVector =
          dest instanceof FixedWidthVector ? (FixedWidthVector) dest : null;
      if (fixedWidthVector != null) {
        fixedWidthVector.allocateNew(destCount);
      } else {
        dest.setInitialCapacity(destCount);
      }
      for (int destIndex = 0; destIndex < destCount; destIndex++) {
        int sourceIndex = selection == null ? destIndex : (int) selection.getValueAsLong(destIndex);
        if (indices.isNull(sourceIndex)) {
          continue;
        }
        int dictionaryIndex = (int) indices.getValueAsLong(sourceIndex);
        if (fixedWidthVector != null) {
          fixedWidthVector.copyFrom(dictionaryIndex, destIndex, dictionaryVector);
        } else {
          dest.copyFromSafe(dictionaryIndex, destIndex, dictionaryVector);
        }
      }
      dest.setValueCount(destCount);
      return ValueVectors.transfer(allocator, dest, indices.getName());
    }
  }
}