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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util;

import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemBatch;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WriteBatches {

  public static Collection<MemBatch.Snapshot> of(DataView data, BufferAllocator allocator) {
    switch (data.getRawDataType()) {
      case Column:
        return of((ColumnDataView) data, allocator, true);
      case NonAlignedColumn:
        return of((ColumnDataView) data, allocator, false);
      case Row:
        return of((RowDataView) data, allocator, true);
      case NonAlignedRow:
        return of((RowDataView) data, allocator, false);
      default:
        throw new IllegalArgumentException("Unsupported data type: " + data.getRawDataType());
    }
  }

  private static Collection<MemBatch.Snapshot> of(RowDataView data, BufferAllocator allocator, boolean aligned) {
    List<Field> fields = buildFields(data);
    long[] keys = buildKeys(data);

    List<Object[]> columns = IntStream.range(0, data.getPathNum()).mapToObj(i -> new Object[data.getKeySize()]).collect(Collectors.toList());
    for (int keyIndex = 0; keyIndex < data.getKeySize(); keyIndex++) {
      BitmapView bitmap = data.getBitmapView(keyIndex);
      int valueIndex = 0;
      for (int fieldIndex = 0; fieldIndex < data.getPathNum(); fieldIndex++) {
        if (bitmap.get(fieldIndex)) {
          columns.get(fieldIndex)[keyIndex] = data.getValue(keyIndex, valueIndex);
          valueIndex++;
        }
      }
    }

    return buildSnapshot(fields, keys, columns, allocator, aligned);
  }

  private static Collection<MemBatch.Snapshot> of(ColumnDataView data, BufferAllocator allocator, boolean aligned) {
    List<Field> fields = buildFields(data);
    long[] keys = buildKeys(data);

    List<Object[]> columns = IntStream.range(0, data.getPathNum()).mapToObj(i -> new Object[data.getKeySize()]).collect(Collectors.toList());
    for (int fieldIndex = 0; fieldIndex < data.getPathNum(); fieldIndex++) {
      BitmapView bitmap = data.getBitmapView(fieldIndex);
      int valueIndex = 0;
      for (int keyIndex = 0; keyIndex < data.getKeySize(); keyIndex++) {
        if (bitmap.get(keyIndex)) {
          columns.get(fieldIndex)[keyIndex] = data.getValue(fieldIndex, valueIndex);
          valueIndex++;
        }
      }
    }

    return buildSnapshot(fields, keys, columns, allocator, aligned);
  }

  private static List<Field> buildFields(DataView data) {
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < data.getPathNum(); i++) {
      Map<String, String> tags = data.getTags(i);
      if (tags == null) {
        tags = Collections.emptyMap();
      }
      fields.add(ArrowFields.of(data.getPath(i), tags, data.getDataType(i)));
    }
    return fields;
  }

  private static long[] buildKeys(DataView data) {
    long[] keys = new long[data.getKeySize()];
    for (int i = 0; i < data.getKeySize(); i++) {
      keys[i] = data.getKey(i);
    }
    return keys;
  }

  private static List<MemBatch.Snapshot> buildSnapshot(List<Field> fields, long[] keys, List<Object[]> columns, BufferAllocator allocator, boolean aligned) {
    if (aligned) {
      return ImmutableList.of(buildSnapshotAligned(fields, keys, columns, allocator));
    } else {
      return buildSnapshotUnaligned(fields, keys, columns, allocator);
    }
  }

  private static List<MemBatch.Snapshot> buildSnapshotUnaligned(List<Field> fields, long[] keys, List<Object[]> columns, BufferAllocator allocator) {
    Preconditions.checkArgument(fields.size() == columns.size());

    List<MemBatch.Snapshot> snapshots = new ArrayList<>();
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      Object[] values = columns.get(i);
      int[] nonnullIndices = IntStream.range(0, values.length).filter(index -> values[index] != null).toArray();
      long[] nonnullKeys = Arrays.stream(nonnullIndices).mapToLong(index -> keys[index]).toArray();
      Object[] nonnullValues = Arrays.stream(nonnullIndices).mapToObj(index -> values[index]).toArray();
      snapshots.add(buildSnapshotAligned(ImmutableList.of(field), nonnullKeys, ImmutableList.of(nonnullValues), allocator));
    }
    return snapshots;
  }

  private static MemBatch.Snapshot buildSnapshotAligned(List<Field> fields, long[] keys, List<Object[]> columns, BufferAllocator allocator) {
    Preconditions.checkArgument(fields.size() == columns.size());

    BigIntVector keyVector = buildKeyVector(keys, allocator);
    ImmutableList.Builder<FieldVector> fieldVectorsBuilder = ImmutableList.builder();

    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      Object[] values = columns.get(i);
      FieldVector fieldVector = buildFieldVector(field, values, allocator);
      fieldVectorsBuilder.add(fieldVector);
    }

    return new MemBatch.Snapshot(keyVector, fieldVectorsBuilder.build());
  }

  private static BigIntVector buildKeyVector(long[] keys, BufferAllocator allocator) {
    BigIntVector keyVector = (BigIntVector) ArrowFields.KEY.createVector(allocator);
    keyVector.allocateNew(keys.length);
    for (int i = 0; i < keys.length; i++) {
      keyVector.set(i, keys[i]);
    }
    keyVector.setValueCount(keys.length);
    return keyVector;
  }

  private static FieldVector buildFieldVector(Field field, Object[] values, BufferAllocator allocator) {
    FieldVector valueVector = field.createVector(allocator);
    valueVector.setInitialCapacity(values.length);
    if (valueVector instanceof FixedWidthVector) {
      ((FixedWidthVector) valueVector).allocateNew(values.length);
    }

    BiConsumer<Integer, Object> appender = createValueAppender(valueVector);
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      if (value != null) {
        appender.accept(i, value);
      }
    }

    valueVector.setValueCount(values.length);
    return valueVector;
  }

  private static BiConsumer<Integer, Object> createValueAppender(FieldVector valueVector) {
    switch (valueVector.getMinorType()) {
      case BIT: {
        BitVector vector = (BitVector) valueVector;
        return (index, value) -> vector.set(index, (Boolean) value ? 1 : 0);
      }
      case INT: {
        IntVector vector = (IntVector) valueVector;
        return (index, value) -> vector.set(index, (Integer) value);
      }
      case BIGINT: {
        BigIntVector vector = (BigIntVector) valueVector;
        return (index, value) -> vector.set(index, (Long) value);
      }
      case FLOAT4: {
        Float4Vector vector = (Float4Vector) valueVector;
        return (index, value) -> vector.set(index, (Float) value);
      }
      case FLOAT8: {
        Float8Vector vector = (Float8Vector) valueVector;
        return (index, value) -> vector.set(index, (Double) value);
      }
      case VARBINARY: {
        VarBinaryVector vector = (VarBinaryVector) valueVector;
        return (index, value) -> vector.setSafe(index, (byte[]) value);
      }
      default:
        throw new IllegalArgumentException("Unsupported data type: " + valueVector.getMinorType());
    }
  }

}
