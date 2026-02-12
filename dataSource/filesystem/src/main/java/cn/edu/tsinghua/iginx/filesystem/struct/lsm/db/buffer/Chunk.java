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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.NoexceptAutoCloseable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.stream.Collectors;

@ThreadSafe
public class Chunk implements NoexceptAutoCloseable {

  protected final BigIntVector keyVector;
  protected final VectorSchemaRoot valueVectors;

  protected Chunk(Snapshot template, BufferAllocator allocator) {
    List<FieldVector> createdVector = template.valueVectors.getFieldVectors().stream().map(v -> create(v, allocator)).collect(Collectors.toList());
    this.keyVector = create(template.keyVector, allocator);
    this.valueVectors = new VectorSchemaRoot(createdVector);
  }

  @SuppressWarnings("unchecked")
  private static <V extends ValueVector> V create(V template, BufferAllocator allocator) {
    V vector = (V) template.getTransferPair(allocator).getTo();
    vector.setInitialCapacity(template.getValueCount());
    return vector;
  }

  public synchronized Snapshot snapshot(BufferAllocator allocator) {
    List<FieldVector> slicedVector = valueVectors.getFieldVectors().stream().map(v -> slice(v, 0, v.getValueCount(), allocator)).collect(Collectors.toList());
    return new Snapshot(slice(keyVector, 0, keyVector.getValueCount(), allocator), new VectorSchemaRoot(slicedVector));
  }

  @SuppressWarnings("unchecked")
  private static <V extends ValueVector> V slice(V vector, int startIndex, int length, BufferAllocator allocator) {
    TransferPair transferPair = vector.getTransferPair(allocator);
    if (length != 0) {
      transferPair.splitAndTransfer(startIndex, length);
    }
    return (V) transferPair.getTo();
  }

  public synchronized void append(Snapshot data) {
    VectorBatchAppender.batchAppend(keyVector, data.keyVector);
    VectorSchemaRootAppender.append(valueVectors, data.valueVectors);
  }

  public synchronized int getValueCount() {
    return keyVector.getValueCount();
  }

  @Override
  public synchronized void close() {
    keyVector.close();
    valueVectors.close();
  }

  @Immutable
  public static class Snapshot implements NoexceptAutoCloseable {

    protected final BigIntVector keyVector;
    protected final VectorSchemaRoot valueVectors;
    protected final transient ValueVector[] valueVectorArray;

    public Snapshot(@WillCloseWhenClosed BigIntVector keyVector, @WillCloseWhenClosed VectorSchemaRoot valueVectors) {
      Preconditions.checkNotNull(keyVector);
      Preconditions.checkNotNull(valueVectors);
      Preconditions.checkArgument(keyVector.getValueCount() == valueVectors.getRowCount());

      this.keyVector = keyVector;
      this.valueVectors = valueVectors;
      this.valueVectorArray = valueVectors.getFieldVectors().toArray(new ValueVector[0]);
    }

    public Schema getSchema() {
      return valueVectors.getSchema();
    }

    public int getValueCount() {
      return keyVector.getValueCount();
    }

    public Snapshot slice(int startIndex, int length, BufferAllocator allocator) {
      List<FieldVector> slicedVector = valueVectors.getFieldVectors().stream().map(v -> Chunk.slice(v, startIndex, length, allocator)).collect(Collectors.toList());
      return new Snapshot(Chunk.slice(keyVector, startIndex, length, allocator), new VectorSchemaRoot(slicedVector));
    }

    @Override
    public void close() {
      keyVector.close();
      valueVectors.close();
    }

    public long getKey(int index) {
      return keyVector.get(index);
    }

    public void fillRow(int index, Object[] row) {
      for (int col = 0; col < valueVectorArray.length; col++) {
        Object value = valueVectorArray[col].getObject(index);
        if (value != null) {
          row[col] = value;
        }
      }
    }
  }
}
