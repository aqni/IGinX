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
import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@ThreadSafe
public final class MemBatch implements NoexceptAutoCloseable {

  private final BigIntVector keyVector;
  private VectorSchemaRoot fieldVectors;

  public MemBatch(List<Field> fields, int initialCapacity, BufferAllocator allocator) {
    this(new BigIntVector("", allocator), VectorSchemaRoot.create(new Schema(fields), allocator));

    this.keyVector.setInitialCapacity(initialCapacity);
    for (FieldVector valueVector : fieldVectors.getFieldVectors()) {
      valueVector.setInitialCapacity(initialCapacity);
    }
  }

  private MemBatch(@WillCloseWhenClosed BigIntVector keyVector, @WillCloseWhenClosed VectorSchemaRoot fieldVectors) {
    this.keyVector = keyVector;
    this.fieldVectors = fieldVectors;
  }

  public synchronized MemBatch split(List<Field> fields, BufferAllocator allocator) {
    BigIntVector keyVectorReplicate = copy(keyVector, allocator);

    Set<Field> splitFields = new HashSet<>(fields);
    List<FieldVector> splittedVector = fields.stream()
        .map(f -> fieldVectors.getVector(f))
        .map(v -> transfer(v, allocator))
        .collect(ImmutableList.toImmutableList());
    List<FieldVector> remainVector = fieldVectors.getFieldVectors()
        .stream()
        .filter(v -> !splitFields.contains(v.getField()))
        .map(v -> transfer(v, v.getAllocator()))
        .collect(ImmutableList.toImmutableList());

    this.fieldVectors.close();
    this.fieldVectors = new VectorSchemaRoot(remainVector);
    return new MemBatch(keyVectorReplicate, new VectorSchemaRoot(splittedVector));
  }

  public synchronized Snapshot snapshot(BufferAllocator allocator) {
    return new Snapshot(keyVector, fieldVectors, fieldVectors.getSchema().getFields(), 0, keyVector.getValueCount(), allocator);
  }

  @SuppressWarnings("unchecked")
  private static <V extends ValueVector> V transfer(V vector, BufferAllocator allocator) {
    TransferPair transferPair = vector.getTransferPair(allocator);
    transferPair.transfer();
    return (V) transferPair.getTo();
  }

  @SuppressWarnings("unchecked")
  private static <V extends ValueVector> V copy(V vector, BufferAllocator allocator) {
    TransferPair transferPair = vector.getTransferPair(allocator);
    V replicate = (V) transferPair.getTo();
    replicate.setInitialCapacity(vector.getValueCapacity());
    VectorBatchAppender.batchAppend(replicate, vector);
    return replicate;
  }

  public synchronized void append(Snapshot batch) {
    VectorBatchAppender.batchAppend(keyVector, batch.keyVector);
    VectorSchemaRootAppender.append(fieldVectors, batch.fieldVectors);
  }

  public synchronized int getValueCount() {
    return keyVector.getValueCount();
  }

  @Override
  public synchronized void close() {
    keyVector.close();
    fieldVectors.close();
  }

  @Immutable
  public final static class Snapshot implements NoexceptAutoCloseable {

    private final BigIntVector keyVector;
    private final VectorSchemaRoot fieldVectors;

    public Snapshot(@WillCloseWhenClosed BigIntVector keyVector, @WillCloseWhenClosed List<FieldVector> fieldVectors) {
      for (FieldVector fieldVector : fieldVectors) {
        Preconditions.checkArgument(keyVector.getValueCount() == fieldVector.getValueCount());
      }
      this.keyVector = keyVector;
      this.fieldVectors = new VectorSchemaRoot(fieldVectors);
    }

    private Snapshot(
        BigIntVector keyVector,
        VectorSchemaRoot fieldVectors,
        List<Field> fields,
        int startIndex,
        int length,
        BufferAllocator allocator) {
      this(
          slice(keyVector, startIndex, length, allocator),
          fields.stream()
              .map(fieldVectors::getVector)
              .map(v -> slice(v, startIndex, length, allocator))
              .collect(ImmutableList.toImmutableList()));
    }

    @SuppressWarnings("unchecked")
    private static <V extends ValueVector> V slice(V vector, int startIndex, int length, BufferAllocator allocator) {
      TransferPair transferPair = vector.getTransferPair(allocator);
      transferPair.splitAndTransfer(startIndex, length);
      return (V) transferPair.getTo();
    }

    public int getValueCount() {
      return keyVector.getValueCount();
    }

    public BigIntVector getKeyVector() {
      return keyVector;
    }

    public List<FieldVector> getValueVectors() {
      return fieldVectors.getFieldVectors();
    }

    public List<Field> getFields() {
      return fieldVectors.getSchema().getFields();
    }

    public Snapshot slice(List<Field> fields, BufferAllocator allocator) {
      return slice(fields, 0, getValueCount(), allocator);
    }

    public Snapshot slice(int startIndex, int length, BufferAllocator allocator) {
      return slice(getFields(), startIndex, length, allocator);
    }

    public Snapshot slice(List<Field> fields, int startIndex, int length, BufferAllocator allocator) {
      return new Snapshot(keyVector, fieldVectors, fields, startIndex, length, allocator);
    }

    @Override
    public void close() {
      keyVector.close();
      fieldVectors.close();
    }
  }
}
