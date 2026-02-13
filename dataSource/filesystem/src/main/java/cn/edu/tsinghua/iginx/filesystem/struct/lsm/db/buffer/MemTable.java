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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@ThreadSafe
public class MemTable implements NoexceptAutoCloseable {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final int maxChunkValueCount;
  private final BufferAllocator allocator;
  private final Map<List<Field>, MemSubTable> subTables = new IdentityHashMap<>();
  private final Map<Field, List<Field>> fieldToSchema = new HashMap<>();

  public MemTable(BufferAllocator allocator, int maxChunkValueCount) {
    this.allocator = allocator;
    this.maxChunkValueCount = maxChunkValueCount;
  }

  public Snapshot snapshot(BufferAllocator allocator) {
    lock.readLock().lock();
    try {
      return snapshot(fieldToSchema.keySet(), allocator);
    } finally {
      lock.readLock().unlock();
    }
  }

  public Snapshot snapshot(Set<Field> mayBeExistedFields, BufferAllocator allocator) {
    lock.readLock().lock();
    try {
      HashMap<MemSubTable, IntStream> toSnapshot = new HashMap<>();

      Map<List<Field>, List<IntIntPair>> schemaToSourceTargetIndex = hitSubTable(ImmutableList.copyOf(mayBeExistedFields));
      for (Map.Entry<List<Field>, List<IntIntPair>> entry : schemaToSourceTargetIndex.entrySet()) {
        List<Field> schema = entry.getKey();
        if (schema == null) {
          continue;
        }
        List<IntIntPair> sourceTargetIndex = entry.getValue();
        IntStream targetIndex = sourceTargetIndex.stream().mapToInt(IntIntPair::firstInt);
        MemSubTable subTable = subTables.get(schema);
        toSnapshot.put(subTable, targetIndex);
      }

      return new Snapshot(toSnapshot, allocator);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void append(MemBatch.Snapshot data) {
    List<Field> fields = data.getFieldVectors().stream().map(FieldVector::getField).distinct().collect(ImmutableList.toImmutableList());
    Preconditions.checkArgument(fields.size() == data.getFieldVectors().size(), "Fields in snapshot should be distinct");

    boolean needSplitOrCreate = false;
    lock.readLock().lock();
    try {
      Map<List<Field>, List<IntIntPair>> schemaToSourceTargetIndex = hitSubTable(fields);
      for (Map.Entry<List<Field>, List<IntIntPair>> entry : schemaToSourceTargetIndex.entrySet()) {
        List<Field> schema = entry.getKey();
        if (schema == null) {
          needSplitOrCreate = true;
          break;
        }
        List<IntIntPair> sourceTargetIndex = entry.getValue();
        if (sourceTargetIndex.size() != schema.size()) {
          needSplitOrCreate = true;
          break;
        }
      }
      if (!needSplitOrCreate) {
        doAppend(data, schemaToSourceTargetIndex);
      }
    } finally {
      lock.readLock().unlock();
    }
    if (needSplitOrCreate) {
      lock.writeLock().lock();
      try {
        Map<List<Field>, List<IntIntPair>> schemaToSourceTargetIndex = hitSubTable(fields);
        doAppend(data, schemaToSourceTargetIndex);
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  private void doAppend(MemBatch.Snapshot data, Map<List<Field>, List<IntIntPair>> schemaToSourceTargetIndex) {
    for (Map.Entry<List<Field>, List<IntIntPair>> entry : schemaToSourceTargetIndex.entrySet()) {
      List<Field> schema = entry.getKey();
      List<IntIntPair> sourceTargetIndex = entry.getValue();
      IntStream sourceIndex = sourceTargetIndex.stream().mapToInt(IntIntPair::firstInt);

      try (MemBatch.Snapshot dataSlice = data.slice(sourceIndex, allocator)) {
        MemSubTable subTable;

        if (schema == null) {
          // create new sub-table
          ImmutableList<Field> sourceFields = sourceIndex.mapToObj(i -> data.getFieldVectors().get(i).getField()).collect(ImmutableList.toImmutableList());
          subTable = new MemSubTable(sourceFields, allocator, maxChunkValueCount);
          addSubTable(subTable);
        } else if (sourceTargetIndex.size() == schema.size()) {
          // just append
          subTable = subTables.get(schema);
        } else {
          // spilt and append
          IntLinkedOpenHashSet targetIndex = sourceTargetIndex.stream()
              .mapToInt(IntIntPair::secondInt)
              .collect(IntLinkedOpenHashSet::new, IntLinkedOpenHashSet::add, IntLinkedOpenHashSet::addAll);
          schema.forEach(fieldToSchema::remove);
          MemSubTable oldSubTable = subTables.remove(schema);
          subTable = oldSubTable.split(targetIndex, allocator);
          addSubTable(oldSubTable);
          addSubTable(subTable);
        }
        subTable.append(dataSlice);
      }
    }
  }

  private void addSubTable(MemSubTable subTable) {
    List<Field> schema = subTable.getFields();
    subTables.put(schema, subTable);
    for (Field field : schema) {
      fieldToSchema.put(field, schema);
    }
  }

  private Map<List<Field>, List<IntIntPair>> hitSubTable(List<Field> fields) {
    Map<List<Field>, IntList> schemaToHit = new IdentityHashMap<>();
    for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
      Field mayBeExistedField = fields.get(fieldIndex);
      schemaToHit.computeIfAbsent(fieldToSchema.get(mayBeExistedField), key -> new IntArrayList())
          .add(fieldIndex);
    }

    Map<List<Field>, List<IntIntPair>> hitSourceTargetIndex = new IdentityHashMap<>();
    for (Map.Entry<List<Field>, IntList> entry : schemaToHit.entrySet()) {
      List<Field> schema = entry.getKey();
      IntList hitFieldIndexes = entry.getValue();
      Map<Field, Integer> schemaField2Index = IntStream.range(0, schema.size())
          .boxed()
          .collect(Collectors.toMap(schema::get, Integer::valueOf));
      List<IntIntPair> hitFieldIndexesWithTargetIndex = new ArrayList<>();
      for (int sourceIndex : hitFieldIndexes) {
        Field sourceField = fields.get(sourceIndex);
        int targetIndex = schemaField2Index.get(sourceField);
        hitFieldIndexesWithTargetIndex.add(IntIntPair.of(sourceIndex, targetIndex));
      }
      hitSourceTargetIndex.put(schema, hitFieldIndexesWithTargetIndex);
    }
    return hitSourceTargetIndex;
  }

  @Override
  public void close() {
    lock.writeLock().lock();
    try {
      subTables.values().forEach(MemSubTable::close);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public static class Snapshot implements NoexceptAutoCloseable {
    private final List<MemSubTable.Snapshot> subTables;

    Snapshot(Map<MemSubTable, IntStream> columns, BufferAllocator allocator) {
      this.subTables = columns.entrySet().stream()
          .map(e -> e.getKey().snapshot(e.getValue(), allocator))
          .collect(ImmutableList.toImmutableList());
    }

    public List<MemSubTable.Snapshot> getColumns() {
      return subTables;
    }

    @Override
    public void close() {
      subTables.forEach(MemSubTable.Snapshot::close);
    }
  }

}
