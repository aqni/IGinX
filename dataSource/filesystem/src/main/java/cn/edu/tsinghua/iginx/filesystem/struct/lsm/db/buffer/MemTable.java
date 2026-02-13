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
import org.apache.arrow.vector.types.pojo.Field;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class MemTable implements NoexceptAutoCloseable {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final int maxChunkValueCount;
  private final BufferAllocator allocator;
  private final HashMap<List<Field>, MemSubTable> columns = new HashMap<>();
  private final HashMap<Field, List<Field>> fieldToSchema = new HashMap<>();

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
      HashMap<List<Field>, List<Field>> schemaToProjected = new HashMap<>();
      for (Field mayBeExistedField : mayBeExistedFields) {
        List<Field> schema = fieldToSchema.get(mayBeExistedField);
        if (schema != null) {
          schemaToProjected.computeIfAbsent(schema, key -> new ArrayList<>())
              .add(mayBeExistedField);
        }
      }
      HashMap<List<Field>, MemSubTable> toSnapshot = new HashMap<>();
      for (Map.Entry<List<Field>, List<Field>> entry : schemaToProjected.entrySet()) {
        List<Field> schema = entry.getKey();
        List<Field> projected = entry.getValue();
        MemSubTable subTable = columns.get(schema);
        toSnapshot.put(projected, subTable);
      }
      return new Snapshot(toSnapshot, allocator);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void append(MemBatch.Snapshot data) {
    lock.readLock().lock();
    try {
      List<Field> fields = data.getFields();

//      MemSubTable column =
//          columns.computeIfAbsent(
//              data.getSchema(),
//              key -> new MemSubTable(allocator, maxChunkValueCount));
//      column.append(data);
    } finally {
      lock.readLock().unlock();
    }
    lock.writeLock().lock();
    try {
//      MemSubTable column =
//          columns.computeIfAbsent(
//              data.getSchema(),
//              key -> new MemSubTable(allocator, maxChunkValueCount));
//      column.append(data);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() {
    lock.writeLock().lock();
    try {
      columns.values().forEach(MemSubTable::close);
      columns.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public static class Snapshot implements NoexceptAutoCloseable {
    private final List<MemSubTable.Snapshot> subTables;

    Snapshot(HashMap<List<Field>, MemSubTable> columns, BufferAllocator allocator) {
      ImmutableList.Builder<MemSubTable.Snapshot> builder = ImmutableList.builder();
      for (Map.Entry<List<Field>, MemSubTable> entry : columns.entrySet()) {
        List<Field> fields = entry.getKey();
        MemSubTable column = entry.getValue();
        builder.add(column.snapshot(fields, allocator));
      }
      this.subTables = columns.entrySet().stream()
          .map(e -> e.getValue().snapshot(e.getKey(), allocator))
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
