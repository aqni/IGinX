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

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table.MemoryTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.AreaSet;
import com.google.common.collect.RangeSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class MemTable implements AutoCloseable {

  private final int maxChunkValueCount;
  private final BufferAllocator allocator;
  private final HashMap<Schema, MemColumnGroup> columns = new HashMap<>();
  private final HashMap<Field, Schema> fieldToSchema = new HashMap<>();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public MemTable(BufferAllocator allocator, int maxChunkValueCount) {
    this.allocator = allocator;
    this.maxChunkValueCount = maxChunkValueCount;
  }

  public void reset() {
    lock.writeLock().lock();
    try {
      columns.values().forEach(MemColumnGroup::close);
      columns.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() {
    reset();
  }

  public Set<Field> getFields() {
    return Collections.unmodifiableSet(columns.keySet());
  }

  public MemoryTable snapshot(
      List<Field> fields, RangeSet<Long> ranges, BufferAllocator allocator) {
    LinkedHashMap<Field, MemColumnGroup.Snapshot> columns = new LinkedHashMap<>();
    lock.readLock().lock();
    try {
      for (Field field : fields) {
        if (columns.containsKey(field)) {
          throw new IllegalArgumentException("Duplicate field: " + field);
        }
        MemColumnGroup column = this.columns.get(field);
        if (column != null) {
          columns.put(field, column.snapshot(ranges, allocator));
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return new MemoryTable(columns);
  }

  public MemoryTable snapshot(BufferAllocator allocator) {
    LinkedHashMap<Field, MemColumnGroup.Snapshot> columns = new LinkedHashMap<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<Field, MemColumnGroup> entry : this.columns.entrySet()) {
        columns.put(entry.getKey(), entry.getValue().snapshot(allocator));
      }
    } finally {
      lock.readLock().unlock();
    }
    return new MemoryTable(columns);
  }

  public void store(Chunk.Snapshot data) {
    lock.readLock().lock();
    try {
      if (closed) {
        throw new IllegalStateException("MemTable is closed");
      }
      MemColumnGroup column =
          columns.computeIfAbsent(
              data.getSchema(),
              key -> new MemColumnGroup(allocator, maxChunkValueCount));
      column.append(data);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void compact() {
    lock.readLock().lock();
    try {
      columns.values().forEach(MemColumnGroup::compact);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void delete(AreaSet<Long, Field> areas) {
    lock.writeLock().lock();
    try {
      for (Field field : areas.getFields()) {
        MemColumnGroup column = columns.remove(field);
        if (column != null) {
          column.close();
        }
      }
      lock.readLock().lock();
    } finally {
      lock.writeLock().unlock();
    }
    try {
      RangeSet<Long> keys = areas.getKeys();
      if (!keys.isEmpty()) {
        columns.values().forEach(column -> column.delete(keys));
      }
      for (Map.Entry<Field, RangeSet<Long>> entry : areas.getSegments().entrySet()) {
        Field field = entry.getKey();
        RangeSet<Long> ranges = entry.getValue();
        MemColumnGroup column = columns.get(field);
        if (column != null) {
          column.delete(ranges);
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }
}
