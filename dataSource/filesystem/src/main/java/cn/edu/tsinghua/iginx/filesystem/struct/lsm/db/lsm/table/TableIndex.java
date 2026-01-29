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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.schema.FieldIndex;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.StorageManager;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Shared;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.NotIntegrityException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.arrow.vector.types.pojo.Field;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableIndex {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableIndex.class);
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

  private final Map<String, ColumnIndex> indexes = new HashMap<>();
  private final FieldIndex fieldIndex;

  public TableIndex(Shared shared, FieldIndex fieldIndex) {
    this.fieldIndex = fieldIndex;
  }

  public Set<String> find(AreaSet<Long, String> areas) {
    Set<String> result = new HashSet<>();
    lock.readLock().lock();
    try {
      RangeSet<Long> rangeSet = areas.getKeys();
      if (!rangeSet.isEmpty()) {
        for (ColumnIndex columnIndex : indexes.values()) {
          Set<String> tables = columnIndex.find(rangeSet);
          result.addAll(tables);
        }
      }
      for (String field : areas.getFields()) {
        ColumnIndex columnIndex = indexes.get(field);
        if (columnIndex == null) {
          continue;
        }
        Set<String> tables = columnIndex.find();
        result.addAll(tables);
      }
      for (Map.Entry<String, RangeSet<Long>> entry : areas.getSegments().entrySet()) {
        ColumnIndex columnIndex = indexes.get(entry.getKey());
        if (columnIndex == null) {
          continue;
        }
        Set<String> tables = columnIndex.find(entry.getValue());
        result.addAll(tables);
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public Map<String, Range<Long>> ranges() {
    Map<String, Range<Long>> result = new HashMap<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<String, ColumnIndex> entry : indexes.entrySet()) {
        RangeSet<Long> rangeSet = entry.getValue().ranges();
        if (!rangeSet.isEmpty()) {
          result.put(entry.getKey(), rangeSet.span());
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public void declareFields(Map<String, DataType> schema) throws TypeConflictedException {
    List<Field> fields = ArrowFields.fromIginxSchema(schema);
    boolean hasNewField = false;
    lock.readLock().lock();
    try {
      for (Field field : fields) {
        if (!fieldIndex.contain(field)) {
          hasNewField = true;
          break;
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    if (hasNewField) {
      lock.writeLock().lock();
      try {
        for (Field field : fields) {
          fieldIndex.add(field);
        }
        for (Field field : fields) {
          String fieldName = ArrowFields.toFullName(field);
          DataType type = schema.get(fieldName);
          if (!indexes.containsKey(fieldName)) {
            indexes.put(fieldName, new ColumnIndex());
          }
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  public List<Field> findFields(List<String> patterns, @Nullable TagFilter tagFilter) {
    lock.readLock().lock();
    try {
      return fieldIndex.find(patterns, tagFilter);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void addTable(String name, StorageManager.TableMeta meta) {
    lock.readLock().lock();
    try {
      Map<String, DataType> types = meta.getSchema();
      for (String field : types.keySet()) {
        ColumnIndex columnIndex = indexes.get(field);
        if (columnIndex == null) {
          throw new NotIntegrityException("field " + field + " is not found in schema");
        }
        Range<Long> range = meta.getRange(field);
        columnIndex.addTable(name, Objects.requireNonNull(range));
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public void delete(List<Field> fields) throws TypeConflictedException {
    lock.writeLock().lock();
    try {
      for (Field field : fields) {
        fieldIndex.delete(field);
        String fullFieldName = ArrowFields.toFullName(field);
        indexes.remove(fullFieldName);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void delete(AreaSet<Long, String> areas) {
    lock.writeLock().lock();
    try {
      if (!areas.getFields().isEmpty()) {
        throw new IllegalStateException("cannot delete whole fields from table index");
      }
      for (ColumnIndex columnIndex : indexes.values()) {
        columnIndex.delete(areas.getKeys());
      }
      for (Map.Entry<String, RangeSet<Long>> entry : areas.getSegments().entrySet()) {
        ColumnIndex columnIndex = indexes.get(entry.getKey());
        if (columnIndex != null) {
          columnIndex.delete(entry.getValue());
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void clear() {
    lock.writeLock().lock();
    try {
      indexes.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public static class ColumnIndex {
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<String, Range<Long>> tableRange = new HashMap<>();

    public void addTable(String name, Range<Long> range) {
      lock.writeLock().lock();
      try {
        if (this.tableRange.containsKey(name)) {
          throw new NotIntegrityException("table " + name + " already exists");
        }
        this.tableRange.put(name, range);
      } finally {
        lock.writeLock().unlock();
      }
    }

    public void removeTable(String name) {
      lock.writeLock().lock();
      try {
        this.tableRange.remove(name);
      } finally {
        lock.writeLock().unlock();
      }
    }

    public Set<String> find(RangeSet<Long> ranges) {
      Set<String> result = new HashSet<>();
      lock.readLock().lock();
      try {
        for (Map.Entry<String, Range<Long>> entry : tableRange.entrySet()) {
          if (ranges.intersects(entry.getValue())) {
            result.add(entry.getKey());
          }
        }
      } finally {
        lock.readLock().unlock();
      }
      return result;
    }

    public Set<String> find() {
      Set<String> result = new HashSet<>();
      lock.readLock().lock();
      try {
        result.addAll(tableRange.keySet());
      } finally {
        lock.readLock().unlock();
      }
      return result;
    }

    public void delete(RangeSet<Long> ranges) {
      lock.writeLock().lock();
      try {
        RangeSet<Long> validRanges = ranges.complement();
        Iterator<Map.Entry<String, Range<Long>>> iterator = tableRange.entrySet().iterator();
        Map<String, Range<Long>> overlap = new HashMap<>();
        while (iterator.hasNext()) {
          Map.Entry<String, Range<Long>> entry = iterator.next();
          if (!ranges.intersects(entry.getValue())) {
            continue;
          }
          if (ranges.encloses(entry.getValue())) {
            iterator.remove();
          } else {
            overlap.put(entry.getKey(), validRanges.subRangeSet(entry.getValue()).span());
          }
        }
        tableRange.putAll(overlap);
      } finally {
        lock.writeLock().unlock();
      }
    }

    public RangeSet<Long> ranges() {
      TreeRangeSet<Long> rangeSet = TreeRangeSet.create();
      lock.readLock().lock();
      try {
        for (Range<Long> range : tableRange.values()) {
          rangeSet.add(range);
        }
      } finally {
        lock.readLock().unlock();
      }
      return ImmutableRangeSet.copyOf(rangeSet);
    }
  }
}
