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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db;

import cn.edu.tsinghua.iginx.engine.shared.data.read.FilterRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.DataBuffer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemTableQueue;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.chunk.Chunk;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.catalog.Catalog;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.StorageManager;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.*;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.scanner.BatchPlaneScanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.scanner.Scanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.NoexceptAutoCloseables;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Shared;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneTierDB implements Database {
  private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);

  private final ReadWriteLock deleteLock = new ReentrantReadWriteLock(true);

  private final String name;
  private final Shared shared;
  private final BufferAllocator allocator;
  private final Catalog catalog;
  private final TableStorage tableStorage;
  private final MemTableQueue memTableQueue;
  private final Flusher flusher;

  public OneTierDB(String name, Shared shared, StorageManager readerWriter) throws IOException {
    this.name = name;
    this.shared = shared;
    this.allocator = shared.getAllocator().newChildAllocator(name, 0, Long.MAX_VALUE);
    this.catalog = new Catalog(shared);
    this.tableStorage = new TableStorage(shared, catalog, readerWriter);
    this.memTableQueue = new MemTableQueue(shared, allocator);
    this.flusher = new Flusher(name, shared, allocator, memTableQueue, tableStorage);
  }

  @Override
  public RowStream query(List<String> patterns, @Nullable TagFilter tagFilter, Filter filter)
      throws StorageException {
    deleteLock.readLock().lock();
    try {
      List<Field> fields = catalog.find(patterns, tagFilter);
      List<String> columnKeys =
          fields.stream()
              .map(field -> TagKVUtils.toFullName(ArrowFields.toColumnKey(field)))
              .collect(Collectors.toList());

      Map<String, Field> schema = new HashMap<>();
      for (int i = 0; i < fields.size(); i++) {
        schema.put(columnKeys.get(i), fields.get(i));
      }

      //      Filter projectedFilter = ProjectUtils.project(filter, schemaMatchTags);
      RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(filter);

      List<Scanner<Long, Scanner<String, Object>>> inMemories;
      try {
        inMemories = memTableQueue.scan(fields, rangeSet, allocator);
      } catch (IOException e) {
        throw new StorageException(e);
      }

      Set<String> columnKeySet = new HashSet<>(columnKeys);
      try (AutoCloseable c = AutoCloseables.all(inMemories)) {
        DataBuffer<Long, String, Object> readBuffer =
            tableStorage.query(columnKeySet, rangeSet, filter);
        for (Scanner<Long, Scanner<String, Object>> scanner : inMemories) {
          readBuffer.putRows(scanner);
        }
        Scanner<Long, Scanner<String, Object>> scanner =
            readBuffer.scanRows(columnKeySet, Range.all());
        RowStream rowStream = new ScannerRowStream(scanner, schema);
        if (!Filters.isTrue(filter)) {
          rowStream = new FilterRowStreamWrapper(rowStream, filter);
        }
        return rowStream;
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new StorageException(e);
      }
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  @Override
  public List<Field> schema(List<String> patterns, @Nullable TagFilter tagFilter)
      throws StorageException {
    deleteLock.readLock().lock();
    try {
      return catalog.find(patterns, tagFilter);
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  @Override
  public void insert(DataView data) throws StorageException {
    DataViewWrapper wrappedData = new DataViewWrapper(data);
    try {
      if (wrappedData.isRowData()) {
        try (Scanner<Long, Scanner<String, Object>> scanner = wrappedData.getRowsScanner()) {
          try (Scanner<Long, Scanner<Long, Scanner<String, Object>>> batchScanner =
              new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
            while (batchScanner.iterate()) {
              try (Scanner<Long, Scanner<String, Object>> batch = batchScanner.value()) {
                putAll(
                    WriteBatches.recordOfRows(batch, wrappedData.getSchema(), allocator),
                    wrappedData.getSchema());
              }
            }
          }
        }
      } else {
        try (Scanner<String, Scanner<Long, Object>> scanner = wrappedData.getColumnsScanner()) {
          try (Scanner<Long, Scanner<String, Scanner<Long, Object>>> batchScanner =
              new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
            while (batchScanner.iterate()) {
              try (Scanner<String, Scanner<Long, Object>> batch = batchScanner.value()) {
                putAll(
                    WriteBatches.recordOfColumns(batch, wrappedData.getSchema(), allocator),
                    wrappedData.getSchema());
              }
            }
          }
        }
      }
    } catch (InterruptedException e) {
      throw new StorageException(e);
    }
  }

  private void putAll(@WillClose Iterable<Chunk.Snapshot> chunks, Map<String, DataType> schema)
      throws TypeConflictedException, InterruptedException {
    deleteLock.readLock().lock();
    try (NoexceptAutoCloseable guarder = NoexceptAutoCloseables.all(chunks)) {
      List<Field> fields = ArrowFields.fromIginxSchema(schema);
      catalog.verifyAndInsertFields(fields);
      memTableQueue.store(chunks);
      if (shared.getStorageProperties().getWriteBufferTimeout().toMillis() <= 0) {
        memTableQueue.flush();
      }
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  @Override
  public void delete(List<String> patterns, @Nullable TagFilter tagFilter, RangeSet<Long> ranges)
      throws StorageException {
    deleteLock.writeLock().lock();
    try {
      List<Field> fields = catalog.findFields(patterns, tagFilter);
      if (ranges.encloses(Range.all())) {
        if (Patterns.isAll(patterns) && tagFilter == null) {
          clear();
        } else {
          deleteFields(fields);
        }
      } else {
        deleteRanges(fields, ranges);
      }
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  private void deleteRanges(List<Field> fields, RangeSet<Long> ranges) {
    AreaSet<Long, Field> areaSet = new AreaSet<>();
    areaSet.add(new HashSet<>(fields), ranges);
    AreaSet<Long, String> innerAreas = ArrowFields.toInnerAreas(areaSet);
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to delete {} in {}", areaSet, name);
      memTableQueue.delete(areaSet);
      catalog.delete(innerAreas);
      tableStorage.delete(innerAreas);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  private void deleteFields(List<Field> fields) throws StorageException {
    deleteLock.writeLock().lock();
    try {
      catalog.delete(fields);
      AreaSet<Long, Field> areas = new AreaSet<>();
      areas.add(new HashSet<>(fields));
      AreaSet<Long, String> innerAreas = ArrowFields.toInnerAreas(areas);
      memTableQueue.delete(areas);
      tableStorage.delete(innerAreas);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  private void clear() {
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to clear {}", name);
      flusher.stop();
      catalog.clear();
      memTableQueue.clear();
      tableStorage.clear();
      if (allocator.getAllocatedMemory() > 0) {
        throw new IllegalStateException("allocator is not empty: " + allocator.toVerboseString());
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("cleared {}, allocator: {}", name, allocator);
      }
      flusher.start();
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws Exception {
    deleteLock.writeLock().lock();
    try {
      if (shared.getStorageProperties().toFlushOnClose()) {
        memTableQueue.flush();
      }
      flusher.close();
      memTableQueue.close();
      tableStorage.close();
      allocator.close();
    } finally {
      deleteLock.writeLock().unlock();
    }
  }
}
