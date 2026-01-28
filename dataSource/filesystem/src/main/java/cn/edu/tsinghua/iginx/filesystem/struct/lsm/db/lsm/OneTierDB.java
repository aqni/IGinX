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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.Database;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.buffer.DataBuffer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.buffer.MemTableQueue;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.buffer.chunk.Chunk;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.compact.Flusher;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.storage.StorageManager;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.table.TableIndex;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.table.TableStorage;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.WriteBatches;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.iterator.BatchPlaneScanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.manager.data.ScannerRowStream;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.manager.utils.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.manager.utils.TagKVUtils;
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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.pojo.Field;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.WillClose;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class OneTierDB implements Database {
  private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);

  private final ReadWriteLock deleteLock = new ReentrantReadWriteLock(true);

  private final String name;
  private final Shared shared;
  private final BufferAllocator allocator;
  private final TableIndex tableIndex;
  private final TableStorage tableStorage;
  private final MemTableQueue memTableQueue;
  private final Flusher flusher;

  public OneTierDB(String name, Shared shared, StorageManager readerWriter) throws IOException {
    this.name = name;
    this.shared = shared;
    this.allocator = shared.getAllocator().newChildAllocator(name, 0, Long.MAX_VALUE);
    this.tableIndex = new TableIndex(shared);
    this.tableStorage = new TableStorage(shared, tableIndex, readerWriter);
    this.memTableQueue = new MemTableQueue(shared, allocator);
    this.flusher = new Flusher(name, shared, allocator, memTableQueue, tableStorage);
  }

  @Override
  public RowStream query(List<String> patterns,
                         @Nullable TagFilter tagFilter,
                         Filter filter) throws StorageException, IOException {
    deleteLock.readLock().lock();
    try {
      List<Field> fields = new ArrayList<>(tableIndex.findFields(patterns, tagFilter).values());
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

      List<Scanner<Long, Scanner<String, Object>>> inMemories =
          memTableQueue.scan(fields, rangeSet, allocator);

      Set<String> columnKeySet = new HashSet<>(columnKeys);
      try (AutoCloseable c = AutoCloseables.all(inMemories)) {
        DataBuffer<Long, String, Object> readBuffer =
            tableStorage.query(columnKeySet, rangeSet, filter);
        for (Scanner<Long, Scanner<String, Object>> scanner : inMemories) {
          readBuffer.putRows(scanner);
        }
        Scanner<Long, Scanner<String, Object>> scanner = readBuffer.scanRows(columnKeySet, Range.all());
        return new ScannerRowStream(scanner, schema);
      } catch (Exception e) {
        throw new StorageException(e);
      }
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  @Override
  public Set<Field> schema() throws StorageException {
    deleteLock.readLock().lock();
    try {
      Map<String, DataType> types = tableIndex.getType();
      return ArrowFields.of(types);
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  @Override
  public void upsertRows(
      Scanner<Long, Scanner<String, Object>> scanner, Map<String, DataType> schema)
      throws StorageException, InterruptedException {
    try (Scanner<Long, Scanner<Long, Scanner<String, Object>>> batchScanner =
             new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
      while (batchScanner.iterate()) {
        try (Scanner<Long, Scanner<String, Object>> batch = batchScanner.value()) {
          putAll(WriteBatches.recordOfRows(batch, schema, allocator), schema);
        }
      }
    }
  }

  @Override
  public void upsertColumns(
      Scanner<String, Scanner<Long, Object>> scanner, Map<String, DataType> schema)
      throws StorageException, InterruptedException {
    try (Scanner<Long, Scanner<String, Scanner<Long, Object>>> batchScanner =
             new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
      while (batchScanner.iterate()) {
        try (Scanner<String, Scanner<Long, Object>> batch = batchScanner.value()) {
          putAll(WriteBatches.recordOfColumns(batch, schema, allocator), schema);
        }
      }
    }
  }

  private void putAll(@WillClose Iterable<Chunk.Snapshot> chunks, Map<String, DataType> schema)
      throws TypeConflictedException, InterruptedException {
    deleteLock.readLock().lock();
    try (NoexceptAutoCloseable guarder = NoexceptAutoCloseables.all(chunks)) {
      tableIndex.declareFields(schema);
      memTableQueue.store(chunks);
      if (shared.getStorageProperties().getWriteBufferTimeout().toMillis() <= 0) {
        memTableQueue.flush();
      }
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  @Override
  public void delete(AreaSet<Long, Field> range) throws StorageException {
    AreaSet<Long, String> innerAreas = ArrowFields.toInnerAreas(range);
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to delete {} in {}", range, name);
      memTableQueue.delete(range);
      tableStorage.delete(innerAreas);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  @Override
  public void delete(List<String> patterns, @Nullable TagFilter tagFilter) throws StorageException {
    deleteLock.writeLock().lock();
    try {
      Map<Long, Field> matchedField = tableIndex.findFields(patterns, tagFilter);
      tableIndex.delete(matchedField);

      AreaSet<Long, Field> areas = new AreaSet<>();
      areas.add(new HashSet<>(matchedField.values()));
      AreaSet<Long, String> innerAreas = ArrowFields.toInnerAreas(areas);
      memTableQueue.delete(areas);
      tableStorage.delete(innerAreas);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  @Override
  public void clear() throws StorageException {
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to clear {}", name);
      flusher.stop();
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
