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

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemBatch;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemTableQueue;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.Catalog;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.DataViewWrapper;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseables;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.WriteBatches;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.StorageException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.Shared;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OneTierDB implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);

  private final ReadWriteLock deleteLock = new ReentrantReadWriteLock(true);

  private final Path path;
  private final Shared shared;

  private final BufferAllocator allocator;
  private final Catalog catalog;
  private final TableStorage tableStorage;
  private final MemTableQueue memTableQueue;
  private final Compactor flusher;

  public OneTierDB(Shared shared, Path path) {
    this.path = path;
    this.shared = shared;
    this.catalog = new Catalog(shared.getConfig().getCatalog());
    this.tableStorage = new TableStorage(path, shared.getConfig().getStorage(), catalog, shared.getCachePool());
    this.allocator = shared.getAllocator().newChildAllocator(path.toString(), 0, Long.MAX_VALUE);
    this.memTableQueue = new MemTableQueue(shared.getConfig().getMemtable(), shared.getMemTablePermits(), allocator);
    this.flusher = new Compactor(path.toString(), shared.getFlusherPermits(), shared.getConfig().getMemtable().getTimeout(), memTableQueue, tableStorage);
    flusher.start();
  }

  public RowStream query(List<String> patterns, @Nullable TagFilter tagFilter, Filter filter) throws StorageException {
    String queryDescription = String.format("Query{patterns: %s, tagFilter: %s, filter: %s}", patterns, tagFilter, filter);
    return null;

//    deleteLock.readLock().lock();
//    try () {
//      List<Field> fields = catalog.find(patterns, tagFilter);
//      List<String> columnKeys = fields.stream().map(field -> TagKVUtils.toFullName(ArrowFields.toColumnKey(field))).collect(ImmutableList.toImmutableList());
//
//      Map<String, Field> schema = new HashMap<>();
//      for (int i = 0; i < fields.size(); i++) {
//        schema.put(columnKeys.get(i), fields.get(i));
//      }
//
//      //      Filter projectedFilter = ProjectUtils.project(filter, schemaMatchTags);
//      RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(filter);
//
//      List<Scanner<Long, Scanner<String, Object>>> inMemories;
//      try {
//        inMemories = memTableQueue.scan(fields, rangeSet, allocator);
//      } catch (IOException e) {
//        throw new StorageException(e);
//      }
//
//      Set<String> columnKeySet = new HashSet<>(columnKeys);
//      try (AutoCloseable c = AutoCloseables.all(inMemories)) {
//        DataBuffer<Long, String, Object> readBuffer = tableStorage.query(columnKeySet, rangeSet, filter);
//        for (Scanner<Long, Scanner<String, Object>> scanner : inMemories) {
//          readBuffer.putRows(scanner);
//        }
//        Scanner<Long, Scanner<String, Object>> scanner = readBuffer.scanRows(columnKeySet, Range.all());
//        RowStream rowStream = new ScannerRowStream(scanner, schema);
//        if (!Filters.isTrue(filter)) {
//          rowStream = new FilterRowStreamWrapper(rowStream, filter);
//        }
//        return rowStream;
//      } catch (RuntimeException e) {
//        throw e;
//      } catch (Exception e) {
//        throw new StorageException(e);
//      }
//    } finally {
//      deleteLock.readLock().unlock();
//    }

  }

  public List<Field> schema(List<String> patterns, @Nullable TagFilter tagFilter) throws StorageException {
    deleteLock.readLock().lock();
    try {
      return catalog.find(patterns, tagFilter);
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  public void insert(DataView data) throws StorageException, InterruptedException {
    List<MemBatch.Snapshot> batches = new ArrayList<>();
    try (NoexceptAutoCloseable closer = NoexceptAutoCloseables.all(batches)) {
      DataViewWrapper wrappedData = new DataViewWrapper(data);
      if (wrappedData.isRowData()) {
        try (Scanner<Long, Scanner<String, Object>> scanner = wrappedData.getRowsScanner()) {
          batches.addAll(WriteBatches.recordOfRows(scanner, wrappedData.getSchema(), allocator));
        }
      } else {
        try (Scanner<String, Scanner<Long, Object>> scanner = wrappedData.getColumnsScanner()) {
          batches.addAll(WriteBatches.recordOfColumns(scanner, wrappedData.getSchema(), allocator));
        }
      }

      deleteLock.readLock().lock();
      try {
        List<Field> fields = batches.stream().map(MemBatch.Snapshot::getFieldVectors).flatMap(List::stream).map(FieldVector::getField).collect(ImmutableList.toImmutableList());
        catalog.verifyAndInsertFields(fields);
        memTableQueue.store(batches);
        if (shared.getConfig().getMemtable().getTimeout().toMillis() <= 0) {
          memTableQueue.flushAll(false);
        }
      } finally {
        deleteLock.readLock().unlock();
      }
    }
  }

  public void delete(List<String> patterns, @Nullable TagFilter tagFilter, RangeSet<Long> ranges) throws StorageException, InterruptedException {
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
    } catch (IOException e) {
      throw new StorageException(e);
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  private void deleteRanges(List<Field> fields, RangeSet<Long> keyRangeSet) throws InterruptedException, IOException {
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to delete {} where {} in {}", fields, keyRangeSet, path);
      memTableQueue.flushAll(true);
      catalog.delete(fields, keyRangeSet);
      tableStorage.delete(fields, keyRangeSet);
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  private void deleteFields(List<Field> fields) throws StorageException, IOException, InterruptedException {
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to delete {} in {}", fields, path);
      memTableQueue.flushAll(true);
      catalog.delete(fields);
      tableStorage.delete(fields);
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  private void clear() throws InterruptedException {
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to clear {}", path);
      flusher.stop();
      catalog.clear();
      memTableQueue.clear();
      tableStorage.clear();
      if (allocator.getAllocatedMemory() > 0) {
        throw new IllegalStateException("allocator is not empty: " + allocator.toVerboseString());
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("cleared {}, allocator: {}", path, allocator);
      }
      flusher.start();
    } finally {
      deleteLock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws InterruptedException {
    deleteLock.writeLock().lock();
    try {
      if (shared.getConfig().isFlushOnClose()) {
        memTableQueue.flushAll(true);
      }
      flusher.stop();
      memTableQueue.close();
      allocator.close();
    } finally {
      deleteLock.writeLock().unlock();
    }
  }
}
