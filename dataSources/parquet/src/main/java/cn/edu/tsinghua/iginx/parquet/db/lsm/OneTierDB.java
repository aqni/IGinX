/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.db.lsm;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.Database;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Prefetch;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.DataBuffer;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.Table;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableIndex;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.TableStorage;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.BatchPlaneScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.StorageShared;
import cn.edu.tsinghua.iginx.parquet.util.exception.NotIntegrityException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneTierDB<K extends Comparable<K>, F, T, V> implements Database<K, F, T, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OneTierDB.class);
  private final StorageShared shared;
  private final TableStorage<K, F, T, V> tableStorage;
  private final TableIndex<K, F, T, V> tableIndex;
  private final Prefetch prefetch;

  private DataBuffer<K, F, V> writeBuffer = new DataBuffer<>();
  private String previousTableName = null;
  private final AtomicLong bufferDirtiedTime = new AtomicLong(Long.MAX_VALUE);
  private final LongAdder bufferInsertedSize = new LongAdder();
  private final Lock checkLock = new ReentrantLock(false);
  private final ReadWriteLock deleteLock = new ReentrantReadWriteLock(true);
  private final ReadWriteLock commitLock = new ReentrantReadWriteLock(true);
  private final ReadWriteLock storageLock = new ReentrantReadWriteLock(true);

  private final String name;
  private final long timeout;
  private final ScheduledExecutorService scheduler;

  public OneTierDB(
      String name, StorageShared shared, ReadWriter<K, F, T, V> readerWriter, Prefetch prefetch)
      throws IOException {
    this.name = name;
    this.shared = shared;
    this.tableStorage = new TableStorage<>(name, shared, readerWriter);
    this.tableIndex = new TableIndex<>(tableStorage);
    this.prefetch = prefetch;

    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setNameFormat("OneTierDB-" + name + "-%d").build();
    this.timeout = shared.getStorageProperties().getWriteBufferTimeout().toMillis();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
    if (timeout > 0) {
      LOGGER.info("db {} start to check buffer timeout check every {}ms", name, timeout);
      this.scheduler.scheduleWithFixedDelay(
          this::checkBufferTimeout, timeout, timeout, TimeUnit.MILLISECONDS);
    } else {
      LOGGER.info("db {} buffer timeout check is immediately after writing", name);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Scanner<K, Scanner<F, V>> query(Set<F> fields, RangeSet<K> ranges, Filter filter)
      throws StorageException {
    DataBuffer<K, F, V> readBuffer = new DataBuffer<>();

    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    storageLock.readLock().lock();
    try {
      AreaSet<K, F> areas = new AreaSet<>();
      areas.add(fields, ranges);
      Set<String> tables = tableIndex.find(areas);
      List<String> sortedTableNames = new ArrayList<>(tables);
      sortedTableNames.sort(Comparator.naturalOrder());

      QueriedTableSequence<K, F, T, V> sequence =
          new QueriedTableSequence<>(shared, sortedTableNames, tableStorage);
      Prefetch.Prefetcher fetcher = prefetch.prefetch(sequence);
      for (int i = 0; i < sequence.count(); i++) {
        QueriedTableSequence<K, F, T, V>.Entry entry =
            (QueriedTableSequence<K, F, T, V>.Entry) fetcher.get(i);
        Table<K, F, T, V> table = entry.getTable();
        try (Scanner<K, Scanner<F, V>> scanner = table.scan(fields, ranges)) {
          readBuffer.putRows(scanner);
        }
      }

      for (Range<K> range : ranges.asRanges()) {
        readBuffer.putRows(writeBuffer.scanRows(fields, range));
      }
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      storageLock.readLock().unlock();
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }

    return readBuffer.scanRows(fields, Range.all());
  }

  @Override
  public Optional<Range<K>> range() throws StorageException {
    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    storageLock.readLock().lock();
    try {
      TreeRangeSet<K> rangeSet = TreeRangeSet.create();
      for (Range<K> range : tableIndex.ranges().values()) {
        rangeSet.add(range);
      }
      for (Range<K> range : writeBuffer.ranges().values()) {
        rangeSet.add(range);
      }
      if (rangeSet.isEmpty()) {
        return Optional.empty();
      } else {
        return Optional.of(rangeSet.span());
      }
    } finally {
      storageLock.readLock().unlock();
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public Map<F, T> schema() throws StorageException {
    return tableIndex.getType();
  }

  @Override
  public void upsertRows(Scanner<K, Scanner<F, V>> scanner, Map<F, T> schema)
      throws StorageException {
    beforeWriting();
    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    storageLock.readLock().lock();
    try {
      tableIndex.declareFields(schema);
      try (Scanner<Long, Scanner<K, Scanner<F, V>>> batchScanner =
          new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
        while (batchScanner.iterate()) {
          try (Scanner<K, Scanner<F, V>> batch = batchScanner.value()) {
            writeBuffer.putRows(batch);
            bufferInsertedSize.add(batchScanner.key());
          }
        }
      } catch (IOException e) {
        throw new StorageRuntimeException(e);
      }
    } finally {
      updateDirty();
      storageLock.readLock().unlock();
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }
    afterWriting();
  }

  @Override
  public void upsertColumns(Scanner<F, Scanner<K, V>> scanner, Map<F, T> schema)
      throws StorageException {
    beforeWriting();
    commitLock.readLock().lock();
    deleteLock.readLock().lock();
    storageLock.readLock().lock();
    try {
      tableIndex.declareFields(schema);
      try (Scanner<Long, Scanner<F, Scanner<K, V>>> batchScanner =
          new BatchPlaneScanner<>(scanner, shared.getStorageProperties().getWriteBatchSize())) {
        while (batchScanner.iterate()) {
          try (Scanner<F, Scanner<K, V>> batch = batchScanner.value()) {
            writeBuffer.putColumns(batch);
            bufferInsertedSize.add(batchScanner.key());
          }
        }
      } catch (IOException e) {
        throw new StorageRuntimeException(e);
      }
    } finally {
      updateDirty();
      storageLock.readLock().unlock();
      deleteLock.readLock().unlock();
      commitLock.readLock().unlock();
    }
    afterWriting();
  }

  private void updateDirty() {
    long currentTime = System.currentTimeMillis();
    bufferDirtiedTime.updateAndGet(oldTime -> Math.min(oldTime, currentTime));
  }

  private void beforeWriting() {
    checkBufferSize();
  }

  private void afterWriting() {
    if (timeout <= 0) {
      checkBufferTimeout();
    }
  }

  private void checkBufferSize() {
    if (bufferInsertedSize.sum() < shared.getStorageProperties().getWriteBufferSize()) {
      return;
    }
    checkLock.lock();
    try {
      if (bufferInsertedSize.sum() < shared.getStorageProperties().getWriteBufferSize()) {
        return;
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "flushing is triggered when write buffer size {} reaching {}",
            bufferInsertedSize.sum(),
            shared.getStorageProperties().getWriteBufferSize());
      }
      commitMemoryTable(false, new CountDownLatch(1));
    } finally {
      checkLock.unlock();
    }
  }

  private void checkBufferTimeout() {
    CountDownLatch latch = new CountDownLatch(1);
    checkLock.lock();
    try {
      long interval = System.currentTimeMillis() - bufferDirtiedTime.get();
      if (interval < timeout) {
        return;
      }

      LOGGER.debug(
          "flushing is triggered when write buffer dirtied time {}ms reaching {}ms",
          interval,
          timeout);

      commitMemoryTable(true, latch);
    } finally {
      checkLock.unlock();
    }
    LOGGER.debug("waiting for flushing table");
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new StorageRuntimeException(e);
    }
    LOGGER.debug("table is flushed");
  }

  private void commitMemoryTable(boolean temp, CountDownLatch latch) {
    commitLock.writeLock().lock();
    deleteLock.readLock().lock();
    try {
      Map<F, T> types =
          tableIndex.getType(
              writeBuffer.fields(),
              f -> {
                throw new NotIntegrityException("field " + f + " is not found in schema");
              });

      MemoryTable<K, F, T, V> table = new MemoryTable<>(writeBuffer, types);

      String toDelete = previousTableName;
      String committedTableName =
          tableStorage.flush(
              table,
              () -> {
                if (toDelete != null) {
                  storageLock.writeLock().lock();
                  try {
                    LOGGER.debug("delete table {} of {}", toDelete, name);
                    tableIndex.removeTable(toDelete);
                    tableStorage.remove(toDelete);
                  } catch (Throwable e) {
                    LOGGER.error("failed to delete table {}", toDelete, e);
                  } finally {
                    storageLock.writeLock().unlock();
                  }
                  LOGGER.trace("table {} is deleted", toDelete);
                }
                latch.countDown();
              });

      LOGGER.debug(
          "submit table {} to flush, with temp={}, latch={}", committedTableName, temp, latch);
      tableIndex.addTable(committedTableName, table.getMeta());
      this.bufferDirtiedTime.set(Long.MAX_VALUE);
      previousTableName = committedTableName;
      if (!temp) {
        LOGGER.info("reset write buffer");
        this.writeBuffer = new DataBuffer<>();
        bufferInsertedSize.reset();
        previousTableName = null;
      }
    } catch (InterruptedException e) {
      throw new StorageRuntimeException(e);
    } finally {
      deleteLock.readLock().unlock();
      commitLock.writeLock().unlock();
    }
  }

  @Override
  public void delete(AreaSet<K, F> areas) throws StorageException {
    commitLock.readLock().lock();
    deleteLock.writeLock().lock();
    storageLock.readLock().lock();
    try {
      LOGGER.info("start to delete {} in {}", areas, name);
      writeBuffer.remove(areas);
      Set<String> tables = tableIndex.find(areas);
      tableStorage.delete(tables, areas);
      tableIndex.delete(areas);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    } finally {
      storageLock.readLock().unlock();
      deleteLock.writeLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public void clear() throws StorageException {
    commitLock.readLock().lock();
    deleteLock.writeLock().lock();
    try {
      LOGGER.debug("start to clear {}", name);
      tableStorage.clear();
      tableIndex.clear();
      writeBuffer.clear();
      bufferDirtiedTime.set(Long.MAX_VALUE);
      bufferInsertedSize.reset();
    } catch (InterruptedException e) {
      throw new StorageRuntimeException(e);
    } finally {
      deleteLock.writeLock().unlock();
      commitLock.readLock().unlock();
    }
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("flushing is triggered when closing");
    scheduler.shutdown();
    commitMemoryTable(false, new CountDownLatch(1));
    tableStorage.close();
    tableIndex.close();
  }
}
