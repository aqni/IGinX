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

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table.InMemoryTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Awaitable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Shared;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.RangeSet;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongConsumer;

@ThreadSafe
public class MemTableQueue implements NoexceptAutoCloseable {
  private final Logger LOGGER = LoggerFactory.getLogger(MemTableQueue.class);

  private final ReentrantReadWriteLock queueLock = new ReentrantReadWriteLock();

  private final BlockingQueue<Long> toFlushIds = new PriorityBlockingQueue<>();
  private final NavigableMap<Long, ArchivedMemTable> archives = new TreeMap<>();
  private final BufferAllocator allocator;
  private final ActiveMemTable active;

  public MemTableQueue(Shared shared, BufferAllocator allocator) {
    String allocatorName = String.join("-", allocator.getName(), MemTableQueue.class.getSimpleName());
    this.allocator = allocator.newChildAllocator(allocatorName, 0, Long.MAX_VALUE);
    this.active = new ActiveMemTable(shared, this.allocator);
  }

  public void store(Iterable<MemBatch.Snapshot> data) throws InterruptedException {
    queueLock.writeLock().lock();
    try {
      if (active.isOverloaded()) {
        archive(true);
      }
    } finally {
      queueLock.writeLock().unlock();
    }
    active.store(data);
  }

  private void archive(boolean createNewTable) throws InterruptedException {
    queueLock.writeLock().lock();
    try {
      Map<Long, ArchivedMemTable> tables = active.archive(createNewTable);
      archives.putAll(tables);
      toFlushIds.addAll(tables.keySet());
    } finally {
      queueLock.writeLock().unlock();
    }
  }

  public void flushAll(boolean compact) throws InterruptedException {
    List<Awaitable> waiters = new ArrayList<>();
    queueLock.writeLock().lock();
    try {
      archive(compact);
      archives.values().forEach(archivedMemTable -> waiters.add(archivedMemTable::waitUntilClosed));
    } finally {
      queueLock.writeLock().unlock();
    }
    for (Awaitable waiter : waiters) {
      waiter.await();
    }
  }

  public TookTable takeToFlush() throws InterruptedException {
    long id = toFlushIds.take();
    queueLock.writeLock().lock();
    try {
      ArchivedMemTable archivedMemTable = archives.get(id);
      MemTable.Snapshot snapshot = archivedMemTable.getMemTable().snapshot(allocator);
      return new TookTable(id, InMemoryTable.of(snapshot), toFlushIds::add, this::remove);
    } finally {
      queueLock.writeLock().unlock();
    }
  }

  private void remove(long id) {
    queueLock.writeLock().lock();
    try (ArchivedMemTable ignored = archives.remove(id)) {
      active.onTableFlushed(id);
    } finally {
      queueLock.writeLock().unlock();
    }
  }

  public long takeToDelete() throws InterruptedException {
    return active.takeToDelete();
  }

  public List<InMemoryTable> snapshot(List<Field> fields, RangeSet<Long> ranges, BufferAllocator allocator) {
    List<MemTable.Snapshot> snapshots = new ArrayList<>();
    queueLock.readLock().lock();
    try {
      for (ArchivedMemTable archivedMemTable : archives.values()) {
        snapshots.add(archivedMemTable.getMemTable().snapshot(fields, allocator));
      }
      snapshots.add(active.snapshot(fields, allocator));
    } finally {
      queueLock.readLock().unlock();
    }
    return snapshots.stream().map(s -> InMemoryTable.of(s, ranges)).collect(ImmutableList.toImmutableList());
  }

  public void clear() {
    queueLock.writeLock().lock();
    try {
      toFlushIds.clear();
      archives.values().forEach(ArchivedMemTable::close);
      archives.clear();
      active.clear();
    } finally {
      queueLock.writeLock().unlock();
    }
  }

  @Override
  public void close() {
    queueLock.writeLock().lock();
    try {
      clear();
      active.close();
      allocator.close();
    } finally {
      queueLock.writeLock().unlock();
    }
  }

  static class ArchivedMemTable implements NoexceptAutoCloseable {
    private final MemTable memTable;
    private final Collection<NoexceptAutoCloseable> onClose;
    private final CountDownLatch latch = new CountDownLatch(1);

    public ArchivedMemTable(@WillCloseWhenClosed MemTable memTable, @WillCloseWhenClosed Collection<NoexceptAutoCloseable> onClose) {
      this.memTable = Preconditions.checkNotNull(memTable);
      this.onClose = new ArrayList<>(onClose);
    }

    public MemTable getMemTable() {
      return memTable;
    }

    public void waitUntilClosed() throws InterruptedException {
      latch.await();
    }

    @Override
    public void close() {
      latch.countDown();
      onClose.forEach(NoexceptAutoCloseable::close);
    }
  }

  static class ActiveMemTable {
    private final ReentrantReadWriteLock switchTableLock = new ReentrantReadWriteLock(true);

    private final Shared shared;
    private final BufferAllocator allocator;

    private long currentId = 0;
    private BufferAllocator activeAllocator;
    private MemTable activeTable;
    private volatile boolean activeTableWritten;
    private final NavigableSet<Long> duplicateIds = new TreeSet<>();
    private final BlockingQueue<Long> toDeleteIds = new PriorityBlockingQueue<>();

    ActiveMemTable(Shared shared, BufferAllocator allocator) {
      this.shared = Preconditions.checkNotNull(shared);
      this.allocator = Preconditions.checkNotNull(allocator);
      createNewMemtable();
    }

    private void createNewMemtable() {
      String name = String.join("-", allocator.getName(), MemTable.class.getSimpleName(), String.valueOf(currentId));
      activeAllocator = allocator.newChildAllocator(name, 0, Long.MAX_VALUE);
      activeTable = new MemTable(activeAllocator, shared.getStorageProperties().getWriteBufferChunkValuesMax());
      activeTableWritten = false;
    }

    public boolean isOverloaded() {
      switchTableLock.readLock().lock();
      try {
        return activeAllocator.getAllocatedMemory() >= shared.getStorageProperties().getWriteBufferSize();
      } finally {
        switchTableLock.readLock().unlock();
      }
    }

    public void store(Iterable<MemBatch.Snapshot> data) {
      switchTableLock.readLock().lock();
      try {
        for (MemBatch.Snapshot snapshot : data) {
          activeTable.append(snapshot);
        }
        activeTableWritten = true;
      } finally {
        switchTableLock.readLock().unlock();
      }
    }

    public Map<Long, ArchivedMemTable> archive(boolean createNewTable) throws InterruptedException {
      switchTableLock.writeLock().lock();
      try {
        if (!activeTableWritten) {
          return Collections.emptyMap();
        }
        List<NoexceptAutoCloseable> onClose = new ArrayList<>();
        shared.getMemTablePermits().acquire();
        onClose.add(() -> shared.getMemTablePermits().release());

        Map<Long, ArchivedMemTable> result = new HashMap<>();
        long archiveId = currentId++;
        result.put(archiveId, new ArchivedMemTable(activeTable, onClose));

        if (createNewTable) {
          onClose.add(activeTable);
          onClose.add(activeAllocator::close);
          createNewMemtable();
        } else {
          duplicateIds.add(archiveId);
        }
        return result;
      } finally {
        switchTableLock.writeLock().unlock();
      }
    }

    public MemTable.Snapshot snapshot(List<Field> fields, BufferAllocator allocator) {
      switchTableLock.readLock().lock();
      try {
        return activeTable.snapshot(fields, allocator);
      } finally {
        switchTableLock.readLock().unlock();
      }
    }

    public long takeToDelete() throws InterruptedException {
      return toDeleteIds.take();
    }

    public void onTableFlushed(long id) {
      switchTableLock.writeLock().lock();
      try {
        Set<Long> toDeleteIds = duplicateIds.subSet(Long.MIN_VALUE, id);
        this.toDeleteIds.addAll(toDeleteIds);
        toDeleteIds.clear();
      } finally {
        switchTableLock.writeLock().unlock();
      }
    }

    public void clear() {
      switchTableLock.writeLock().lock();
      try {
        activeTable.close();
        activeAllocator.close();
        currentId = 0;
        createNewMemtable();
        duplicateIds.clear();
        toDeleteIds.clear();
      } finally {
        switchTableLock.writeLock().unlock();
      }
    }

    public void close() {
      switchTableLock.writeLock().lock();
      try {
        clear();
        activeTable.close();
        activeAllocator.close();
      } finally {
        switchTableLock.writeLock().unlock();
      }
    }
  }

  public static class TookTable implements NoexceptAutoCloseable {

    private final long id;
    private final InMemoryTable table;
    private final LongConsumer onFailure;
    private final LongConsumer onSuccess;
    private boolean failed = false;

    TookTable(long id, @WillCloseWhenClosed InMemoryTable memTable, LongConsumer onFailure, LongConsumer onSuccess) {
      this.id = id;
      this.table = memTable;
      this.onFailure = onFailure;
      this.onSuccess = onSuccess;
    }

    public long getId() {
      return id;
    }

    public InMemoryTable getMemTable() {
      return table;
    }

    public void fail() {
      failed = true;
    }

    @Override
    public void close() {
      table.close();
      if (failed) {
        onFailure.accept(id);
      } else {
        onSuccess.accept(id);
      }
    }
  }
}
