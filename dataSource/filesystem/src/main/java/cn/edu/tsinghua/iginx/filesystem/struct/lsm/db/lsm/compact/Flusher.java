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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.compact;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.buffer.MemTableQueue;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.table.TableStorage;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Shared;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnegative;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class Flusher implements NoexceptAutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Flusher.class);

  private final String name;
  private final Shared shared;
  private final BufferAllocator allocator;
  private final MemTableQueue memTableQueue;
  private final TableStorage tableStorage;

  private ScheduledExecutorService scheduler;
  private ExecutorService dispatcher;
  private ExecutorService leader;
  private ExecutorService worker;
  private boolean running = false;

  public Flusher(
      String name,
      Shared shared,
      BufferAllocator allocator,
      MemTableQueue memTableQueue,
      TableStorage tableStorage) {
    this.name = name;
    this.shared = shared;
    this.allocator =
        allocator.newChildAllocator(allocator.getName() + "-flusher", 0, Long.MAX_VALUE);
    this.memTableQueue = memTableQueue;
    this.tableStorage = tableStorage;
    start();
  }

  @Override
  public void close() {
    stop();
    allocator.close();
  }

  public void start() {
    Preconditions.checkState(!running, "flusher is already running");

    ThreadFactory expireFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-scheduler-%d").build();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(expireFactory);
    long timeout = shared.getStorageProperties().getWriteBufferTimeout().toMillis();
    if (timeout > 0) {
      LOGGER.info("flusher {} start to force flush every {} ms", name, timeout);
      this.scheduler.scheduleWithFixedDelay(
          handleInterruption(memTableQueue::flush), timeout, timeout, TimeUnit.MILLISECONDS);
    }

    ThreadFactory workerFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-worker-%d").build();
    this.worker = Executors.newCachedThreadPool(workerFactory);

    ThreadFactory leaderFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-leader-%d").build();
    this.leader = Executors.newCachedThreadPool(leaderFactory);

    ThreadFactory dispatcherFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-dispatcher-%d").build();
    this.dispatcher = Executors.newSingleThreadExecutor(dispatcherFactory);
    dispatcher.submit(handleInterruption((this::dispatch)));

    running = true;
  }

  public void stop() {
    Preconditions.checkState(running, "flusher is not running");

    scheduler.shutdownNow();
    dispatcher.shutdownNow();
    worker.shutdownNow();
    leader.shutdownNow();

    try {
      boolean schedulerTerminated = scheduler.awaitTermination(1, TimeUnit.MINUTES);
      boolean dispatcherTerminated = dispatcher.awaitTermination(1, TimeUnit.MINUTES);
      boolean workerTerminated = worker.awaitTermination(1, TimeUnit.MINUTES);
      boolean leaderTerminated = leader.awaitTermination(1, TimeUnit.MINUTES);
      if (!schedulerTerminated || !dispatcherTerminated || !workerTerminated || !leaderTerminated) {
        throw new IllegalStateException("flusher is not terminated");
      }
      running = false;
    } catch (InterruptedException e) {
      LOGGER.debug("flusher is interrupted:", e);
    }
  }

  interface InterruptibleRunnable {
    void run() throws InterruptedException, ExecutionException;
  }

  private Runnable handleInterruption(InterruptibleRunnable runnable) {
    return () -> {
      try {
        runnable.run();
      } catch (InterruptedException e) {
        LOGGER.debug("interrupted", e);
      } catch (Exception e) {
        LOGGER.error("unexpected error", e);
      }
    };
  }

  private void dispatch() throws InterruptedException {
    long memtableIdAtLeast = 0;
    while (!Thread.currentThread().isInterrupted()) {
      long memtableId = memTableQueue.awaitNext(memtableIdAtLeast);
      LOGGER.debug("memtable {} is ready to flush", memtableId);
      CountDownLatch pullNext = new CountDownLatch(1);
      leader.submit(handleInterruption(() -> submitAndWaitFlush(memtableId, pullNext)));
      pullNext.await();
      memtableIdAtLeast = memtableId + 1;
    }
    throw new InterruptedException();
  }

  private void submitAndWaitFlush(@Nonnegative long memtableId, CountDownLatch onSubmit)
      throws InterruptedException, ExecutionException {
    List<String> tableNames = new ArrayList<>();

    LOGGER.debug("start to flush memtable {}", memtableId);

    try (MemoryTable snapshot = memTableQueue.snapshot(memtableId, allocator)) {
      List<Future<List<String>>> futureFlushed = submitToFlushAllField(memtableId, snapshot);

      onSubmit.countDown();

      for (Future<List<String>> future : futureFlushed) {
        tableNames.addAll(future.get());
      }
    }

    LOGGER.debug("memtable {} is flushed to tables {}", memtableId, tableNames);

    memTableQueue.eliminate(
        memtableId,
        tombstone -> {
          for (String tableName : tableNames) {
            tableStorage.commit(tableName, tombstone);
          }
        });

    LOGGER.debug("memtable {} is eliminated", memtableId);
  }

  private List<Future<List<String>>> submitToFlushEachField(long memtableId, MemoryTable snapshot)
      throws InterruptedException {
    List<Future<List<String>>> futureFlushed = new ArrayList<>();

    Field[] fields = snapshot.getFields().toArray(new Field[0]);
    for (int columnNumber = 0; columnNumber < fields.length; columnNumber++) {
      Field field = fields[columnNumber];
      String suffix = String.valueOf(columnNumber);

      shared.getFlusherPermits().acquire();
      Future<List<String>> future =
          worker.submit(
              () -> {
                try {
                  return tableStorage.flush(
                      memtableId, suffix, snapshot.subTable(Collections.singletonList(field)));
                } finally {
                  shared.getFlusherPermits().release();
                }
              });
      futureFlushed.add(future);
    }
    return futureFlushed;
  }

  private List<Future<List<String>>> submitToFlushAllField(long memtableId, MemoryTable snapshot)
      throws InterruptedException {
    shared.getFlusherPermits().acquire();
    Future<List<String>> future =
        worker.submit(
            () -> {
              try {
                return tableStorage.flush(memtableId, "all", snapshot);
              } finally {
                shared.getFlusherPermits().release();
              }
            });
    return Collections.singletonList(future);
  }

  private List<Future<List<String>>> submitToFlushGroupedField(
      long memtableId, MemoryTable snapshot) throws InterruptedException {
    Map<String, List<Field>> groupedFields = new HashMap<>();
    for (Field field : snapshot.getFields()) {
      String name = field.getName();
      String namePrefix = name.contains(".") ? name.substring(0, name.lastIndexOf(".")) : null;
      groupedFields.computeIfAbsent(namePrefix, k -> new ArrayList<>()).add(field);
    }

    List<String> singlePrefixes =
        groupedFields.entrySet().stream()
            .filter(entry -> entry.getValue().size() <= 1)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

    for (String singlePrefix : singlePrefixes) {
      groupedFields
          .computeIfAbsent(null, k -> new ArrayList<>())
          .addAll(groupedFields.remove(singlePrefix));
    }

    List<Future<List<String>>> futureFlushed = new ArrayList<>();
    for (String prefix : groupedFields.keySet()) {
      List<Field> fields = groupedFields.get(prefix);

      shared.getFlusherPermits().acquire();
      Future<List<String>> future =
          worker.submit(
              () -> {
                try {
                  return tableStorage.flush(memtableId, prefix, snapshot.subTable(fields));
                } finally {
                  shared.getFlusherPermits().release();
                }
              });
      futureFlushed.add(future);
    }
    return futureFlushed;
  }
}
