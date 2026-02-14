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

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemTableQueue;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Shared;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;

@NotThreadSafe
public class Flusher {

  private static final Logger LOGGER = LoggerFactory.getLogger(Flusher.class);

  private final String name;
  private final Shared shared;
  private final MemTableQueue memTableQueue;
  private final TableStorage tableStorage;
  private final long idBase = System.nanoTime();

  private ExecutorService flusher = null;
  private ExecutorService flushDispatcher = null;
  private ExecutorService deleter = null;
  private ScheduledExecutorService scheduler = null;

  public Flusher(
      String name,
      Shared shared,
      MemTableQueue memTableQueue,
      TableStorage tableStorage) {
    this.name = name;
    this.shared = shared;
    this.memTableQueue = memTableQueue;
    this.tableStorage = tableStorage;
  }

  public void start() {
    Preconditions.checkState(flusher == null);
    ThreadFactory workerFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-worker-%d").build();
    flusher = Executors.newCachedThreadPool(workerFactory);

    Preconditions.checkState(flushDispatcher == null);
    ThreadFactory dispatcherFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-dispatcher-%d").build();
    flushDispatcher = Executors.newSingleThreadExecutor(dispatcherFactory);
    flushDispatcher.submit(this::dispatchLoop);

    Preconditions.checkState(deleter == null);
    ThreadFactory deleterFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-deleter-%d").build();
    deleter = Executors.newCachedThreadPool(deleterFactory);
    deleter.submit(this::deleteLoop);

    Preconditions.checkState(scheduler == null);
    ThreadFactory schedulerFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-scheduler-%d").build();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(schedulerFactory);
    Duration timeout = shared.getStorageProperties().getWriteBufferTimeout();
    if (!timeout.isZero() && !timeout.isNegative()) {
      LOGGER.info("flusher {} start to force flush every {}", name, timeout);
      this.scheduler.scheduleWithFixedDelay(this::schedule, timeout.getNano(), timeout.getNano(), TimeUnit.NANOSECONDS);
    }
  }

  public void stop() throws InterruptedException {
    scheduler.shutdownNow();
    awaitTermination(scheduler);
    scheduler = null;

    flushDispatcher.shutdownNow();
    awaitTermination(flushDispatcher);
    flushDispatcher = null;

    flusher.shutdown();
    awaitTermination(flusher);
    flusher = null;

    deleter.shutdownNow();
    awaitTermination(deleter);
    deleter = null;
  }

  private void awaitTermination(ExecutorService executor) throws InterruptedException {
    while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
      LOGGER.error("executor {} did not terminate in time, continue waiting", executor);
    }
  }

  private void flush(MemTableQueue.TookTable table) {
    LOGGER.debug("start to flush memtable {}", table.getId());
    try (MemTableQueue.TookTable ignored = table) {
      tableStorage.flush(idBase + table.getId(), table.getMemTable());
    } catch (StorageException | IOException e) {
      table.fail();
      LOGGER.error("flush memtable {} failed", idBase + table.getId(), e);
    }
  }

  private void dispatchLoop() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        MemTableQueue.TookTable table = memTableQueue.takeToFlush();
        flusher.submit(() -> flush(table));
        LOGGER.debug("memtable {} is submit to flush", idBase + table.getId());
      }
    } catch (InterruptedException e) {
      LOGGER.info("flusher {} dispatch loop is cancel", name);
    }
  }

  private void deleteLoop() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        long id = memTableQueue.takeToDelete();
        LOGGER.debug("flushed table {} is need to delete", idBase + id);
        try {
          tableStorage.delete(idBase + id);
        } catch (IOException e) {
          LOGGER.error("delete memtable {} failed, giving up", idBase + id, e);
        }
      }
    } catch (InterruptedException e) {
      LOGGER.info("flusher {} delete loop is cancel", name);
    }
  }

  private void schedule() {
    try {
      memTableQueue.flushAll(false);
    } catch (InterruptedException e) {
      LOGGER.info("flusher {} schedule task is cancel", name);
    }
  }

}
