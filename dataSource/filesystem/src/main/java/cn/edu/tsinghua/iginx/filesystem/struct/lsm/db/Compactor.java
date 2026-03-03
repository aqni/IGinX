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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;

@NotThreadSafe
public class Compactor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);

  private final String name;
  private final Semaphore flusherPermits;
  private final Duration memTableTimeout;
  private final MemTableQueue memTableQueue;
  private final TableStorage tableStorage;
  private final long idBase = System.nanoTime();

  private ExecutorService flusher = null;
  private ExecutorService flushDispatcher = null;
  private ExecutorService deleter = null;
  private ScheduledExecutorService scheduler = null;

  public Compactor(
      String name,
      Semaphore flusherPermits,
      Duration memTableTimeout,
      MemTableQueue memTableQueue,
      TableStorage tableStorage) {
    this.name = name;
    this.flusherPermits = flusherPermits;
    this.memTableTimeout = memTableTimeout;
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
    flushDispatcher.submit(this::flushLoop);

    Preconditions.checkState(deleter == null);
    ThreadFactory deleterFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-deleter-%d").build();
    deleter = Executors.newCachedThreadPool(deleterFactory);
    deleter.submit(this::deleteLoop);

    Preconditions.checkState(scheduler == null);
    ThreadFactory schedulerFactory =
        new ThreadFactoryBuilder().setNameFormat("flusher-" + name + "-scheduler-%d").build();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(schedulerFactory);
    if (!memTableTimeout.isZero() && !memTableTimeout.isNegative()) {
      LOGGER.info("flusher {} start to force flush every {}", name, memTableTimeout);
      this.scheduler.scheduleWithFixedDelay(this::schedule, memTableTimeout.getNano(), memTableTimeout.getNano(), TimeUnit.NANOSECONDS);
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

  private void flushLoop() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        MemTableQueue.TookTable table = memTableQueue.takeToFlush();
        long tableId = idBase + table.getId();

        flusherPermits.acquireUninterruptibly();
        flusher.submit(() -> {
          try (MemTableQueue.TookTable ignored = table) {
            LOGGER.debug("start to flush table {}", tableId);
            tableStorage.flush(tableId, table.getMemTable(), table.isFinalTable());
          } catch (Throwable e) {
            table.fail();
            LOGGER.error("flush memtable {} failed", tableId, e);
          } finally {
            flusherPermits.release();
            LOGGER.debug("end to flush table {}", tableId);
          }
        });

        LOGGER.debug("memtable {} is submit to flush", tableId);
      }
    } catch (InterruptedException ignored) {
    } catch (Throwable e) {
      LOGGER.error("flusher {} dispatch loop is error", name, e);
    }
    LOGGER.info("flusher {} dispatch loop is exited", name);
  }

  private void deleteLoop() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        long id = memTableQueue.takeToDelete();
        LOGGER.debug("flushed table {} is need to delete", idBase + id);
        try {
          tableStorage.deleteNoCompact(idBase + id);
        } catch (IOException e) {
          LOGGER.error("delete memtable {} failed, giving up", idBase + id, e);
        }
      }
    } catch (InterruptedException ignored) {
    } catch (Throwable e) {
      LOGGER.error("flusher {} delete loop is error", name, e);
    }
    LOGGER.info("flusher {} delete loop is exited", name);
  }

  private void schedule() {
    try {
      memTableQueue.flushAll(false);
    } catch (InterruptedException e) {
      LOGGER.info("flusher {} schedule task is cancel", name);
    } catch (Throwable e) {
      LOGGER.error("flusher {} schedule task is error", name, e);
    }
  }

}
