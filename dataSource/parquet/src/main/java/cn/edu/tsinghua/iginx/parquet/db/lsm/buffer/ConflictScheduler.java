/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer;

import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.Chunk;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.arrow.vector.types.pojo.Field;

public class ConflictScheduler {

  private final ConcurrentHashMap<Field, Lock> locks = new ConcurrentHashMap<>();

  public void reset() {
    locks.clear();
  }

  private Lock getLock(Field field) {
    return locks.computeIfAbsent(field, f -> new ReentrantLock());
  }

  private Lock getLock(Chunk.Snapshot data) {
    return getLock(data.getField());
  }

  public void append(MemTable activeTable, Iterable<Chunk.Snapshot> data) {
    List<Chunk.Snapshot> toStore = new ArrayList<>();
    data.forEach(toStore::add);
    while (!toStore.isEmpty()) {
      List<Chunk.Snapshot> blocked = tryAppend(activeTable, toStore);
      if (blocked.size() * 2 > toStore.size()) {
        Collections.shuffle(toStore);
        awaitAppend(activeTable, toStore);
        break;
      }
      toStore = blocked;
    }
  }

  private List<Chunk.Snapshot> tryAppend(MemTable activeTable, Iterable<Chunk.Snapshot> data) {
    List<Chunk.Snapshot> blocked = new ArrayList<>();

    for (Chunk.Snapshot snapshot : data) {
      Lock lock = getLock(snapshot);
      if (lock.tryLock()) {
        try {
          activeTable.store(snapshot);
        } finally {
          lock.unlock();
        }
      } else {
        blocked.add(snapshot);
      }
    }
    return blocked;
  }

  private void awaitAppend(MemTable activeTable, Iterable<Chunk.Snapshot> data) {
    for (Chunk.Snapshot snapshot : data) {
      Lock lock = getLock(snapshot);
      lock.lock();
      try {
        activeTable.store(snapshot);
      } finally {
        lock.unlock();
      }
    }
  }
}
