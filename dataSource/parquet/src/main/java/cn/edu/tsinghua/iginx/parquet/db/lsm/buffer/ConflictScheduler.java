package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer;

import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.Chunk;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    List<Chunk.Snapshot> blocked = tryAppend(activeTable, data);

    Collections.shuffle(blocked);

    awaitAppend(activeTable, blocked);
  }

  private List<Chunk.Snapshot> tryAppend(MemTable activeTable, Iterable<Chunk.Snapshot> data) {
    List<Chunk.Snapshot> blocked = new ArrayList<>();

    for (Chunk.Snapshot snapshot : data) {
      Lock lock = getLock(snapshot);
      if(lock.tryLock()) {
        try {
          activeTable.store(snapshot);
        } finally {
          lock.unlock();
        }
      }else{
        blocked.add(snapshot);
      }
    }
    return blocked;
  }

  private void awaitAppend(MemTable activeTable, Iterable<Chunk.Snapshot> data) {
    for(Chunk.Snapshot snapshot : data) {
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
