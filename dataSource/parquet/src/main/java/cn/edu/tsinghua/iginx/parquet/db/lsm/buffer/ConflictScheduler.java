package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer;

import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.Chunk;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import org.apache.arrow.vector.types.pojo.Field;

public class ConflictScheduler {

  private final ConcurrentMap<Field, ExecutorService> executors = new ConcurrentHashMap<>();
  private final Set<Field> touched = Collections.newSetFromMap(new ConcurrentHashMap<>());

  public void reset() {
    // remove all untouched fields
    executors.keySet().removeIf(field -> !touched.contains(field));
    touched.clear();
  }

  private ExecutorService getExecutor(Field field) {
    touched.add(field);
    return executors.computeIfAbsent(field, f -> Executors.newSingleThreadExecutor());
  }

  private ExecutorService getExecutor(Chunk.Snapshot data) {
    return getExecutor(data.getField());
  }

  public void append(MemTable activeTable, Iterable<Chunk.Snapshot> data) {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (Chunk.Snapshot chunk : data) {
      CompletableFuture<Void> future =
          CompletableFuture.runAsync(
              () -> {
                activeTable.store(chunk);
              },
              getExecutor(chunk));
      futures.add(future);
    }
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
  }
}
