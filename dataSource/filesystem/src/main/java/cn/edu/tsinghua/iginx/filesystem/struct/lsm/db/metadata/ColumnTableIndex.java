package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.NotIntegrityException;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.arrow.vector.types.Types;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ColumnTableIndex {
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Map<Long, Range<Long>> tableRange = new HashMap<>();
  private final Types.MinorType type;

  public ColumnTableIndex(Types.MinorType type) {
    this.type = type;
  }

  public void addTable(long id, Range<Long> range) {
    lock.writeLock().lock();
    try {
      if (this.tableRange.containsKey(id)) {
        throw new NotIntegrityException("table " + id + " already exists");
      }
      this.tableRange.put(id, range);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Set<Long> find(RangeSet<Long> ranges) {
    Set<Long> result = new HashSet<>();
    lock.readLock().lock();
    try {
      for (Map.Entry<Long, Range<Long>> entry : tableRange.entrySet()) {
        if (ranges.intersects(entry.getValue())) {
          result.add(entry.getKey());
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public void delete(RangeSet<Long> keyRangeSet) {
    lock.writeLock().lock();
    try {
      RangeSet<Long> validRanges = keyRangeSet.complement();
      Iterator<Map.Entry<Long, Range<Long>>> iterator = tableRange.entrySet().iterator();
      Map<Long, Range<Long>> overlap = new HashMap<>();
      while (iterator.hasNext()) {
        Map.Entry<Long, Range<Long>> entry = iterator.next();
        if (!keyRangeSet.intersects(entry.getValue())) {
          continue;
        }
        if (keyRangeSet.encloses(entry.getValue())) {
          iterator.remove();
        } else {
          overlap.put(entry.getKey(), validRanges.subRangeSet(entry.getValue()).span());
        }
      }
      tableRange.putAll(overlap);
    } finally {
      lock.writeLock().unlock();
    }
  }
}
