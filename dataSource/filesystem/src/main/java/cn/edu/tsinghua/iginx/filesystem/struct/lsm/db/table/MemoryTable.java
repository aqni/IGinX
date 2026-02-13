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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemSubTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.StorageManager;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.TagKVUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.scanner.*;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.scanner.Scanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.SingleCache;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryTable implements Table, NoexceptAutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryTable.class);

  private final LinkedHashMap<Field, MemSubTable.Snapshot> columns;
  private final Map<String, Field> fieldMap = new HashMap<>();
  private final SingleCache<StorageManager.TableMeta> meta =
      new SingleCache<>(() -> new MemoryTableMeta(getSchema(), getRanges(), getCounts()));

  public MemoryTable(@WillCloseWhenClosed LinkedHashMap<Field, MemSubTable.Snapshot> columns) {
    this.columns = new LinkedHashMap<>(columns);
    for (Field field : columns.keySet()) {
      fieldMap.put(getFieldString(field), field);
    }
  }

  public static MemoryTable empty() {
    return new MemoryTable(new LinkedHashMap<>());
  }

  public Set<Field> getFields() {
    return columns.keySet();
  }

  public boolean isEmpty() {
    return columns.isEmpty();
  }

  private Map<String, DataType> getSchema() {
    return (Map) ArrowFields.toIginxSchema(columns.keySet());
  }

  private Map<String, Range<Long>> getRanges() {
    return columns.keySet().stream()
        .collect(Collectors.toMap(this::getFieldString, this::getRange));
  }

  private Map<String, Long> getCounts() {
    // TODO: give statistics
    return Collections.emptyMap();
  }

  private String getFieldString(Field field) {
    return TagKVUtils.toFullName(ArrowFields.toColumnKey(field));
  }

  private Range<Long> getRange(Field field) {
    MemSubTable.Snapshot snapshot = columns.get(field);
    RangeSet<Long> ranges = snapshot.getRanges();
    if (ranges.isEmpty()) {
      return Range.closed(0L, 0L);
    }
    return ranges.span();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MemoryTable.class.getSimpleName() + "[", "]")
        .add("meta=" + meta)
        .toString();
  }

  @Override
  public StorageManager.TableMeta getMeta() {
    return meta.get();
  }

  @Override
  public Scanner<Long, Scanner<String, Object>> scan(
      Set<String> fields, RangeSet<Long> ranges, @Nullable Filter superSetPredicate) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("read {} where {} from {}", fields, ranges, meta);
    }
    Map<String, Scanner<Long, Object>> columns = new HashMap<>();
    for (String field : fields) {
      if (!fieldMap.containsKey(field)) {
        continue;
      }
      Field arrowField = fieldMap.get(field);
      MemSubTable.Snapshot snapshot = this.columns.get(arrowField);
      columns.put(field, scan(snapshot, ranges));
    }
    return new ColumnUnionRowScanner<>(columns);
  }

  private Scanner<Long, Object> scan(MemSubTable.Snapshot snapshot, RangeSet<Long> ranges) {
    MemSubTable.Snapshot sliced = snapshot.slice(ranges);
    return new ListenCloseScanner<>(new IteratorScanner<>(sliced.iterator()), sliced::close);
  }

  @Override
  public void close() {
    columns.values().forEach(MemSubTable.Snapshot::close);
    columns.clear();
  }

  public MemoryTable subTable(List<Field> fields) {
    LinkedHashMap<Field, MemSubTable.Snapshot> subColumns = new LinkedHashMap<>();
    for (Field field : fields) {
      subColumns.put(field, columns.get(field));
    }
    return new MemoryTable(subColumns) {
      @Override
      public void close() {
        // do nothing
      }
    };
  }

  public static class MemoryTableMeta implements StorageManager.TableMeta {

    private final Map<String, DataType> schema;
    private final Map<String, Range<Long>> ranges;
    private final Map<String, Long> counts;

    MemoryTableMeta(
        Map<String, DataType> schema, Map<String, Range<Long>> ranges, Map<String, Long> counts) {
      this.schema = Collections.unmodifiableMap(schema);
      this.ranges = Collections.unmodifiableMap(ranges);
      this.counts = Collections.unmodifiableMap(counts);
    }

    public Map<String, DataType> getSchema() {
      return schema;
    }

    @Override
    public Range<Long> getRange(String field) {
      if (!schema.containsKey(field)) {
        throw new NoSuchElementException();
      }
      return Objects.requireNonNull(ranges.get(field));
    }

    @Override
    public Long getValueCount(String field) {
      if (!schema.containsKey(field)) {
        throw new NoSuchElementException();
      }
      return counts.get(field);
    }

    public Map<String, Range<Long>> getRanges() {
      return ranges;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", MemoryTableMeta.class.getSimpleName() + "[", "]")
          .add("schema=" + schema)
          .add("ranges=" + ranges)
          .toString();
    }
  }
}

//    public Iterator<LongObjectPair<Object[]>> scan(RangeSet<Long> ranges) {
//      @SuppressWarnings("unchecked")
//      Iterator<LongIntPair>[] iterators = Arrays.stream(chunks).map(c -> c.iterator(ranges)).toArray(Iterator[]::new);
//
//      return new Iterator<LongObjectPair<Object[]>>() {
//        private final LongIntPair[] currentValues = new LongIntPair[iterators.length];
//        private final IntPriorityQueue iteratorHeap = new IntHeapPriorityQueue(currentValues.length, IntComparator.comparingLong(i -> currentValues[i].leftLong()).thenComparing(IntComparators.NATURAL_COMPARATOR));
//        private LongObjectPair<Object[]> nextResult;
//
//        {
//          for (int i = 0; i < iterators.length; i++) {
//            if (iterators[i].hasNext()) {
//              currentValues[i] = iterators[i].next();
//              iteratorHeap.enqueue(i);
//            }
//          }
//          advance();
//        }
//
//        private void advance() {
//          if (iteratorHeap.isEmpty()) {
//            nextResult = null;
//            return;
//          }
//
//          long minKey = currentValues[iteratorHeap.firstInt()].leftLong();
//          Object[] row = new Object[fields.size()];
//          do {
//            int iterIdx = iteratorHeap.dequeueInt();
//            chunks[iterIdx].fillRowWithOriginIndex(currentValues[iterIdx].rightInt(), row);
//            if (iterators[iterIdx].hasNext()) {
//              currentValues[iterIdx] = iterators[iterIdx].next();
//              iteratorHeap.enqueue(iterIdx);
//            }
//          } while (!iteratorHeap.isEmpty() && currentValues[iteratorHeap.firstInt()].leftLong() == minKey);
//          nextResult = new LongObjectImmutablePair<>(minKey, row);
//        }
//
//        @Override
//        public boolean hasNext() {
//          return nextResult != null;
//        }
//
//        @Override
//        public LongObjectPair<Object[]> next() {
//          if (!hasNext()) {
//            throw new NoSuchElementException();
//          }
//          LongObjectPair<Object[]> result = nextResult;
//          advance();
//          return result;
//        }
//      };
//    }

//    Iterator<LongIntPair> iterator(RangeSet<Long> keyRanges) {
//      RangeSet<Long> intersected = keyRanges.subRangeSet(keyRange);
//      return new Iterator<LongIntPair>() {
//
//        private int nextIndex = 0;
//        private LongIntPair nextValue;
//
//        {
//          advance();
//        }
//
//        @Override
//        public boolean hasNext() {
//          return nextValue != null;
//        }
//
//        @Override
//        public LongIntPair next() {
//          if (!hasNext()) {
//            throw new NoSuchElementException();
//          }
//          LongIntPair result = nextValue;
//          advance();
//          return result;
//        }
//
//        private void advance() {
//          while (nextIndex < getValueCount()) {
//            int originIndex = getOriginIndex(nextIndex);
//            nextIndex++;
//            long key = snapshot.getKey(originIndex);
//            if (intersected.contains(key)) {
//              nextValue = new LongIntImmutablePair(key, originIndex);
//              return;
//            }
//          }
//          nextValue = null;
//        }
//      };
//    }
