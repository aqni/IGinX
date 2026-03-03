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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.table;

import cn.edu.tsinghua.iginx.engine.shared.data.read.FilterRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemSubTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.ArrowFields;
import com.google.common.collect.*;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntPriorityQueue;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.WillCloseWhenClosed;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InMemoryTable extends AbstractTable implements NoexceptAutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryTable.class);

  private final MemTable.Snapshot snapshot;
  private final ImmutableList<SubTable> subTables;

  public InMemoryTable(@WillCloseWhenClosed MemTable.Snapshot snapshot) {
    this.snapshot = snapshot;
    this.subTables = IntStream.range(0, snapshot.getSubTableCount())
        .mapToObj(snapshot::getSubTable)
        .map(InMemorySubTable::new)
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public void close() {
    snapshot.close();
  }

  @Override
  public List<SubTable> getSubTables() {
    return subTables;
  }

  private static class InMemorySubTable implements SubTable {

    private final MemSubTable.Snapshot snapshot;
    private final Meta meta;

    InMemorySubTable(MemSubTable.Snapshot snapshot) {
      this.snapshot = snapshot;
      RangeSet<Long> rangeSet = TreeRangeSet.create();
      for (MemSubTable.SortedChunkSnapshot chunk : snapshot.getChunks()) {
        rangeSet.add(chunk.getKeyRange());
      }
      Range<Long> range = rangeSet.isEmpty() ? Range.closedOpen(0L, 0L) : rangeSet.span();
      ImmutableMap<Field, Statistic> fieldStats = snapshot.getFields().stream()
          .collect(ImmutableMap.toImmutableMap(
              field -> field,
              field -> new Statistic(range)
          ));
      this.meta = new Meta(fieldStats);
    }

    @Override
    public Meta getMeta() {
      return meta;
    }

    @Override
    public RowStream scan(List<Field> fields, Filter predicate) {
      RangeSet<Long> keyRangeSet = FilterRangeUtils.rangeSetOf(predicate);
      Filter remainingFilter = FilterRangeUtils.withoutKeyRangeSet(predicate);

      Map<Field, Integer> snapshotFieldIndexMap = new HashMap<>();
      List<Field> snapshotFields = snapshot.getFields();
      for (int i = 0; i < snapshotFields.size(); i++) {
        snapshotFieldIndexMap.put(snapshotFields.get(i), i);
      }

      int[] fieldIndexMapping = new int[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        Field requestedField = fields.get(i);
        Integer fieldIndex = snapshotFieldIndexMap.get(requestedField);

        // 检查字段是否存在
        if (fieldIndex == null) {
          throw new IllegalArgumentException("Requested field not found: " + requestedField);
        }

        fieldIndexMapping[i] = fieldIndex;
      }

      RowStream rowStream = new MergedRowStream(snapshot, keyRangeSet, fieldIndexMapping);
      if (!Filters.isTrue(remainingFilter)) {
        rowStream = new FilterRowStreamWrapper(rowStream, remainingFilter);
      }
      return rowStream;
    }
  }

  /**
   * 多路归并RowStream实现
   * 使用最小堆对多个已排序的chunk进行归并去重
   */
  private static class MergedRowStream implements RowStream {

    private final Header header;
    private final int[] fieldIndexMapping;
    private final ChunkIterator[] iterators;
    private final IntPriorityQueue heap;
    private Row nextRow;

    MergedRowStream(MemSubTable.Snapshot snapshot, RangeSet<Long> keyRangeSet, int[] fieldIndexMapping) {
      List<Field> snapshotFields = snapshot.getFields();
      this.header = new Header(cn.edu.tsinghua.iginx.engine.shared.data.read.Field.KEY,
          Arrays.stream(fieldIndexMapping).mapToObj(snapshotFields::get).map(ArrowFields::toIginxField).collect(Collectors.toList())
      );
      this.fieldIndexMapping = fieldIndexMapping;

      List<ChunkIterator> validIterators = new ArrayList<>();
      for (MemSubTable.SortedChunkSnapshot chunk : snapshot.getChunks()) {
        if (keyRangeSet.intersects(chunk.getKeyRange())) {
          validIterators.add(new ChunkIterator(chunk, keyRangeSet));
        }
      }

      this.iterators = validIterators.toArray(new ChunkIterator[0]);
      this.heap = new IntHeapPriorityQueue(
          iterators.length,
          (i1, i2) -> Long.compare(iterators[i1].currentKey, iterators[i2].currentKey)
      );
      for (int i = 0; i < iterators.length; i++) {
        if (iterators[i].advance()) {
          heap.enqueue(i);
        }
      }

      advance();
    }

    private void advance() {
      if (heap.isEmpty()) {
        nextRow = null;
        return;
      }

      int minIdx = heap.dequeueInt();
      long minKey = iterators[minIdx].currentKey;

      Object[] orderedValues = new Object[fieldIndexMapping.length];
      iterators[minIdx].fillValues(orderedValues, fieldIndexMapping);

      if (iterators[minIdx].advance()) {
        heap.enqueue(minIdx);
      }

      while (!heap.isEmpty() && iterators[heap.firstInt()].currentKey == minKey) {
        int idx = heap.dequeueInt();
        if (iterators[idx].advance()) {
          heap.enqueue(idx);
        }
      }

      nextRow = new Row(header, minKey, orderedValues);
    }

    @Override
    public Header getHeader() {
      return header;
    }

    @Override
    public boolean hasNext() {
      return nextRow != null;
    }

    @Override
    public Row next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Row row = nextRow;
      advance();
      return row;
    }

    @Override
    public void close() {
    }

    private static class ChunkIterator {
      private final BigIntVector keyVector;
      private final FieldVector[] fieldVectors;
      private final short[] sortedIndex;
      private final RangeSet<Long> keyRangeSet;
      private int position;
      long currentKey;

      ChunkIterator(MemSubTable.SortedChunkSnapshot chunk, RangeSet<Long> keyRangeSet) {
        this.keyVector = chunk.getSnapshot().getKeyVector();
        this.fieldVectors = chunk.getSnapshot().getFieldVectors().toArray(new FieldVector[0]);
        this.sortedIndex = chunk.getIndex();
        this.keyRangeSet = keyRangeSet;
        this.position = 0;
        this.currentKey = Long.MAX_VALUE;
      }

      boolean advance() {
        while (position < sortedIndex.length) {
          int idx = sortedIndex[position++];
          long key = keyVector.get(idx);
          if (keyRangeSet.contains(key)) {
            currentKey = key;
            return true;
          }
        }
        currentKey = Long.MAX_VALUE;
        return false;
      }

      void fillValues(Object[] orderedValues, int[] fieldIndexMapping) {
        int idx = sortedIndex[position - 1];
        for (int i = 0; i < fieldIndexMapping.length; i++) {
          orderedValues[i] = fieldVectors[fieldIndexMapping[i]].getObject(idx);
        }
      }
    }
  }

}
