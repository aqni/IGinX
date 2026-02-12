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

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.NoexceptAutoCloseable;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import it.unimi.dsi.fastutil.longs.LongObjectImmutablePair;
import it.unimi.dsi.fastutil.shorts.ShortArrays;
import it.unimi.dsi.fastutil.shorts.ShortComparator;
import it.unimi.dsi.fastutil.shorts.ShortImmutableList;
import it.unimi.dsi.fastutil.shorts.ShortList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@ThreadSafe
public class MemColumnGroup implements NoexceptAutoCloseable {

  private final int maxChunkValueCount;
  private final BufferAllocator allocator;
  private final List<SortedChunkSnapshot> snapshots = new ArrayList<>();
  private Chunk active = null;
  private SortedChunkSnapshot activeChunkSnapshot = null;

  public MemColumnGroup(BufferAllocator allocator, int maxChunkValueCount) {
    Preconditions.checkNotNull(allocator);
    Preconditions.checkArgument(maxChunkValueCount > 0);
    this.allocator = allocator;
    this.maxChunkValueCount = maxChunkValueCount;
  }

  public synchronized Snapshot snapshot(BufferAllocator allocator) {
    List<SortedChunkSnapshot> chunkSnapshots = new ArrayList<>(snapshots);
    if (active != null) {
      if (activeChunkSnapshot == null) {
        try (Chunk.Snapshot activeSnapshot = active.snapshot(allocator)) {
          activeChunkSnapshot = new SortedChunkSnapshot(activeSnapshot, allocator);
        }
      }
      chunkSnapshots.add(activeChunkSnapshot);
    }
    return new Snapshot(chunkSnapshots, allocator);
  }

  public synchronized void append(Chunk.Snapshot snapshot) {
    int start = 0;
    int end = snapshot.getValueCount();
    while (start < end) {
      int activeValueCount = active == null ? 0 : active.getValueCount();
      int length = Math.min(end - start, maxChunkValueCount - activeValueCount);
      try (Chunk.Snapshot slice = snapshot.slice(start, length, allocator)) {
        if (length == maxChunkValueCount) {
          snapshots.add(new SortedChunkSnapshot(slice, allocator));
        } else {
          if (active == null) {
            active = new Chunk(slice, allocator);
          }
          active.append(slice);
          if (activeChunkSnapshot != null) {
            activeChunkSnapshot.close();
            activeChunkSnapshot = null;
          }
          if (active.getValueCount() >= maxChunkValueCount) {
            try (Chunk.Snapshot activeSnapshot = active.snapshot(allocator)) {
              snapshots.add(new SortedChunkSnapshot(activeSnapshot, allocator));
            }
            active.close();
            active = null;
          }
        }
      }
      start += length;
    }
  }

  @Override
  public synchronized void close() {
    snapshots.forEach(SortedChunkSnapshot::close);
    snapshots.clear();
    if (active != null) {
      active.close();
    }
    if (activeChunkSnapshot != null) {
      activeChunkSnapshot.close();
    }
  }

  public static class Snapshot implements NoexceptAutoCloseable {

    private final List<SortedChunkSnapshot> chunks;

    Snapshot(List<SortedChunkSnapshot> compactedChunkSnapshots, BufferAllocator allocator) {
      this.chunks = compactedChunkSnapshots.stream().map(s -> s.slice(0, s.getValueCount(), allocator)).collect(Collectors.toList());
    }

    public Snapshot slice(BufferAllocator allocator) {
      return new Snapshot(chunks, allocator);
    }

    public Iterator<LongObjectImmutablePair<Object[]>> scan(int[] columnIndexes, RangeSet<Long> ranges) {
      List<Iterator<LongObjectImmutablePair<BiConsumer<int[], Object[]>>>> iterators = chunks.stream()
          .map(c -> c.iterator(ranges))
          .collect(Collectors.toList());



    }

    @Override
    public void close() {
      chunks.forEach(SortedChunkSnapshot::close);
      chunks.clear();
    }
  }

  private static class SortedChunkSnapshot implements NoexceptAutoCloseable {

    private final Chunk.Snapshot snapshot;
    private final ShortList index;

    private SortedChunkSnapshot(Chunk.Snapshot snapshot, ShortList index, BufferAllocator allocator) {
      Preconditions.checkArgument(snapshot.getValueCount() > 0);
      Preconditions.checkArgument(snapshot.getValueCount() <= Short.MAX_VALUE);
      this.snapshot = snapshot.slice(0, snapshot.getValueCount(), allocator);
      this.index = index;
    }

    SortedChunkSnapshot(Chunk.Snapshot snapshot, BufferAllocator allocator) {
      this(snapshot, indexOf(snapshot), allocator);
    }

    private static ShortList indexOf(Chunk.Snapshot snapshot) {
      int valueCount = snapshot.getValueCount();
      short[] index = new short[valueCount];
      for (short i = 0; i < valueCount; i++) {
        index[i] = i;
      }
      ShortArrays.stableSort(index, ShortComparator.comparingLong(snapshot::getKey));
      return new ShortImmutableList(index);
    }

    public int getValueCount() {
      return index.size();
    }

    public SortedChunkSnapshot slice(int startIndex, int length, BufferAllocator allocator) {
      return new SortedChunkSnapshot(snapshot, index.subList(startIndex, startIndex + length), allocator);
    }

    Iterator<LongObjectImmutablePair<BiConsumer<int[], Object[]>>> iterator(RangeSet<Long> ranges) {
      Range<Long> chunkRange = Range.closed(getKey(0), getKey(getValueCount() - 1));
      if (!ranges.intersects(chunkRange)) {
        return Collections.emptyIterator();
      }
      return new Iterator<LongObjectImmutablePair<BiConsumer<int[], Object[]>>>() {

        private int nextIndex = 0;
        private LongObjectImmutablePair<BiConsumer<int[], Object[]>> nextValue;

        {
          fetchNext();
        }

        @Override
        public boolean hasNext() {
          return nextValue != null;
        }

        @Override
        public LongObjectImmutablePair<BiConsumer<int[], Object[]>> next() {
          LongObjectImmutablePair<BiConsumer<int[], Object[]>> result = nextValue;
          fetchNext();
          return result;
        }

        private void fetchNext() {
          while (nextIndex < getValueCount()) {
            int originIndex = getOriginIndex(nextIndex);
            nextIndex++;
            long key = snapshot.getKey(originIndex);
            if (ranges.contains(key)) {
              BiConsumer<int[], Object[]> value = (columnIndexes, row) -> snapshot.fillRow(originIndex, columnIndexes, row);
              nextValue = new LongObjectImmutablePair<>(key, value);
              return;
            }
          }
          nextValue = null;
        }
      };
    }

    private long getKey(int index) {
      return snapshot.getKey(getOriginIndex(index));
    }

    private int getOriginIndex(int index) {
      return this.index.getShort(index);
    }

    @Override
    public void close() {
      snapshot.close();
    }
  }
}
