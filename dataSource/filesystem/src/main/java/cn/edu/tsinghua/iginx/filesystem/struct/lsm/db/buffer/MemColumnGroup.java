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

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.iterator.DedupIterator;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.iterator.StableMergeIterator;
import com.google.common.collect.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;

@ThreadSafe
public class MemColumnGroup implements AutoCloseable {

  private final int maxChunkValueCount;
  private final List<ChunkSnapshotHolder> compactedChunkSnapshots = new ArrayList<>();
  private final ChunkHolder active;

  public MemColumnGroup(BufferAllocator allocator, int maxChunkValueCount) {
    Preconditions.checkNotNull(allocator);
    Preconditions.checkArgument(maxChunkValueCount > 0);
    this.active = new ChunkHolder(allocator);
    this.maxChunkValueCount = maxChunkValueCount;
  }

  // TODO: make use of ValueFilter
  public synchronized Snapshot snapshot(RangeSet<Long> ranges, BufferAllocator allocator) {
    active.refresh(compactedChunkSnapshots);
    return createSnapshot(ranges, compactedChunkSnapshots, allocator);
  }

  public synchronized Snapshot snapshot(BufferAllocator allocator) {
    return snapshot(ImmutableRangeSet.of(Range.all()), allocator);
  }

  private static Snapshot createSnapshot(
      RangeSet<Long> ranges,
      List<ChunkSnapshotHolder> snapshotHolders,
      @Nullable BufferAllocator allocator) {
    List<ChunkSnapshotHolder> snapshots = new ArrayList<>();
    for (ChunkSnapshotHolder snapshot : snapshotHolders) {
      ChunkSnapshotHolder filtered;
      if (allocator != null) {
        filtered = snapshot.slice(allocator);
      } else {
        filtered = snapshot.slice();
      }
      filtered.delete(ranges.complement());
      if (filtered.isEmpty()) {
        filtered.close();
      } else {
        snapshots.add(filtered);
      }
    }
    return new Snapshot(snapshots);
  }

  public synchronized void store(Chunk.Snapshot snapshot) {
    int start = 0;
    int end = snapshot.getValueCount();
    while (start < end) {
      int length = Math.min(end - start, maxChunkValueCount - active.getValueCount());
      if (length == maxChunkValueCount) {
        compactedChunkSnapshots.add(active.sorted(snapshot, start, end));
      } else {
        active.store(snapshot, start, length);
      }
      start += length;
    }
  }

  public synchronized void delete(RangeSet<Long> ranges) {
    deleted.addAll(ranges);
  }

  @Override
  public synchronized void close() {
    compactedChunkSnapshots.forEach(ChunkSnapshotHolder::close);
    compactedChunkSnapshots.clear();
    active.close();
  }

  public static class Snapshot implements AutoCloseable, Iterable<Map.Entry<Long, Object>> {

    private final List<ChunkSnapshotHolder> snapshots;

    Snapshot(@WillCloseWhenClosed List<ChunkSnapshotHolder> snapshots) {
      this.snapshots = Preconditions.checkNotNull(snapshots);
    }

    public Snapshot slice(RangeSet<Long> ranges, BufferAllocator allocator) {
      return createSnapshot(ranges, snapshots, allocator);
    }

    public Snapshot slice(RangeSet<Long> ranges) {
      return createSnapshot(ranges, snapshots, null);
    }

    @Override
    public void close() {
      snapshots.forEach(ChunkSnapshotHolder::close);
      snapshots.clear();
    }

    @Override
    @Nonnull
    public Iterator<Map.Entry<Long, Object>> iterator() {
      Iterator<Map.Entry<Long, Object>> mergedIterator =
          new StableMergeIterator<>(getIterators(), Map.Entry.comparingByKey());
      return new DedupIterator<>(mergedIterator, Map.Entry::getKey);
    }

    private List<Iterator<Map.Entry<Long, Object>>> getIterators() {
      // TODO: 对 Iterator 进行 concat 减少 StableMergeIterator 内 queue 中的项数
      //       目前使用贪心算法，可能不是最优解
      List<RangeMap<Long, Integer>> rangeGroups = new ArrayList<>();
      RangeMap<Long, Integer> currentRanges = TreeRangeMap.create();
      for (int i = 0; i < snapshots.size(); i++) {
        ChunkSnapshotHolder snapshot = snapshots.get(i);
        Range<Long> keyRange = snapshot.getKeyRange();
        if (currentRanges.subRangeMap(keyRange).asMapOfRanges().isEmpty()) {
          currentRanges.put(keyRange, i);
        } else {
          rangeGroups.add(currentRanges);
          currentRanges = TreeRangeMap.create();
          currentRanges.put(keyRange, i);
        }
      }
      rangeGroups.add(currentRanges);
      List<Iterator<Map.Entry<Long, Object>>> iterators = new ArrayList<>();
      for (RangeMap<Long, Integer> rangeMap : rangeGroups) {
        List<Iterator<Map.Entry<Long, Object>>> groupIterators = new ArrayList<>();
        for (int i : rangeMap.asMapOfRanges().values()) {
          groupIterators.add(snapshots.get(i).iterator());
        }
        iterators.add(Iterators.concat(groupIterators.iterator()));
      }
      return iterators;
    }

    public RangeSet<Long> getRanges() {
      RangeSet<Long> range = TreeRangeSet.create();
      snapshots.forEach(snapshot -> range.add(snapshot.getKeyRange()));
      return range;
    }
  }

  private static class ChunkSnapshotHolder
      implements AutoCloseable, Iterable<Map.Entry<Long, Object>> {

    private final Chunk.Snapshot snapshot;
    private final RangeSet<Long> deleted;

    public ChunkSnapshotHolder(@WillCloseWhenClosed Chunk.Snapshot snapshot) {
      this(snapshot, TreeRangeSet.create());
    }

    private ChunkSnapshotHolder(@WillCloseWhenClosed Chunk.Snapshot snapshot, RangeSet<Long> deleted) {
      this.snapshot = snapshot;
      this.deleted = TreeRangeSet.create(deleted);
    }

    public void delete(RangeSet<Long> ranges) {
      if (deleted == null) {
        Range<Long> fullRange = getKeyRange(snapshot);
        if (!ranges.intersects(fullRange)) {
          return;
        }
        deleted = TreeRangeSet.create();
        deleted.add(fullRange);
      }
      deleted.removeAll(ranges);
    }

    public Range<Long> getKeyRange() {
      if (deleted == null) {
        return getKeyRange(snapshot);
      }
      if (deleted.isEmpty()) {
        return Range.closedOpen(0L, 0L);
      }
      return deleted.span();
    }

    @Override
    public void close() {
      snapshot.close();
    }

    public ChunkSnapshotHolder slice(BufferAllocator allocator) {
      return new ChunkSnapshotHolder(snapshot.slice(allocator), deleted);
    }

    public ChunkSnapshotHolder slice() {
      return new ChunkSnapshotHolder(snapshot.slice(), deleted);
    }

    @Override
    @Nonnull
    public Iterator<Map.Entry<Long, Object>> iterator() {
      Iterator<Map.Entry<Long, Object>> iterator = snapshot.iterator();
      if (deleted == null) {
        return iterator;
      }
      return Iterators.filter(iterator, entry -> deleted.contains(entry.getKey()));
    }
  }

  private static class ChunkHolder implements AutoCloseable {
    private final BufferAllocator allocator;
    private Chunk activeChunk = null;
    private boolean isDirty = false;
    private boolean hasOld = false;

    ChunkHolder(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    public int getValueCount() {
      return activeChunk == null ? 0 : activeChunk.getValueCount();
    }

    public ChunkSnapshotHolder sorted(Chunk.Snapshot snapshot, int offset, int length) {
      Chunk.Snapshot sorted = factory.sorted(snapshot.slice(offset, length, allocator), allocator);
      return new ChunkSnapshotHolder(sorted);
    }

    public void store(Chunk.Snapshot data) {
      if (data.getValueCount() == 0) {
        return;
      }
      if (activeChunk == null) {
        activeChunk = factory.wrap(data, allocator);
      }
      activeChunk.store(data);
      isDirty = true;
    }

    public void store(Chunk.Snapshot snapshot, int offset, int length) {
      try (Chunk.Snapshot slice = snapshot.slice(offset, length, allocator)) {
        store(slice);
      }
    }

    public void delete(RangeSet<Long> ranges) {
      if (activeChunk != null) {
        activeChunk.delete(ranges);
      }
    }

    public void refresh(List<ChunkSnapshotHolder> compactedChunkSnapshots) {
      if (!isDirty) {
        return;
      }
      isDirty = false;

      if (hasOld) {
        int offset = compactedChunkSnapshots.size() - 1;
        compactedChunkSnapshots.remove(offset).close();
      }

      Chunk.Snapshot snapshot = activeChunk.snapshot(allocator);
      if (snapshot.getValueCount() > 0) {
        compactedChunkSnapshots.add(new ChunkSnapshotHolder(snapshot));
        hasOld = true;
      } else {
        snapshot.close();
        reset();
      }
    }

    public void reset() {
      if (activeChunk != null) {
        activeChunk.close();
        activeChunk = null;
      }
      isDirty = false;
      hasOld = false;
    }

    public void close() {
      reset();
    }
  }
}

//@Immutable
//protected static class IndexedSnapshot extends AppendableChunk.Snapshot {
//
//  protected final IntVector indexes;
//
//  public IndexedSnapshot(@WillCloseWhenClosed AppendableChunk.Snapshot snapshot, @WillCloseWhenClosed IntVector indexes) {
//    super(snapshot.keyVector, snapshot.valueVectors);
//    this.indexes = indexes;
//    Preconditions.checkArgument(snapshot.keyVector.getMinorType() == Types.MinorType.BIGINT);
//    Preconditions.checkArgument(!indexes.getField().isNullable());
//  }
//
//  @Override
//  public void close() {
//    super.close();
//    indexes.close();
//  }
//
//  protected IntVector indexOf(AppendableChunk.Snapshot snapshot, BufferAllocator allocator) {
//    if (deletedRangeSet.get(0).isEmpty()) {
//      if (ArrowVectors.isStrictlyOrdered(snapshot.keyVector)) {
//        return null;
//      }
//    }
//
//    IntVector indexes = ArrowVectors.stableSortIndexes(snapshot.keyVector, allocator);
//    ArrowVectors.dedupSortedIndexes(snapshot.keyVector, indexes);
//    if (!deletedRangeSet.get(0).isEmpty()) {
//      ArrowVectors.filter(indexes, i -> !isDeleted(snapshot, i));
//    }
//    return indexes;
//  }
