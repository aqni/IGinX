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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import it.unimi.dsi.fastutil.shorts.ShortArrays;
import it.unimi.dsi.fastutil.shorts.ShortComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Field;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@ThreadSafe
public class MemSubTable implements NoexceptAutoCloseable {

  private List<Field> fields;
  private final BufferAllocator allocator;
  private final int maxChunkValueCount;
  private List<SortedChunkSnapshot> snapshots;
  private MemBatch active;
  private SortedChunkSnapshot activeChunkSnapshot;

  public MemSubTable(List<Field> fields, BufferAllocator allocator, int maxChunkValueCount) {
    this(fields, allocator, maxChunkValueCount, new ArrayList<>(), null, null);
  }

  private MemSubTable(
      List<Field> fields,
      BufferAllocator allocator,
      int maxChunkValueCount,
      List<SortedChunkSnapshot> snapshots,
      MemBatch active,
      SortedChunkSnapshot activeChunkSnapshot
  ) {
    Preconditions.checkNotNull(fields);
    Preconditions.checkNotNull(allocator);
    Preconditions.checkArgument(maxChunkValueCount > 0);
    this.fields = fields;
    this.allocator = allocator;
    this.maxChunkValueCount = maxChunkValueCount;
    this.snapshots = snapshots;
    this.active = active;
    this.activeChunkSnapshot = activeChunkSnapshot;
  }

  public synchronized MemSubTable split(List<Field> splitFields, BufferAllocator allocator) {
    Set<Field> splitFieldSet = new HashSet<>(splitFields);
    List<Field> remainingFields = this.fields.stream()
        .filter(f -> !splitFieldSet.contains(f))
        .collect(ImmutableList.toImmutableList());

    List<SortedChunkSnapshot> splitSnapshots = new ArrayList<>();
    List<SortedChunkSnapshot> remainingSnapshots = new ArrayList<>();
    for (SortedChunkSnapshot snapshot : this.snapshots) {
      splitSnapshots.add(snapshot.slice(splitFields, allocator));
      remainingSnapshots.add(snapshot.slice(remainingFields, this.allocator));
      snapshot.close();
    }

    SortedChunkSnapshot splitActiveChunkSnapshot = null;
    SortedChunkSnapshot remainingActiveChunkSnapshot = null;
    if (this.activeChunkSnapshot != null) {
      splitActiveChunkSnapshot = this.activeChunkSnapshot.slice(splitFields, allocator);
      remainingActiveChunkSnapshot = this.activeChunkSnapshot.slice(remainingFields, this.allocator);
      this.activeChunkSnapshot.close();
    }

    MemBatch splitActive = null;
    if (active != null) {
      splitActive = active.split(splitFields, allocator);
    }

    this.fields = remainingFields;
    this.snapshots = remainingSnapshots;
    this.activeChunkSnapshot = remainingActiveChunkSnapshot;

    return new MemSubTable(splitFields, allocator, maxChunkValueCount,
        splitSnapshots, splitActive, splitActiveChunkSnapshot);
  }

  public synchronized Snapshot snapshot(List<Field> fields, BufferAllocator allocator) {
    if (active != null) {
      if (activeChunkSnapshot == null) {
        try (MemBatch.Snapshot activeSnapshot = active.snapshot(allocator)) {
          activeChunkSnapshot = new SortedChunkSnapshot(activeSnapshot, allocator);
        }
      }
    }
    List<SortedChunkSnapshot> chunkSnapshots = new ArrayList<>(snapshots);
    if (activeChunkSnapshot != null) {
      chunkSnapshots.add(activeChunkSnapshot);
    }
    return new Snapshot(fields, chunkSnapshots, allocator);
  }

  public synchronized void append(MemBatch.Snapshot batch) {
    int start = 0;
    int end = batch.getValueCount();
    while (start < end) {
      int activeValueCount = active == null ? 0 : active.getValueCount();
      int length = Math.min(end - start, maxChunkValueCount - activeValueCount);
      try (MemBatch.Snapshot slice = batch.slice(start, length, allocator)) {
        if (length == maxChunkValueCount) {
          snapshots.add(new SortedChunkSnapshot(slice, allocator));
        } else {
          if (active == null) {
            active = new MemBatch(fields, batch.getValueCount(), allocator);
          }
          active.append(slice);
          if (activeChunkSnapshot != null) {
            activeChunkSnapshot.close();
            activeChunkSnapshot = null;
          }
          if (active.getValueCount() >= maxChunkValueCount) {
            try (MemBatch.Snapshot activeSnapshot = active.snapshot(allocator)) {
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
    private final List<Field> fields;
    private final List<SortedChunkSnapshot> chunks;

    Snapshot(List<Field> fields, List<SortedChunkSnapshot> chunks, BufferAllocator allocator) {
      this.fields = fields;
      this.chunks = chunks.stream().map(s -> s.slice(fields, allocator)).collect(ImmutableList.toImmutableList());
    }

    public List<Field> getSchema() {
      return fields;
    }

    public List<SortedChunkSnapshot> getChunks() {
      return chunks;
    }

    @Override
    public void close() {
      chunks.forEach(SortedChunkSnapshot::close);
    }
  }

  public static class SortedChunkSnapshot implements NoexceptAutoCloseable {

    private final MemBatch.Snapshot snapshot;
    private final ShareMeta shareMeta;

    private SortedChunkSnapshot(MemBatch.Snapshot snapshot, List<Field> fields, BufferAllocator allocator, ShareMeta shareMeta) {
      Preconditions.checkArgument(snapshot.getValueCount() > 0);
      Preconditions.checkArgument(snapshot.getValueCount() <= Short.MAX_VALUE);
      this.snapshot = snapshot.slice(fields, allocator);
      this.shareMeta = shareMeta;
    }

    SortedChunkSnapshot(MemBatch.Snapshot snapshot, BufferAllocator allocator) {
      this(snapshot, snapshot.getFields(), allocator, new ShareMeta());
    }

    public SortedChunkSnapshot slice(List<Field> fields, BufferAllocator allocator) {
      return new SortedChunkSnapshot(snapshot, fields, allocator, shareMeta);
    }

    public MemBatch.Snapshot getSnapshot() {
      return snapshot;
    }

    public short[] getIndex() {
      return shareMeta.getKeyIndex(snapshot.getKeyVector());
    }

    public Range<Long> getKeyRange() {
      return shareMeta.getKeyRange(snapshot.getKeyVector());
    }

    @Override
    public void close() {
      snapshot.close();
    }

    private static class ShareMeta {
      private short[] index = null;
      private Range<Long> keyRange = null;

      public synchronized short[] getKeyIndex(BigIntVector keyVector) {
        if (index == null) {
          int valueCount = keyVector.getValueCount();
          index = new short[valueCount];
          for (short i = 0; i < valueCount; i++) {
            index[i] = i;
          }
          ShortArrays.stableSort(index, ShortComparator.comparingLong(keyVector::getValueAsLong));
        }
        return index;
      }

      public synchronized Range<Long> getKeyRange(BigIntVector keyVector) {
        if (keyRange == null) {
          short[] keyIndex = getKeyIndex(keyVector);
          long minKey = Long.MAX_VALUE;
          long maxKey = Long.MIN_VALUE;
          if (keyIndex.length > 0) {
            minKey = keyVector.getValueAsLong(keyIndex[0]);
            maxKey = keyVector.getValueAsLong(keyIndex[keyIndex.length - 1]);
          }
          keyRange = Range.closed(minKey, maxKey);
        }
        return keyRange;
      }
    }
  }
}
