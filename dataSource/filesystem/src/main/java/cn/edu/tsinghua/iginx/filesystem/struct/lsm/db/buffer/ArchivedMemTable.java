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

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table.MemoryTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.NoexceptAutoCloseable;
import com.google.common.collect.RangeSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;

class ArchivedMemTable implements NoexceptAutoCloseable {
  private final MemTable memTable;
  private final Collection<NoexceptAutoCloseable> onClose;
  private final CountDownLatch latch = new CountDownLatch(1);

  public ArchivedMemTable(
      @WillCloseWhenClosed MemTable memTable,
      @WillCloseWhenClosed Collection<NoexceptAutoCloseable> onClose) {
    this.memTable = Preconditions.checkNotNull(memTable);
    this.onClose = new ArrayList<>(onClose);
  }

  public synchronized MemoryTable snapshot(BufferAllocator allocator) {
    memTable.compact();
    return memTable.snapshot(allocator);
  }

  public synchronized MemoryTable snapshot(
      List<Field> fields, RangeSet<Long> ranges, BufferAllocator allocator) {
    memTable.compact();
    return memTable.snapshot(fields, ranges, allocator);
  }

  public void waitUntilClosed() throws InterruptedException {
    latch.await();
  }

  @Override
  public void close() {
    latch.countDown();
    memTable.close();
    onClose.forEach(NoexceptAutoCloseable::close);
  }
}
