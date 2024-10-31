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
package cn.edu.tsinghua.iginx.engine.physical.task;

import java.time.Duration;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TaskMetrics {

  private final LongAdder affectRows = new LongAdder();
  private final LongAdder span = new LongAdder();

  public void accumulateAffectRows(long number) {
    affectRows.add(number);
  }

  public void accumulateCpuTime(long nanos) {
    this.span.add(nanos);
  }

  public long affectRows() {
    return affectRows.sum();
  }

  public Duration cpuTime() {
    return Duration.ofNanos(span.sum());
  }
}