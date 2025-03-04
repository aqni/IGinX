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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinOption;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.StatefulBinaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.utils.PhysicalJoinPlannerUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import java.util.*;

public class OuterJoinInfoGenerator implements BinaryExecutorFactory<StatefulBinaryExecutor> {

  private final OuterJoin operator;

  public OuterJoinInfoGenerator(OuterJoin operator) {
    this.operator = Objects.requireNonNull(operator);
  }

  @Override
  public StatefulBinaryExecutor initialize(
      ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema)
      throws ComputeException {
    JoinOption joinOption = toJoinOption(operator.getOuterJoinType());
    return PhysicalJoinPlannerUtils.constructJoin(
        context,
        leftSchema,
        rightSchema,
        operator,
        joinOption,
        operator.getFilter(),
        operator.getJoinColumns(),
        null,
        false);
  }

  private static JoinOption toJoinOption(OuterJoinType outerJoinType) {
    switch (outerJoinType) {
      case LEFT:
        return JoinOption.LEFT;
      case RIGHT:
        return JoinOption.RIGHT;
      case FULL:
        return JoinOption.FULL;
      default:
        throw new IllegalArgumentException("OuterJoinType is not supported: " + outerJoinType);
    }
  }
}
