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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateful;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.VectorSchemaRoots;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class LimitUnaryExecutor extends StatefulUnaryExecutor {

  private final long offset;
  private final long limit;
  private long currentOffset;

  public LimitUnaryExecutor(ExecutorContext context, Schema inputSchema, long offset, long limit) {
    super(context, inputSchema, 1);
    Preconditions.checkArgument(offset >= 0, "Offset must be non-negative");
    Preconditions.checkArgument(limit >= 0, "Limit must be non-negative");
    this.offset = offset;
    this.limit = limit;
    this.currentOffset = 0;
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return getInputSchema();
  }

  @Override
  protected String getInfo() {
    return "Offset " + offset + " Limit " + limit;
  }

  @Override
  public boolean needConsume() throws ComputeException {
    return super.needConsume() && currentOffset < offset + limit;
  }

  @Override
  protected void consumeUnchecked(VectorSchemaRoot batch) throws ComputeException {
    int slicedStartIndex = (int) Math.min(batch.getRowCount(), Math.max(0, offset - currentOffset));
    int slicedEndIndex =
        (int) Math.min(batch.getRowCount(), Math.max(0, offset + limit - currentOffset));
    int slicedRowCount = slicedEndIndex - slicedStartIndex;
    if (slicedRowCount > 0) {
      offerResult(
          VectorSchemaRoots.slice(context.getAllocator(), batch, slicedStartIndex, slicedRowCount));
    }
    currentOffset += batch.getRowCount();
  }

  @Override
  protected void consumeEnd() throws ComputeException {}
}