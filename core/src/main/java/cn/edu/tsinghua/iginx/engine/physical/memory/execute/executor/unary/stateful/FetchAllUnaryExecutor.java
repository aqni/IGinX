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
import java.util.ArrayDeque;
import java.util.Queue;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class FetchAllUnaryExecutor extends StatefulUnaryExecutor {

  private final Queue<VectorSchemaRoot> readyBatches = new ArrayDeque<>();

  public FetchAllUnaryExecutor(ExecutorContext context, Schema inputSchema) {
    super(context, inputSchema, Integer.MAX_VALUE);
  }

  @Override
  public Schema getOutputSchema() throws ComputeException {
    return getInputSchema();
  }

  @Override
  protected String getInfo() {
    return "FetchAll";
  }

  @Override
  protected void consumeUnchecked(VectorSchemaRoot batch) throws ComputeException {
    offerResult(VectorSchemaRoots.slice(context.getAllocator(), batch));
  }

  @Override
  protected void consumeEnd() throws ComputeException {}
}