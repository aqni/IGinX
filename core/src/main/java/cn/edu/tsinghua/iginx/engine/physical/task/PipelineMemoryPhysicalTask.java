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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.UnaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.StatelessUnaryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.StopWatch;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import java.util.Objects;
import javax.annotation.WillClose;
import javax.annotation.WillCloseWhenClosed;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.arrow.vector.VectorSchemaRoot;

@Immutable
public class PipelineMemoryPhysicalTask extends UnaryMemoryPhysicalTask {

  private final UnaryExecutorFactory<? extends StatelessUnaryExecutor> executorFactory;

  public PipelineMemoryPhysicalTask(
      PhysicalTask parentTask,
      List<Operator> operators,
      RequestContext context,
      UnaryExecutorFactory<? extends StatelessUnaryExecutor> executorFactory) {
    super(parentTask, operators, context);
    this.executorFactory = Objects.requireNonNull(executorFactory);
  }

  private String info;

  @Override
  public String getInfo() {
    return info == null ? super.getInfo() : info;
  }

  @Override
  protected BatchStream compute(@WillClose BatchStream previous) throws PhysicalException {
    StatelessUnaryExecutor executor = null;
    BatchSchema schema = previous.getSchema();
    BatchSchema outputSchema;
    try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
      executor = executorFactory.initialize(executorContext, schema);
      outputSchema = BatchSchema.of(executor.getOutputSchema());
      info = executor.toString();
    } catch (PhysicalException e) {
      try (BatchStream previousHolder = previous;
          StatelessUnaryExecutor executorHolder = executor) {
        throw e;
      }
    }
    return new PipelineBatchStream(previous, outputSchema, executor);
  }

  private class PipelineBatchStream implements BatchStream {

    private final BatchStream source;
    private final BatchSchema outputSchema;
    private final StatelessUnaryExecutor executor;

    public PipelineBatchStream(
        @WillCloseWhenClosed BatchStream source,
        BatchSchema outputSchema,
        StatelessUnaryExecutor executor) {
      this.source = Objects.requireNonNull(source);
      this.outputSchema = Objects.requireNonNull(outputSchema);
      this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public BatchSchema getSchema() throws ComputeException {
      return outputSchema;
    }

    @Override
    public Batch getNext() throws PhysicalException {
      try (Batch sourceNext = source.getNext()) {
        try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
          VectorSchemaRoot compute = executor.compute(sourceNext.raw());
          getMetrics().accumulateAffectRows(sourceNext.getRowCount());
          return Batch.of(compute);
        }
      }
    }

    @Override
    public void close() throws PhysicalException {
      try (StopWatch watch = new StopWatch(getMetrics()::accumulateCpuTime)) {
        try (BatchStream source = this.source;
            StatelessUnaryExecutor executor = this.executor) {
          // Do nothing
        }
      }
    }
  }
}