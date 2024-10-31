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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ArityException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public abstract class AbstractAccumulation implements Accumulation {

  private final String name;
  private final Arity arity;

  protected AbstractAccumulation(String name, Arity arity) {
    this.name = Objects.requireNonNull(name);
    this.arity = Objects.requireNonNull(arity);
  }

  @Override
  public String getName() {
    return name;
  }

  public Accumulator accumulate(@WillNotClose BufferAllocator allocator, Schema inputSchema)
      throws ComputeException {
    if (!arity.checkArity(inputSchema.getFields().size())) {
      throw new ArityException(this, inputSchema, arity);
    }
    return accumulateImpl(allocator, inputSchema);
  }

  protected abstract AbstractAccumulator<? extends AbstractState> accumulateImpl(
      @WillNotClose BufferAllocator allocator, Schema inputSchema) throws ComputeException;

  protected abstract class AbstractAccumulator<S extends AbstractState> implements Accumulator {

    protected final BufferAllocator allocator;
    protected final Schema schema;
    protected final Field outputField;
    private final Class<? extends S> stateClass;
    private final Supplier<S> factory;

    protected AbstractAccumulator(
        BufferAllocator allocator,
        Schema inputSchema,
        Field outputField,
        Class<? extends S> stateClass,
        Supplier<S> factory) {
      this.allocator = allocator;
      this.schema = inputSchema;
      this.outputField = outputField;
      this.stateClass = stateClass;
      this.factory = factory;
    }

    @Override
    public String toString() {
      return getName();
    }

    @Override
    public String getName() {
      return AbstractAccumulation.this.getName()
          + schema.getFields().stream()
              .map(Field::toString)
              .collect(Collectors.joining(", ", "<", ">"));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void update(@WillNotClose State state, @WillNotClose VectorSchemaRoot input)
        throws ComputeException {
      if (!Objects.equals(input.getSchema(), schema)) {
        throw new ComputeException(
            "Schema mismatch: expected " + schema + ", but got " + input.getSchema());
      }
      if (!stateClass.isInstance(state)) {
        throw new ComputeException(
            "State mismatch: expected " + stateClass + ", but got " + state.getClass());
      }
      S specialState = (S) state;
      specialState.update(input);
    }

    @Override
    @SuppressWarnings("unchecked")
    public FieldVector evaluate(List<State> states) throws ComputeException {
      try (FieldVector result = outputField.createVector(allocator)) {
        result.setInitialCapacity(states.size());
        FieldWriter writer = result.getMinorType().getNewFieldWriter(result);
        for (int row = 0; row < states.size(); row++) {
          State state = states.get(row);
          if (!stateClass.isInstance(state)) {
            throw new ComputeException(
                "State mismatch: expected " + stateClass + ", but got " + state.getClass());
          }
          S specialState = (S) state;
          writer.setPosition(row);
          specialState.evaluate(writer);
        }
        result.setValueCount(states.size());
        return ValueVectors.transfer(allocator, result);
      }
    }

    @Override
    public S createState() throws ComputeException {
      return factory.get();
    }
  }

  protected abstract static class AbstractState implements Accumulator.State {
    protected abstract void update(VectorSchemaRoot root) throws ComputeException;

    protected abstract void evaluate(FieldWriter writer) throws ComputeException;

    public void close() {}
  }
}