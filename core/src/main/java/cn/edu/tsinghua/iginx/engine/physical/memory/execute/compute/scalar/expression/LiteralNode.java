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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class LiteralNode extends AbstractPhysicalExpression {

  private final Object value;

  public LiteralNode(@Nullable Object value) {
    this(value, null);
  }

  public LiteralNode(@Nullable Object value, String alias) {
    super(alias, Collections.emptyList());
    this.value = value;
  }

  @Override
  public String getName() {
    return String.valueOf(value);
  }

  @Override
  protected FieldVector invokeImpl(BufferAllocator allocator, VectorSchemaRoot input)
      throws ComputeException {
    return ConstantVectors.of(allocator, value, input.getRowCount());
  }
}