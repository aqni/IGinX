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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Schemas;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class ResultRowCountException extends ResultException {

  private final int valueCount;
  private final int expectedValueCount;

  public ResultRowCountException(
      PhysicalFunction function, Field outputField, int expectedValueCount, int valueCount) {
    this(function, Schemas.of(outputField), valueCount, expectedValueCount);
  }

  public ResultRowCountException(
      PhysicalFunction function, Schema outputSchema, int expectedValueCount, int valueCount) {
    super(
        function,
        outputSchema,
        "Expected " + expectedValueCount + " values, but got " + valueCount);
    this.valueCount = valueCount;
    this.expectedValueCount = expectedValueCount;
  }

  public int getValueCount() {
    return valueCount;
  }

  public int getExpectedValueCount() {
    return expectedValueCount;
  }
}
