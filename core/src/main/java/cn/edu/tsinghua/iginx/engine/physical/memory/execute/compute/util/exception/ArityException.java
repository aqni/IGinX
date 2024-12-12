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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArityException extends ArgumentException {

  private final Arity arity;

  public ArityException(PhysicalFunction function, Schema schema, Arity arity) {
    super(function, schema, "arity " + arity + " is not satisfied");
    this.arity = arity;
  }

  public Arity getArity() {
    return arity;
  }
}