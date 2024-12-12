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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic;

public final class Or extends BinaryLogicScalarFunction {

  public static final String NAME = "or";

  public Or() {
    super(NAME);
  }

  @Override
  public byte evaluate(byte left, byte right) {
    return (byte) (left | right);
  }

  @Override
  public long evaluate(long left, long right) {
    return left | right;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Or;
  }
}