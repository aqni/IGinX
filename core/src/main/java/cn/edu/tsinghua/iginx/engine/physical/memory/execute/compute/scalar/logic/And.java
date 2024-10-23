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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.logic;

public final class And extends BinaryLogicFunction {

  public static final String NAME = "and";

  public And() {
    super(NAME);
  }

  @Override
  public byte evaluate(byte left, byte right) {
    return (byte) (left & right);
  }

  @Override
  public long evaluate(long left, long right) {
    return left & right;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof And;
  }
}
