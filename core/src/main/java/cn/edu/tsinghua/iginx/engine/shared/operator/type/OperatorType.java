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
package cn.edu.tsinghua.iginx.engine.shared.operator.type;

public enum OperatorType {

  // Exception[0,9]
  Unknown(0),

  // MultipleOperator[10,19]
  CombineNonQuery(10),
  Folded,

  // isGlobalOperator[20,29]
  ShowColumns(20),
  Migration,

  // BinaryOperator[30,49]
  Join(30),
  PathUnion,
  // SetOperator[35, 39)
  Union(35),
  Except,
  Intersect,
  // JoinOperator[40,49]
  InnerJoin(40),
  OuterJoin,
  CrossJoin,
  SingleJoin,
  MarkJoin,

  // isUnaryOperator >= 50
  Binary(50),
  Unary,
  Delete,
  Insert,
  Multiple,
  Project,
  Select,
  Sort,
  Limit,
  Downsample,
  RowTransform,
  SetTransform,
  MappingTransform,
  Rename,
  Reorder,
  AddSchemaPrefix,
  GroupBy,
  Distinct,
  AddSequence,
  RemoveNullColumn,
  ProjectWaitingForPath,
  ValueToSelectedPath;

  private int value;

  OperatorType() {
    this(OperatorTypeCounter.nextValue);
  }

  OperatorType(int value) {
    this.value = value;
    OperatorTypeCounter.nextValue = value + 1;
  }

  public int getValue() {
    return value;
  }

  private static class OperatorTypeCounter {

    private static int nextValue = 0;
  }

  public static boolean isBinaryOperator(OperatorType op) {
    return op.value >= 30 && op.value <= 49;
  }

  public static boolean isUnaryOperator(OperatorType op) {
    return op.value >= 50;
  }

  public static boolean isJoinOperator(OperatorType op) {
    return op.value >= 40 && op.value <= 49;
  }

  public static boolean isMultipleOperator(OperatorType op) {
    return op.value >= 10 && op.value <= 19;
  }

  public static boolean isGlobalOperator(OperatorType op) {
    return op == ShowColumns || op == Migration;
  }

  public static boolean isNeedBroadcasting(OperatorType op) {
    return op == Delete || op == Insert;
  }

  public static boolean isSetOperator(OperatorType op) {
    return op.value >= 35 && op.value < 39;
  }

  public static boolean isHasFunction(OperatorType op) {
    return op == GroupBy
        || op == Downsample
        || op == RowTransform
        || op == SetTransform
        || op == MappingTransform;
  }
}
