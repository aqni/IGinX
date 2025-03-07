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
package cn.edu.tsinghua.iginx.migration;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class MigrationUtils {

  /** 获取列表的排列组合 */
  public static <T> List<List<T>> combination(List<T> values, int size) {

    if (0 == size) {
      return Collections.singletonList(Collections.emptyList());
    }

    if (values.isEmpty()) {
      return Collections.emptyList();
    }

    List<List<T>> combination = new LinkedList<>();

    T actual = values.iterator().next();

    List<T> subSet = new LinkedList<T>(values);
    subSet.remove(actual);

    List<List<T>> subSetCombination = combination(subSet, size - 1);

    for (List<T> set : subSetCombination) {
      List<T> newSet = new LinkedList<T>(set);
      newSet.add(0, actual);
      combination.add(newSet);
    }

    combination.addAll(combination(subSet, size));

    return combination;
  }

  public static double mean(Collection<Long> data) {
    return data.stream().mapToLong(Long::longValue).sum() * 1.0 / data.size();
  }

  /** 标准差计算 */
  public static double variance(Collection<Long> data) {
    double variance = 0;
    for (long dataItem : data) {
      variance = variance + (Math.pow((dataItem - mean(data)), 2));
    }
    variance = variance / (data.size() - 1);
    return Math.sqrt(variance);
  }
}
