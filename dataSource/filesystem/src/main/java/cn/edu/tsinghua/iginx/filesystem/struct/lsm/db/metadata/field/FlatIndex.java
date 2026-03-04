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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.field;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class FlatIndex implements FieldIndex {

  private final Object2ObjectMap<ColumnKey, DataType> fieldToTypeMap =
      new Object2ObjectOpenHashMap<>();

  @Override
  public boolean contain(Field field) throws TypeConflictedException {
    ColumnKey key = new ColumnKey(field.getName(), field.getTags());
    DataType existingType = fieldToTypeMap.get(key);
    if (existingType == null) {
      return false;
    } else {
      DataType expectedType = field.getType();
      if (existingType != expectedType) {
        throw new TypeConflictedException(
            key.toString(), expectedType.toString(), existingType.toString());
      }
      return true;
    }
  }


  @Override
  public Set<Field> find(List<String> patterns, @Nullable TagFilter tagFilter) {
    List<Predicate<String>> matchers =
        patterns.stream()
            .map(StringUtils::toColumnMatcher)
            .collect(ImmutableList.toImmutableList());

    List<Field> result = new ArrayList<>();
    fieldToTypeMap.forEach(
        (compactFieldFullName, type) -> {
          String name = compactFieldFullName.getPath();
          Map<String, String> tags = compactFieldFullName.getTags();
          boolean patternMatched = matchers.stream().anyMatch(matcher -> matcher.test(name));
          if (!patternMatched) {
            return;
          }
          if (tagFilter != null && !TagKVUtils.match(tags, tagFilter)) {
            return;
          }
          Field field = new Field(name, type, tags);
          result.add(field);
        });

    return ImmutableSet.copyOf(result);
  }

  @Override
  public void insert(Set<Field> fields) throws TypeConflictedException {
    for (Field field : fields) {
      ColumnKey key = new ColumnKey(field.getName(), field.getTags());
      DataType newType = field.getType();
      DataType existingType = fieldToTypeMap.get(key);
      if (existingType != null) {
        if (existingType != newType) {
          throw new TypeConflictedException(
              key.toString(),
              newType.toString(),
              existingType.toString());
        }
      }
      fieldToTypeMap.put(key, newType);
    }
  }

  @Override
  public void remove(Set<Field> fields) throws TypeConflictedException {
    for (Field field : fields) {
      ColumnKey key = new ColumnKey(field.getName(), field.getTags());
      DataType existingType = fieldToTypeMap.get(key);
      if (existingType != null) {
        DataType expectedType = field.getType();
        if (existingType != expectedType) {
          throw new TypeConflictedException(
              key.toString(),
              expectedType.toString(),
              existingType.toString());
        }
      }
      fieldToTypeMap.remove(key);
    }
  }

  @Override
  public void clear() {
    fieldToTypeMap.clear();
  }
}
