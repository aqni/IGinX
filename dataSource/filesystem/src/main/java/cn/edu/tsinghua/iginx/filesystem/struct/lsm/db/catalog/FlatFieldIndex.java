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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.catalog;

import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FlatFieldIndex implements FieldIndex {

  private final Object2ObjectMap<CompactFieldFullName, Types.MinorType> fieldToTypeMap =
      new Object2ObjectOpenHashMap<>();

  @Override
  public List<Boolean> contain(List<Field> fields) throws TypeConflictedException {
    List<Boolean> results = new ArrayList<>(fields.size());
    for (Field field : fields) {
      CompactFieldFullName key = new CompactFieldFullName(field.getName(), field.getMetadata());
      Types.MinorType existingType = fieldToTypeMap.get(key);
      if (existingType == null) {
        results.add(false);
      } else {
        Types.MinorType expectedType = Types.getMinorTypeForArrowType(field.getType());
        if (existingType != expectedType) {
          throw new TypeConflictedException(
              key.toString(), expectedType.toString(), existingType.toString());
        }
        results.add(true);
      }
    }
    return results;
  }

  @Override
  public List<Field> find(List<String> patterns, @Nullable TagFilter tagFilter) {
    List<Predicate<String>> matchers =
        patterns.stream().map(StringUtils::toColumnMatcher).collect(Collectors.toList());

    List<Field> result = new ArrayList<>();
    fieldToTypeMap.forEach(
        (compactFieldFullName, minorType) -> {
          String name = compactFieldFullName.getName();
          Map<String, String> tags = compactFieldFullName.getTags();
          boolean patternMatched = matchers.stream().anyMatch(matcher -> matcher.test(name));
          if (!patternMatched) {
            return;
          }
          if (tagFilter != null && !TagKVUtils.match(tags, tagFilter)) {
            return;
          }
          Field field = ArrowFields.of(name, tags, minorType);
          result.add(field);
        });

    return result;
  }

  @Override
  public void insert(List<Field> fields) throws TypeConflictedException {
    for (Field field : fields) {
      CompactFieldFullName key = new CompactFieldFullName(field.getName(), field.getMetadata());
      Types.MinorType newType = Types.getMinorTypeForArrowType(field.getType());
      Types.MinorType existingType = fieldToTypeMap.get(key);
      if (existingType != null) {
        if (existingType != newType) {
          throw new TypeConflictedException(
              key.toString(),
              Types.getMinorTypeForArrowType(field.getType()).toString(),
              existingType.toString());
        }
        continue;
      }
      fieldToTypeMap.put(key, newType);
    }
  }

  @Override
  public void delete(List<Field> fields) throws TypeConflictedException {
    for (Field field : fields) {
      CompactFieldFullName key = new CompactFieldFullName(field.getName(), field.getMetadata());
      Types.MinorType existingType = fieldToTypeMap.get(key);
      if (existingType == null) {
        continue;
      }
      Types.MinorType expectedType = Types.getMinorTypeForArrowType(field.getType());
      if (existingType != expectedType) {
        throw new TypeConflictedException(
            key.toString(),
            Types.getMinorTypeForArrowType(field.getType()).toString(),
            existingType.toString());
      }
      fieldToTypeMap.remove(key);
    }
  }

  @Override
  public void clear() {
    fieldToTypeMap.clear();
  }
}
