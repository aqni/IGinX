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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.schema;

import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.checkerframework.checker.nullness.qual.Nullable;

public class HashFieldIndex implements FieldIndex {

  private final Object2ObjectMap<FieldId, Types.MinorType> fieldToTypeMap =
      new Object2ObjectOpenHashMap<>();

  @Override
  public void add(Field field) throws TypeConflictedException {
    if (!contain(field)) {
      FieldId key = new FieldId(field.getName(), field.getMetadata());
      Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
      fieldToTypeMap.put(key, minorType);
    }
  }

  @Override
  public boolean contain(Field field) throws TypeConflictedException {
    FieldId key = new FieldId(field.getName(), field.getMetadata());
    Types.MinorType existingType = fieldToTypeMap.get(key);
    if (existingType == null) {
      return false;
    }

    Types.MinorType newType = Types.getMinorTypeForArrowType(field.getType());
    if (existingType != newType) {
      throw new TypeConflictedException(
          key.toString(), newType.toString(), existingType.toString());
    }
    return true;
  }

  @Override
  public void delete(Field field) throws TypeConflictedException {
    if (contain(field)) {
      FieldId key = new FieldId(field.getName(), field.getMetadata());
      fieldToTypeMap.remove(key);
    }
  }

  @Override
  public List<Field> find(List<String> patterns, @Nullable TagFilter tagFilter) {
    List<Predicate<String>> matchers =
        patterns.stream().map(StringUtils::toColumnMatcher).collect(Collectors.toList());

    List<Field> result = new ArrayList<>();
    fieldToTypeMap.forEach(
        (fieldId, minorType) -> {
          String name = fieldId.getName();
          Map<String, String> tags = fieldId.getTags();
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
  public void clear() {
    fieldToTypeMap.clear();
  }
}
