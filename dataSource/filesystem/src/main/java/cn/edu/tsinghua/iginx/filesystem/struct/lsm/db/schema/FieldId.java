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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FieldId {
  private final String[] data;

  public FieldId(String fullName, Map<String, String> tags) {
    int size = 1 + tags.size() + tags.size();
    this.data = new String[size];
    this.data[0] = fullName;

    List<String> tagKeys = tags.keySet().stream().sorted().collect(Collectors.toList());
    for (int i = 0; i < tagKeys.size(); i++) {
      String key = tagKeys.get(i);
      String value = tags.get(key);
      this.data[1 + i] = key;
      this.data[1 + tags.size() + i] = value;
    }
  }

  public String getName() {
    return data[0];
  }

  public Map<String, String> getTags() {
    int tagCount = (data.length - 1) / 2;
    if (tagCount == 0) {
      return Collections.emptyMap();
    }
    Object2ObjectOpenHashMap<String, String> result = new Object2ObjectOpenHashMap<>(tagCount);
    for (int i = 0; i < tagCount; i++) {
      String key = data[1 + i];
      String value = data[1 + tagCount + i];
      result.put(key, value);
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    FieldId fieldId = (FieldId) o;
    return Arrays.deepEquals(data, fieldId.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }

  @Override
  public String toString() {
    return getName() + getTags();
  }
}
