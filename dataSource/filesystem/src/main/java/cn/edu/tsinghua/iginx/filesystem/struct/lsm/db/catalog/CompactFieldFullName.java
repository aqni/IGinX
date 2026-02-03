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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CompactFieldFullName {
  private final int tagCount;
  private final String[] data;

  public CompactFieldFullName(String fullName, Map<String, String> tags) {
    this.tagCount = tags.size();

    String[] nodes = fullName.split("\\.");
    this.data = Arrays.copyOf(nodes, nodes.length + 2 * tagCount);

    List<Map.Entry<String, String>> sortedTags =
        tags.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
    for (int i = 0; i < sortedTags.size(); i++) {
      Map.Entry<String, String> entry = sortedTags.get(i);
      String key = entry.getKey();
      String value = entry.getValue();
      this.data[nodes.length + i] = key;
      this.data[nodes.length + tagCount + i] = value;
    }
  }

  public String getName() {
    int nodeNum = data.length - 2 * tagCount;
    return String.join(".", Arrays.asList(data).subList(0, nodeNum));
  }

  public Map<String, String> getTags() {
    if (tagCount == 0) {
      return Collections.emptyMap();
    }
    int nodeNum = data.length - 2 * tagCount;
    Object2ObjectOpenHashMap<String, String> result = new Object2ObjectOpenHashMap<>(tagCount);
    for (int i = 0; i < tagCount; i++) {
      String key = data[nodeNum + i];
      String value = data[nodeNum + tagCount + i];
      result.put(key, value);
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    CompactFieldFullName compactFieldFullName = (CompactFieldFullName) o;
    return Arrays.deepEquals(data, compactFieldFullName.data);
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
