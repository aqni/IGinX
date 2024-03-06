/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.util.record;

import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.StringJoiner;

public class ArrayHeader implements RecordIterator.Header {

  private final String[] names;

  private final DataType[] types;

  public ArrayHeader(Collection<Map.Entry<String, DataType>> fields) {
    this.names = new String[fields.size()];
    this.types = new DataType[fields.size()];
    int i = 0;
    for (Map.Entry<String, DataType> entry : fields) {
      names[i] = entry.getKey();
      types[i] = entry.getValue();
      i++;
    }
  }

  @Override
  public int size() {
    return names.length;
  }

  @Override
  public String getName(int i) {
    return names[i];
  }

  @Override
  public DataType getType(int i) {
    return types[i];
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArrayHeader header = (ArrayHeader) o;
    return Arrays.equals(names, header.names) && Arrays.equals(types, header.types);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(names);
    result = 31 * result + Arrays.hashCode(types);
    return result;
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(", ", ArrayHeader.class.getSimpleName() + "[", "]");
    for (int i = 0; i < names.length; i++) {
      joiner.add(names[i] + ":" + types[i]);
    }
    return joiner.toString();
  }
}
