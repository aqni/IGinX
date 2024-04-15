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

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class Record {
  private final long key;
  private final List<Object> valueList;

  public Record(long key, List<Object> valueList) {
    this.key = key;
    this.valueList = valueList;
  }

  public long getKey() {
    return key;
  }

  public List<Object> getValueList() {
    return valueList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Record that = (Record) o;
    return key == that.key && Objects.equals(valueList, that.valueList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, valueList);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Record.class.getSimpleName() + "[", "]")
        .add("key=" + key)
        .add("value=" + valueList)
        .toString();
  }
}
