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

import java.util.Objects;
import java.util.StringJoiner;

public class IntObjPair {
  private final int key;
  private final Object value;

  public IntObjPair(int key, Object value) {
    this.key = key;
    this.value = value;
  }

  public int getKey() {
    return key;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IntObjPair intObjPair = (IntObjPair) o;
    return key == intObjPair.key && Objects.equals(value, intObjPair.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", IntObjPair.class.getSimpleName() + "[", "]")
        .add("key=" + key)
        .add("value=" + value)
        .toString();
  }
}
