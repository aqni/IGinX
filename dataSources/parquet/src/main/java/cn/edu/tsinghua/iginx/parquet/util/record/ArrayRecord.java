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

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ArrayRecord implements RecordIterator.Record {

  private final long key;

  private final Object[] values;

  public ArrayRecord(long key, Object[] values) {
    this.key = key;
    this.values = values;
  }

  @Override
  public long getKey() {
    return key;
  }

  @Override
  public Object getValue(int i) {
    if (i < values.length) {
      return values[i];
    }
    return null;
  }

  @Nonnull
  @Override
  public Iterator<IntObjPair> iterator() {
    return new Iterator<IntObjPair>() {
      private int i = 0;

      @Override
      public boolean hasNext() {
        while (i < values.length && values[i] == null) {
          i++;
        }
        return i < values.length;
      }

      @Override
      public IntObjPair next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return new IntObjPair(i, values[i++]);
      }
    };
  }
}
