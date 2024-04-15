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

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.parquet.io.parquet.ParquetRecordIterator;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;

import java.util.List;
import java.util.ListIterator;
import java.util.PriorityQueue;

public class MergedRecordIterator implements RecordIterator {

  private final PriorityQueue<Entry> queue;

  public MergedRecordIterator(Iterable<ParquetRecordIterator> iterators) {
    try{

    }catch (Exception e) {

      throw e;
    }

  }

  @Override
  public List<Field> header() throws StorageException {
    return null;
  }

  @Override
  public Record next() throws StorageException {
    return null;
  }

  @Override
  public void close() throws StorageException {

  }

  private static class Entry implements Comparable<Entry> {
    private final ParquetRecordIterator iterator;
    private final int order;
    private final int[] indexMap;

    public Entry(ParquetRecordIterator iterator, int order, int[] indexMap) {
      this.iterator = iterator;
      this.order = order;
      this.indexMap = indexMap;
    }

    private Record record;

    public boolean fetchNext() throws StorageException {
      record = iterator.next();
      return record != null;
    }

    public long getKey() {
      return record.getKey();
    }

    public void fill(List<Object> values) {
      List<Object> valueList = record.getValueList();
      ListIterator<Object> iterator = valueList.listIterator();
      while (iterator.hasNext()) {
        int index = iterator.nextIndex();
        Object value = iterator.next();
        values.set(indexMap[index], value);
      }
    }

    public void close() throws StorageException {
      iterator.close();
    }

    @Override
    public int compareTo(Entry o) {
      long key1 = record.getKey();
      long key2 = o.record.getKey();
      return (key1 == key2) ? Integer.compare(order, o.order) : Long.compare(key1, key2);
    }
  }
}
