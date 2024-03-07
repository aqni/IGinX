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

package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.parquet.util.exception.UnsupportedTypeException;
import cn.edu.tsinghua.iginx.parquet.util.record.Record;
import cn.edu.tsinghua.iginx.parquet.util.record.RecordIterator;
import com.google.common.collect.Range;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParquetRecordIterator implements RecordIterator {

  private final IParquetReader reader;
  private final ParquetSchema schema;

  public ParquetRecordIterator(IParquetReader reader, List<String> prefix) throws UnsupportedTypeException {
    this.reader = reader;
    this.schema = new ParquetSchema(reader.getSchema(), reader.getMeta().getFileMetaData().getKeyValueMetaData(), prefix);
  }

  @Override
  public List<Field> header() throws UnsupportedTypeException {
    return schema.getHeader();
  }

  @Override
  public Record next() throws StorageException {
    try {
      IRecord record = this.reader.read();
      if (record == null) {
        return null;
      }

      Long key = null;
      int valueIndexOffset = 0;
      List<Object> values = new ArrayList<>(header().size());
      for (Map.Entry<Integer, Object> indexedValue : record) {
        Integer index = indexedValue.getKey();
        Object value = indexedValue.getValue();
        if (index.equals(schema.getKeyIndex())) {
          key = (Long) value;
          valueIndexOffset = -1;
          continue;
        }
        values.set(index + valueIndexOffset, value);
      }

      if (key == null) {
        key = reader.getCurrentRowIndex();
      }

      return new Record(key, values);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
  }

  @Override
  public void close() throws StorageException {
    try {
      reader.close();
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
  }

  public Range<Long> range() {
    return reader.getRange();
  }
}
