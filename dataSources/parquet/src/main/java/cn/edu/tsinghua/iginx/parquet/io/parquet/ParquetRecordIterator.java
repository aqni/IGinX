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

import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.parquet.util.exception.UnsupportedTypeException;
import cn.edu.tsinghua.iginx.parquet.util.record.ArrayHeader;
import cn.edu.tsinghua.iginx.parquet.util.record.ArrayRecord;
import cn.edu.tsinghua.iginx.parquet.util.record.RecordIterator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.*;

public class ParquetRecordIterator implements RecordIterator {

  private final IParquetReader reader;

  public ParquetRecordIterator(IParquetReader reader) {
    this.reader = reader;
  }

  private Header header;
  private Integer keyIndex;

  @Override
  public Header getHeader() throws UnsupportedTypeException {
    if (header == null) {
      MessageType schema = reader.getSchema();
      String prefix = schema.getName();
      List<Map.Entry<String, DataType>> fields = new ArrayList<>();
      for (Type type : schema.getFields()) {
        if (!type.isPrimitive()) {
          throw new UnsupportedTypeException("Parquet", type);
        }
        String name = type.getName();
        if (reader.isIginxData() && name.equals(Constants.KEY_FIELD_NAME)) {
          keyIndex = fields.size();
          continue;
        }
        DataType iType = IParquetReader.toIginxType(type.asPrimitiveType());
        String fullName = prefix.isEmpty() ? name : prefix + "." + name;
        fields.add(new AbstractMap.SimpleImmutableEntry<>(fullName, iType));
      }
      header = new ArrayHeader(fields);
    }
    return header;
  }

  @Override
  public Optional<Record> next() throws StorageException {
    try {
      IRecord record = this.reader.read();
      if (record == null) {
        return Optional.empty();
      }

      Long key = null;

      Object[] values = new Object[getHeader().size()];
      for (Map.Entry<Integer, Object> indexedValue : record) {
        int index = indexedValue.getKey();
        Object value = indexedValue.getValue();
        if (index == keyIndex) {
          key = (Long) value;
          continue;
        }
        values[index] = value;
      }

      if (key == null) {
        key = reader.getCurrentRowIndex();
      }

      return Optional.of(new ArrayRecord(key, values));
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
}
