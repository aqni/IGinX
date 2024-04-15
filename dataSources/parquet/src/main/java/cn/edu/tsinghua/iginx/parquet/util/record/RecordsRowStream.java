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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import java.util.NoSuchElementException;

public class RecordsRowStream implements RowStream {

  private final RecordIterator iterator;

  private final Header header;

  public RecordsRowStream(RecordIterator iterator) throws StorageException {
    this.iterator = iterator;
    header = new Header(Field.KEY, iterator.header());
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return header;
  }

  @Override
  public void close() throws PhysicalException {
    iterator.close();
  }

  private Row fetchedRecord = null;

  @Override
  public boolean hasNext() throws PhysicalException {
    if (fetchedRecord == null) {
      fetchedRecord = fetchNext();
    }
    return fetchedRecord != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    Row result = fetchedRecord;
    fetchedRecord = null;
    return result;
  }

  private Row fetchNext() throws StorageException {
    Record record = iterator.next();
    if (record == null) {
      return null;
    }
    return new Row(header, record.getKey(), record.getValueList().toArray());
  }
}
