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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.manager.data;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AggregatedRowStream implements RowStream {

  private final Row row;
  private boolean hasNext = true;

  public AggregatedRowStream(Map<String, ?> values, String functionName) {
    List<Field> fieldList = new ArrayList<>(values.size());
    List<Object> valuesList = new ArrayList<>(values.size());
    for (Map.Entry<String, ?> entry : values.entrySet()) {
      Object value = entry.getValue();
      valuesList.add(value);

      Map.Entry<String, Map<String, String>> pathWithTags =
          DataViewWrapper.parseFieldName(entry.getKey());
      String path = pathWithTags.getKey();
      Map<String, String> tags = pathWithTags.getValue();
      String pathWithFunctionName = functionName + "(" + path + ")";

      DataType dataType = typeFromValue(value);
      Field field = new Field(pathWithFunctionName, dataType, tags);
      fieldList.add(field);
    }
    row = new Row(new Header(fieldList), valuesList.toArray());
  }

  private static DataType typeFromValue(Object value) {
    if (value instanceof Long) {
      return DataType.LONG;
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + value.getClass().getName());
    }
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return row.getHeader();
  }

  @Override
  public void close() throws PhysicalException {}

  @Override
  public boolean hasNext() throws PhysicalException {
    return hasNext;
  }

  @Override
  public Row next() throws PhysicalException {
    hasNext = false;
    return row;
  }
}
