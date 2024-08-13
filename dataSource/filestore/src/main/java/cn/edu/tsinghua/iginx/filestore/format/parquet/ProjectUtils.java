/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.filestore.format.parquet;

import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.manager.dummy.Storer;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.manager.dummy.Table;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.Constants;

import java.io.IOException;
import java.util.*;
import javax.annotation.Nullable;

import cn.edu.tsinghua.iginx.thrift.DataType;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.Type;
import shaded.iginx.org.apache.parquet.schema.Types;

public class ProjectUtils {
  private ProjectUtils() {}

  static MessageType projectMessageType(MessageType schema, @Nullable Set<String> fields) {
    Set<String> schemaFields = new HashSet<>(Objects.requireNonNull(fields));
    schemaFields.add(Constants.KEY_FIELD_NAME);

    Types.MessageTypeBuilder builder = Types.buildMessage();
    for (String field : schemaFields) {
      if (schema.containsField(field)) {
        Type type = schema.getType(field);
        if (!type.isPrimitive()) {
          throw new IllegalArgumentException("not primitive type is not supported: " + field);
        }
        builder.addField(schema.getType(field));
      }
    }

    return builder.named(schema.getName());
  }

  static Map<String, DataType> toIginxSchema(MessageType schema) {
    Table table = new Table();


      Integer keyIndex = getFieldIndex(schema, Storer.KEY_FIELD_NAME);
      Map<List<Integer>, Integer> indexMap = new HashMap<>();
      List<Integer> schameIndexList = new ArrayList<>();
      List<String> typeNameList = new ArrayList<>();
      for (int i = 0; i < schema.getFieldCount(); i++) {
        if (keyIndex != null && keyIndex == i) {
          continue;
        }
        schameIndexList.add(i);
        putIndexMap(schema.getType(i), typeNameList, schameIndexList, table, indexMap);
        schameIndexList.clear();
      }
      return table.getHeader();

  }
}
