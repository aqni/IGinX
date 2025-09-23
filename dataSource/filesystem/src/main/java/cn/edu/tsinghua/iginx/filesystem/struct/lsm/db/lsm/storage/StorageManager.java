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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.storage;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public interface StorageManager {

  String getName();

  void flush(String name, TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner)
      throws IOException;

  TableMeta readMeta(String name) throws IOException;

  Scanner<Long, Scanner<String, Object>> scanData(
      String name, Set<String> fields, RangeSet<Long> ranges, Filter predicate) throws IOException;

  void delete(String name, AreaSet<Long, String> areas) throws IOException;

  void delete(String name);

  Iterable<String> reload() throws IOException;

  void clear() throws IOException;

  interface TableMeta {
    Map<String, DataType> getSchema();

    Range<Long> getRange(String field);

    default Range<Long> getRange(Iterable<String> fields) {
      Range<Long> range = null;
      for (String field : fields) {
        Range<Long> fieldRange = getRange(field);
        if (range == null) {
          range = getRange(field);
        } else {
          range = range.span(fieldRange);
        }
      }
      return range;
    }

    @Nullable
    Long getValueCount(String field);
  }
}
