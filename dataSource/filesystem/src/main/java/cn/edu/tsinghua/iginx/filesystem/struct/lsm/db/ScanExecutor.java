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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import com.google.common.collect.RangeSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.arrow.util.Preconditions;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ScanExecutor {

  public static RowStream scan(List<Table> allHitTables, List<Field> fields, Filter filter)
      throws IOException, PhysicalException {
    ResultTableBuilder builder = new ResultTableBuilder(fields);
    RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(filter);
    Filter rangeFilter = FilterRangeUtils.filterOf(rangeSet);
    Filter filterWithoutRange = FilterRangeUtils.withoutKeyRangeSet(filter);

    for (Table table : allHitTables) {
      for (Table.SubTable subTable : table.getSubTables()) {
        Set<Field> subTableFields = subTable.getMeta().getFieldStats().keySet();
        List<Field> hitFields =
            fields.stream().filter(subTableFields::contains).collect(Collectors.toList());
        if(!hitFields.isEmpty()){
          try (RowStream rowStream = subTable.scan(hitFields, rangeFilter)) {
            builder.put(rowStream);
          }
        }
      }
    }

    RowStream result = builder.build();
    if (!Filters.isTrue(filterWithoutRange)) {
      result = new FilterRowStreamWrapper(result, filterWithoutRange);
    }
    return result;
  }

  private static class ResultTableBuilder {

    private final Header header;
    private final Object2IntOpenHashMap<cn.edu.tsinghua.iginx.engine.shared.data.read.Field>
        indexOfField = new Object2IntOpenHashMap<>();
    private final Long2ObjectOpenHashMap<Object[]> keyToValues = new Long2ObjectOpenHashMap<>();

    public ResultTableBuilder(List<Field> fields) {
      this.header = new Header(Field.KEY, fields);
      IntStream.range(0, fields.size()).forEach(i -> indexOfField.put(header.getField(i), i));
      Preconditions.checkArgument(indexOfField.size() == fields.size());
    }

    public void put(RowStream rowStream) throws PhysicalException {
      Header header = rowStream.getHeader();
      Preconditions.checkArgument(header.hasKey());
      int[] dstIndex = new int[header.getFieldSize()];
      for (int i = 0; i < dstIndex.length; i++) {
        Preconditions.checkArgument(indexOfField.containsKey(header.getField(i)));
        dstIndex[i] = indexOfField.getInt(header.getField(i));
      }
      while (rowStream.hasNext()) {
        Row row = rowStream.next();
        long key = row.getKey();
        Object[] values = row.getValues();
        Object[] dstValues = keyToValues.computeIfAbsent(key, k -> new Object[indexOfField.size()]);
        for (int i = 0; i < dstIndex.length; i++) {
          Object value = values[i];
          if (value != null) {
            dstValues[dstIndex[i]] = value;
          }
        }
      }
    }

    public cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table build() {
      List<Row> rows =
          keyToValues.long2ObjectEntrySet().stream()
              .sorted(Comparator.comparingLong(Long2ObjectMap.Entry::getLongKey))
              .map(e -> new Row(header, e.getLongKey(), e.getValue()))
              .collect(Collectors.toList());
      keyToValues.clear();
      return new cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table(header, rows);
    }
  }
}
