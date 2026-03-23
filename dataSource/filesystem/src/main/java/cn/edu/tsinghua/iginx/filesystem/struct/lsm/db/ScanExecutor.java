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
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.FileLsm;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event.TableReadEvent;
import com.google.common.collect.*;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import lombok.Value;
import org.apache.arrow.util.Preconditions;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ScanExecutor {

  public static RowStream scan(List<Table> allHitTables, List<Field> fields, Filter filter)
      throws IOException, PhysicalException {
    ResultTableBuilder builder = new ResultTableBuilder(fields);
    RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(filter);
    Filter rangeFilter = FilterRangeUtils.filterOf(rangeSet);
    Filter filterWithoutRange = FilterRangeUtils.withoutKeyRangeSet(filter);

    List<TablePlan> plans = new ArrayList<>();
    for(Table table : allHitTables){
      List<SubTablePlan> subTablePlans = new ArrayList<>();
      List<Table.SubTable> subTables = table.getSubTables();
      for(int subTableId=0; subTableId<subTables.size(); subTableId++) {
        Table.SubTable subTable = subTables.get(subTableId);
        Table.Meta meta = subTable.getMeta();
        ImmutableMap<Field, Table.Statistic> fieldStats = meta.getFieldStats();
        List<Field> hitFields =
                fields.stream().filter(fieldStats::containsKey).collect(Collectors.toList());
        RangeSet<Long> hitRangeSet = TreeRangeSet.create();
        fieldStats.entrySet().stream()
                .filter(e -> hitFields.contains(e.getKey()))
                .map(Map.Entry::getValue)
                .map(Table.Statistic::getKeyRange)
                .forEach(hitRangeSet::add);
        hitRangeSet.removeAll(rangeSet.complement());
        if(!hitFields.isEmpty() && !hitRangeSet.isEmpty()){
          SubTablePlan subTablePlan = new SubTablePlan(subTableId, subTable, hitFields, hitRangeSet);
          subTablePlans.add(subTablePlan);
        }
      }
      TablePlan plan = new TablePlan(table, subTablePlans);
      plans.add(plan);
    }

    IdentityHashMap<SubTablePlan, Boolean> overlapMap = checkSubTablePlanOverlap(plans);

    boolean allPushDown = true;
    for(TablePlan plan : plans) {
      for (SubTablePlan subTablePlan : plan.getSubTablePlans()) {
        boolean hasOverlap = overlapMap.get(subTablePlan);
        List<Field> hitFields = subTablePlan.getFields();
        boolean canPushDown = !hasOverlap && hitFields.equals(fields);

        Filter subTableFilter = canPushDown ? filter : rangeFilter;
        TableReadEvent event = new TableReadEvent();
        event.tableName = plan.getTable().toString();
        event.subTableId = subTablePlan.getSubTableId();
        event.begin();
        try (RowStream rowStream = subTablePlan.getSubTable().scan(hitFields, subTableFilter)) {
          int rows = builder.put(rowStream);
          event.end();
          event.projectedSchema = rowStream.getHeader().toString();
          event.numRows=rows;
          if(rows > 0 && !canPushDown){
            allPushDown = false;
          }
        }finally {
          event.commit();
        }
      }
    }

    RowStream result = builder.build();
    if (!allPushDown && !Filters.isTrue(filterWithoutRange)) {
      result = new FilterRowStreamWrapper(result, filterWithoutRange);
    }
    return result;
  }

  private static IdentityHashMap<SubTablePlan, Boolean> checkSubTablePlanOverlap(List<TablePlan> plans){
    IdentityHashMap<SubTablePlan, Boolean> overlapMap = new IdentityHashMap<>();

    Map<Field, RangeMap<Long, SubTablePlan>> fieldToRangeMap = new HashMap<>();
    for(TablePlan plan : plans) {
      for (SubTablePlan subTablePlan : plan.getSubTablePlans()) {
        overlapMap.put(subTablePlan, false);

        for (Field field : subTablePlan.getFields()) {
          RangeMap<Long, SubTablePlan> rangeMap = fieldToRangeMap.computeIfAbsent(field, f -> TreeRangeMap.create());
          for (Range<Long> range : subTablePlan.getRange().asRanges()) {
            RangeMap<Long, SubTablePlan> overlapping = rangeMap.subRangeMap(range);
            for(SubTablePlan other : overlapping.asMapOfRanges().values()) {
              overlapMap.put(subTablePlan, true);
              overlapMap.put(other, true);
            }
            rangeMap.put(range, subTablePlan);
          }
        }
      }
    }
    return overlapMap;
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

    public int put(RowStream rowStream) throws PhysicalException {
      Header header = rowStream.getHeader();
      Preconditions.checkArgument(header.hasKey());
      int[] dstIndex = new int[header.getFieldSize()];
      for (int i = 0; i < dstIndex.length; i++) {
        Preconditions.checkArgument(indexOfField.containsKey(header.getField(i)));
        dstIndex[i] = indexOfField.getInt(header.getField(i));
      }
      int count = 0;
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
        count++;
      }
      return count;
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

  @Value
  private static class TablePlan {
    @lombok.NonNull
    Table table;
    @lombok.NonNull
    List<SubTablePlan> subTablePlans;
  }

  @Value
  private static class SubTablePlan {
    int subTableId;
    @lombok.NonNull
    Table.SubTable subTable;
    @lombok.NonNull
    List<Field> fields;
    @lombok.NonNull
    RangeSet<Long> range;
  }
}
