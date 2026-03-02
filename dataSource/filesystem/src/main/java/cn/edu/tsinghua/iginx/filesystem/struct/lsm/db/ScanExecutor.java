package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.FilterRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.ArrowFields;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.FilterRangeUtils;
import com.google.common.collect.RangeSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ScanExecutor {

  public static RowStream scan(List<Table> allHitTables, List<Field> fields, Filter filter) throws IOException, PhysicalException {
    ResultTableBuilder builder = new ResultTableBuilder(fields);
    RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(filter);
    Filter rangeFilter = FilterRangeUtils.filterOf(rangeSet);
    for (Table table : allHitTables) {
      for (Table.SubTable subTable : table.getSubTables()) {
        try (RowStream rowStream = subTable.scan(fields, rangeFilter)) {
          builder.put(rowStream);
        }
      }
    }
    return new FilterRowStreamWrapper(builder.build(), filter);
  }

  private static class ResultTableBuilder {

    private final Header header;
    private final Object2IntOpenHashMap<cn.edu.tsinghua.iginx.engine.shared.data.read.Field> indexOfField = new Object2IntOpenHashMap<>();
    private final Long2ObjectOpenHashMap<Object[]> keyToValues = new Long2ObjectOpenHashMap<>();

    public ResultTableBuilder(List<Field> fields) {
      this.header = new Header(
          cn.edu.tsinghua.iginx.engine.shared.data.read.Field.KEY,
          fields.stream().map(ArrowFields::toIginxField).collect(Collectors.toList()));
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
      List<Row> rows = keyToValues.long2ObjectEntrySet().stream()
          .sorted(Comparator.comparingLong(Long2ObjectMap.Entry::getLongKey))
          .map(e -> new Row(header, e.getLongKey(), e.getValue()))
          .collect(Collectors.toList());
      keyToValues.clear();
      return new cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table(header, rows);
    }
  }
}
