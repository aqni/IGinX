package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

public class Untag implements MappingFunction {

  @Override
  public RowStream transform(Table table, FunctionParams params) throws Exception {
    Header sourceHeader = table.getHeader();
    List<Field> sourceFields = sourceHeader.getFields();

    List<Field> tagFields = extractTagKey(sourceFields);
    List<Field> resultFields = new ArrayList<>(tagFields);
    resultFields.addAll(untag(sourceFields));
    Header resultHeader = new Header(sourceHeader.getKey(), resultFields);

    List<String[]> tagValuesList = getTagValuesList(sourceFields, tagFields);
    int[] rowIndexMapping = getRowIndexMapping(sourceFields, tagFields, tagValuesList);
    int[] columnIndexMapping = getColumnIndexMapping(sourceFields, resultHeader);

    return transform(table, resultHeader, tagValuesList, rowIndexMapping, columnIndexMapping);
  }

  private static RowStream transform(
      Table table,
      Header resultHeader,
      List<String[]> tagValuesList,
      int[] rowIndexMapping,
      int[] columnIndexMapping) {
    List<Row> rows = table.getRows().stream()
        .map(row -> transform(row, resultHeader, tagValuesList, rowIndexMapping, columnIndexMapping))
        .flatMap(List::stream)
        .collect(Collectors.toList());
    return new Table(resultHeader, rows);
  }

  private static List<Row> transform(Row row,
                                     Header resultHeader,
                                     List<String[]> tagValuesList,
                                     int[] rowIndexMapping,
                                     int[] columnIndexMapping) {
    List<Row> resultRows = new ArrayList<>(tagValuesList.size());

    for (String[] tagValues : tagValuesList) {
      Object[] values = Arrays.copyOf(tagValues, resultHeader.getFieldSize());
      Row resultRow = new Row(resultHeader, row.getKey(), values);
      resultRows.add(resultRow);
    }

    Object[] sourceValues = row.getValues();
    for (int i = 0; i < sourceValues.length; i++) {
      int columnIndex = columnIndexMapping[i];
      int rowIndex = rowIndexMapping[i];
      resultRows.get(rowIndex).getValues()[columnIndex] = sourceValues[i];
    }

    return resultRows;
  }

  private static int[] getRowIndexMapping(List<Field> sourceFields, List<Field> tagFields, List<String[]> tagValuesList) {
    Map<Map<String, String>, Integer> tagToIndex = new HashMap<>();
    for (String[] tagValues : tagValuesList) {
      tagToIndex.put(getTag(tagValues, tagFields), tagToIndex.size());
    }
    int[] rowIndexMapping = new int[sourceFields.size()];
    for (int i = 0; i < sourceFields.size(); i++) {
      rowIndexMapping[i] = tagToIndex.get(sourceFields.get(i).getTags());
    }
    return rowIndexMapping;
  }

  private static List<String[]> getTagValuesList(List<Field> sourceFields, List<Field> tagFields) {
    return sourceFields.stream()
        .map(Field::getTags)
        .distinct()
        .map(tag -> getTagValues(tag, tagFields))
        .collect(Collectors.toList());
  }

  private static String[] getTagValues(Map<String, String> tag, List<Field> tagFields) {
    String[] tagValues = new String[tagFields.size()];
    for (int i = 0; i < tagFields.size(); i++) {
      tagValues[i] = tag.get(tagFields.get(i).getName());
    }
    return tagValues;
  }

  private static Map<String, String> getTag(String[] tagValues, List<Field> tagFields) {
    Map<String, String> tag = new HashMap<>();
    for (int i = 0; i < tagFields.size(); i++) {
      tag.put(tagFields.get(i).getName(), tagValues[i]);
    }
    return tag;
  }

  private static int[] getColumnIndexMapping(List<Field> sourceFields, Header resultHeader) {
    int[] columnIndexMapping = new int[sourceFields.size()];
    for (int i = 0; i < sourceFields.size(); i++) {
      Field sourceField = sourceFields.get(i);
      columnIndexMapping[i] = resultHeader.indexOf(sourceField.getName());
    }
    return columnIndexMapping;
  }

  private static List<Field> extractTagKey(List<Field> fields) {
    return fields.stream()
        .map(Field::getTags)
        .map(Map::keySet)
        .flatMap(Set::stream)
        .distinct()
        .map(tagKey -> new Field(tagKey, DataType.BINARY))
        .collect(Collectors.toList());
  }

  private static List<Field> untag(List<Field> fields) {
    return fields.stream()
        .map(field -> new Field(field.getName(), field.getType()))
        .distinct()
        .sorted(Comparator.comparing(Field::getName))
        .collect(Collectors.toList());
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.System;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.Mapping;
  }

  @Override
  public String getIdentifier() {
    return "untag";
  }
}
