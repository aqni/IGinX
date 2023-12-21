package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.parquet.entity.Field;
import cn.edu.tsinghua.iginx.parquet.entity.Table;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

@Deprecated
public class Loader {
  private final Path path;

  public Loader(Path path) {
    this.path = path;
  }

  public Path getPath() {
    return path;
  }

  public List<Field> getHeader() throws IOException {
    Table table = new Table();
    IParquetReader.Builder builder = IParquetReader.builder(path);
    try (IParquetReader reader = builder.build()) {
      MessageType schema = reader.getSchema();

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
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("unexpected reader error!", e);
    }
  }

  public void load(Table table) throws IOException {

    IParquetReader.Builder builder = IParquetReader.builder(path);
    try (IParquetReader reader = builder.build()) {
      MessageType schema = reader.getSchema();

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

      IRecord record;
      long cnt = 0;
      while ((record = reader.read()) != null) {
        Long key = null;
        if (keyIndex != null) {
          for (Map.Entry<Integer, Object> entry : record) {
            Integer index = entry.getKey();
            Object value = entry.getValue();
            if (index.equals(keyIndex)) {
              key = (Long) value;
              break;
            }
          }
          assert key != null;
        } else {
          key = cnt++;
        }

        List<Integer> indexList = new ArrayList<>();
        for (Map.Entry<Integer, Object> entry : record) {
          Integer index = entry.getKey();
          if (index.equals(keyIndex)) {
            continue;
          }
          indexList.add(index);
          Object value = entry.getValue();
          putValue(value, indexList, table, indexMap, key);
          indexList.clear();
        }
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("unexpected reader error!", e);
    }
  }

  private static void putValue(
      Object value,
      List<Integer> indexList,
      Table table,
      Map<List<Integer>, Integer> indexMap,
      Long key) {
    if (value instanceof IRecord) {
      IRecord record = (IRecord) value;
      for (Map.Entry<Integer, Object> entry : record) {
        Integer index = entry.getKey();
        Object v = entry.getValue();
        indexList.add(index);
        putValue(v, indexList, table, indexMap, key);
        indexList.remove(indexList.size() - 1);
      }
    } else {
      table.put(indexMap.get(indexList), key, value);
    }
  }

  private static void putIndexMap(
      Type type,
      List<String> typeNameList,
      List<Integer> indexList,
      Table table,
      Map<List<Integer>, Integer> indexMap) {
    typeNameList.add(type.getName());
    if (type.isPrimitive()) {
      PrimitiveType primitiveType = type.asPrimitiveType();
      DataType iginxType = ParquetMeta.toIginxType(primitiveType);
      String name = String.join(".", typeNameList);
      indexMap.put(new ArrayList<>(indexList), table.declareColumn(name, iginxType));
    } else {
      GroupType groupType = type.asGroupType();
      for (int i = 0; i < groupType.getFieldCount(); i++) {
        indexList.add(i);
        putIndexMap(groupType.getType(i), typeNameList, indexList, table, indexMap);
        indexList.remove(indexList.size() - 1);
      }
    }
    typeNameList.remove(typeNameList.size() - 1);
  }

  public static Integer getFieldIndex(MessageType schema, String fieldName) {
    try {
      return schema.getFieldIndex(fieldName);
    } catch (InvalidRecordException e) {
      return null;
    }
  }
}