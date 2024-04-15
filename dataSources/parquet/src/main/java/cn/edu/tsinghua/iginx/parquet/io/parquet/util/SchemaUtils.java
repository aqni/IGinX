package cn.edu.tsinghua.iginx.parquet.io.parquet.util;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.parquet.io.parquet.exception.UnsupportedParquetFormatException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SchemaUtils {

  @Immutable
  public static class RowMapper {
    private static final int KEY_INDEX = -1;
    private final Header header;
    private final List<Integer> mappedIndexes;

    public RowMapper(Header header, List<Integer> mappedIndexes) {
      this.header = header;
      this.mappedIndexes = Collections.unmodifiableList(mappedIndexes);
    }

    public Header getHeader() {
      return header;
    }

    public List<Integer> getMappedIndexes() {
      return mappedIndexes;
    }
  }

  public static RowMapper parseHeader(@Nonnull MessageType schema) throws UnsupportedParquetFormatException {
    Objects.requireNonNull(schema);

    List<Field> fields = new ArrayList<>();
    List<Integer> mappedIndexes = new ArrayList<>();

    boolean keyFound = false;
    for (Type type : schema.getFields()) {
      if (!type.isPrimitive()) {
        String msg = String.format("Non-primitive type %s is not supported", type.getName());
        throw new UnsupportedParquetFormatException(msg);
      }

      PrimitiveType primitiveType = type.asPrimitiveType();
      if (primitiveType.isRepetition(PrimitiveType.Repetition.REQUIRED)) {
        if (keyFound) {
          throw new UnsupportedParquetFormatException("Multiple key columns found in iginx schema");
        }
        keyFound = true;
        mappedIndexes.add(RowMapper.KEY_INDEX);
        continue;
      }

      mappedIndexes.add(fields.size());

      DataType iType = toIginxType(type.asPrimitiveType());



      String typeName = type.getName();

      ColumnKey columnKey = TagKVUtils.splitFullName(typeName);

      Field field = new Field(columnKey.getPath(), iType, columnKey.getTags());
      fields.add(field);
    }

    if (!keyFound) {
      throw new UnsupportedParquetFormatException("Key column not found in iginx schema");
    }

    Header header = new Header(Field.KEY, fields);

    return new RowMapper(header, mappedIndexes);
  }

  public static parseIndexMapper(@Nonnull MessageType schema) {
    Objects.requireNonNull(schema);

    int[] indexMapper = new int[schema.getFields().size()];
    for (int i = 0; i < schema.getFields().size(); i++) {
      indexMapper[i] = i;
    }
    return indexMapper;
  }


  @Nonnull
  public static Header parseDummyHeader(@Nonnull MessageType schema) {
    Objects.requireNonNull(schema);

  }

  public static DataType toIginxType(@Nonnull PrimitiveType.PrimitiveTypeName type) throws UnsupportedParquetFormatException {
    switch (type) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case INT32:
        return DataType.INTEGER;
      case INT64:
        return DataType.LONG;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case BINARY:
      case INT96:
      case FIXED_LEN_BYTE_ARRAY:
        return DataType.BINARY;
      default:
        throw new UnsupportedParquetFormatException("Unsupported parquet primitive type: " + type);
    }
  }

  public static DataType toIginxType(@Nonnull PrimitiveType primitiveType) throws UnsupportedParquetFormatException {
    if (primitiveType.getRepetition().equals(PrimitiveType.Repetition.REPEATED)) {
      throw new UnsupportedParquetFormatException("Repeated fields are not supported");
    }
    return toIginxType(primitiveType.getPrimitiveTypeName());
  }
}
