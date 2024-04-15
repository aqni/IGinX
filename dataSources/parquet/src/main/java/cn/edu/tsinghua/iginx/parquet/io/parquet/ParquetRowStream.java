package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.parquet.io.common.FileRowStream;
import cn.edu.tsinghua.iginx.parquet.io.exception.FileException;
import cn.edu.tsinghua.iginx.parquet.io.parquet.exception.UnsupportedParquetFormatException;
import cn.edu.tsinghua.iginx.parquet.io.parquet.util.SchemaUtils;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.parquet.util.record.Record;
import com.google.common.collect.Range;
import shaded.iginx.org.apache.parquet.schema.MessageType;

import javax.annotation.Nonnull;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@NotThreadSafe
public class ParquetRowStream extends FileRowStream {
  private final IParquetReader reader;
  private final Header header;

  public ParquetRowStream(@Nonnull @WillCloseWhenClosed IParquetReader reader) throws UnsupportedParquetFormatException {
    this.reader = Objects.requireNonNull(reader);
    this.header = getHeader(reader.getSchema(), reader.getMeta().getFileMetaData().getKeyValueMetaData());
  }

  private Header getHeader(MessageType schema, Map<String, String> meta) throws UnsupportedParquetFormatException {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(meta);

    String objectModelName = meta.get(IParquetWriter.OBJECT_MODEL_NAME_PROP);
    if (IParquetWriter.OBJECT_MODEL_NAME_VALUE.equals(objectModelName)) {
      return SchemaUtils.parseHeader(schema);
    } else {
      return SchemaUtils.parseDummyHeader(schema);
    }
  }

  @Override
  public Header getHeader() throws FileException {
    return header;
  }

  @Nonnull
  @Override
  public Record next() throws StorageException {
    try {
      IRecord record = this.reader.read();
      if (record == null) {
        return null;
      }

      Long key = null;
      int valueIndexOffset = 0;
      List<Object> values = new ArrayList<>(header().size());
      for (Map.Entry<Integer, Object> indexedValue : record) {
        Integer index = indexedValue.getKey();
        Object value = indexedValue.getValue();
        if (index.equals(schema.getKeyIndex())) {
          key = (Long) value;
          valueIndexOffset = -1;
          continue;
        }
        values.set(index + valueIndexOffset, value);
      }

      if (key == null) {
        key = reader.getCurrentRowIndex();
      }

      return new Record(key, values);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
  }

  @Override
  public void close() throws StorageException {
    try {
      reader.close();
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() throws FileException {
    return false;
  }

  public Range<Long> getRange() {
    return reader.getRange();
  }

}
