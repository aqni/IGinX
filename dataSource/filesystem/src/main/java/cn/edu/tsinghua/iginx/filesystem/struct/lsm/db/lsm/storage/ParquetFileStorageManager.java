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
import cn.edu.tsinghua.iginx.filesystem.format.parquet.IParquetReader;
import cn.edu.tsinghua.iginx.filesystem.format.parquet.IParquetWriter;
import cn.edu.tsinghua.iginx.filesystem.format.parquet.IRecord;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.manager.dummy.Storer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.iterator.IteratorScanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Constants;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Shared;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ColumnPath;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.Type;

public class ParquetFileStorageManager
    extends FileStorageManager<ParquetFileStorageManager.ParquetTableMeta> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFileStorageManager.class);

  public ParquetFileStorageManager(Shared shared, Path dir) {
    super(shared, dir, "parquet");
  }

  @Override
  public String getName() {
    return super.getName() + "(parquet)";
  }

  @Override
  protected ParquetTableMeta flush(
      TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner, Path path)
      throws IOException {
    MessageType parquetSchema = getMessageType(meta.getSchema());
    int maxBufferSize = shared.getStorageProperties().getParquetOutputBufferMaxSize();
    IParquetWriter.Builder builder = IParquetWriter.builder(path, parquetSchema, maxBufferSize);
    builder.withRowGroupSize(shared.getStorageProperties().getParquetRowGroupSize());
    builder.withPageSize((int) shared.getStorageProperties().getParquetPageSize());
    builder.withCompressionCodec(shared.getStorageProperties().getParquetCompression());

    try (IParquetWriter writer = builder.build()) {
      while (scanner.iterate()) {
        IRecord record = getRecord(parquetSchema, scanner.key(), scanner.value());
        writer.write(record);
      }
      ParquetMetadata parquetMeta = writer.flush();
      ParquetTableMeta tableMeta = ParquetTableMeta.of(parquetMeta);
      return tableMeta;
    } catch (Exception e) {
      throw new IOException("failed to write " + path, e);
    }
  }

  public static IRecord getRecord(MessageType schema, Long key, Scanner<String, Object> value)
      throws StorageException {
    IRecord record = new IRecord();
    record.add(
        schema.getFieldIndex(
            cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.Constants.KEY_FIELD_NAME),
        key);
    while (value.iterate()) {
      record.add(schema.getFieldIndex(value.key()), value.value());
    }
    record.sort();
    return record;
  }

  private static MessageType getMessageType(Map<String, DataType> schema) {
    List<Type> fields = new ArrayList<>();
    fields.add(
        Storer.getParquetType(Constants.KEY_FIELD_NAME, DataType.LONG, Type.Repetition.REQUIRED));
    for (Map.Entry<String, DataType> entry : schema.entrySet()) {
      String name = entry.getKey();
      DataType type = entry.getValue();
      fields.add(Storer.getParquetType(name, type, Type.Repetition.OPTIONAL));
    }
    MessageType parquetSchema = new MessageType(Constants.RECORD_FIELD_NAME, fields);
    return parquetSchema;
  }

  @Override
  protected ParquetTableMeta readMeta(Path path) throws IOException {
    try (IParquetReader reader = IParquetReader.builder(path).build()) {
      ParquetMetadata meta = reader.getMeta();
      return ParquetTableMeta.of(meta);
    } catch (Exception e) {
      throw new StorageRuntimeException(e);
    }
  }

  @Override
  protected Scanner<Long, Scanner<String, Object>> scanFile(
      Path path, ParquetTableMeta meta, Set<String> fields, Filter filter) throws IOException {
    IParquetReader.Builder builder = IParquetReader.builder(path);
    builder.project(fields);
    builder.filter(filter);

    IParquetReader reader = builder.build(meta.getMeta());

    Scanner<Long, Scanner<String, Object>> scanner = new ParquetScanner(reader);
    return scanner;
  }

  private static class ParquetScanner implements Scanner<Long, Scanner<String, Object>> {
    private final IParquetReader reader;
    private Long key;
    private Scanner<String, Object> rowScanner;

    public ParquetScanner(IParquetReader reader) {
      this.reader = reader;
    }

    @Override
    public Long key() throws NoSuchElementException {
      if (key == null) {
        throw new NoSuchElementException();
      }
      return key;
    }

    @Override
    public Scanner<String, Object> value() throws NoSuchElementException {
      if (rowScanner == null) {
        throw new NoSuchElementException();
      }
      return rowScanner;
    }

    @Override
    public boolean iterate() throws StorageException {
      try {
        IRecord record = reader.read();
        if (record == null) {
          key = null;
          rowScanner = null;
          return false;
        }
        Map<String, Object> map = new HashMap<>();
        MessageType parquetSchema = reader.getSchema();
        int keyIndex = parquetSchema.getFieldIndex(Constants.KEY_FIELD_NAME);
        for (Map.Entry<Integer, Object> entry : record) {
          int index = entry.getKey();
          Object value = entry.getValue();
          if (index == keyIndex) {
            key = (Long) value;
          } else {
            map.put(parquetSchema.getType(index).getName(), value);
          }
        }
        rowScanner = new IteratorScanner<>(map.entrySet().iterator());
        return true;
      } catch (Exception e) {
        throw new StorageException("failed to read", e);
      }
    }

    @Override
    public void close() throws StorageException {
      try {
        reader.close();
      } catch (Exception e) {
        throw new StorageException("failed to close", e);
      }
    }
  }

  protected static class ParquetTableMeta implements FileStorageManager.CacheableTableMeta {
    private final Map<String, DataType> schemaDst;
    private final Map<String, Range<Long>> rangeMap;
    private final Map<String, Long> countMap;
    private final ParquetMetadata meta;

    public static ParquetTableMeta of(ParquetMetadata meta) {
      Map<String, DataType> schemaDst = new HashMap<>();
      Map<String, Range<Long>> rangeMap = new HashMap<>();
      Map<String, Long> countMap = new HashMap<>();
      MessageType parquetSchema = meta.getFileMetaData().getSchema();

      Range<Long> ranges = IParquetReader.getRangeOf(meta);

      for (int i = 0; i < parquetSchema.getFieldCount(); i++) {
        Type type = parquetSchema.getType(i);
        if (type.getName().equals(Constants.KEY_FIELD_NAME)) {
          continue;
        }
        DataType iginxType = IParquetReader.toIginxType(type.asPrimitiveType());
        schemaDst.put(type.getName(), iginxType);
        rangeMap.put(type.getName(), ranges);
      }

      Map<ColumnPath, Long> columnPathMap = IParquetReader.getCountsOf(meta);
      columnPathMap.forEach(
          (columnPath, count) -> {
            String[] columnPathArray = columnPath.toArray();
            if (columnPathArray.length != 1) {
              throw new IllegalStateException("invalid column path: " + columnPath);
            }
            String name = columnPath.toArray()[0];
            countMap.put(name, count);
          });

      return new ParquetTableMeta(schemaDst, rangeMap, countMap, meta);
    }

    ParquetTableMeta(
        Map<String, DataType> schemaDst,
        Map<String, Range<Long>> rangeMap,
        Map<String, Long> countMap,
        ParquetMetadata meta) {
      this.schemaDst = schemaDst;
      this.rangeMap = rangeMap;
      this.countMap = countMap;
      this.meta = meta;
    }

    @Override
    public Map<String, DataType> getSchema() {
      return schemaDst;
    }

    @Override
    public Range<Long> getRange(String field) {
      if (!schemaDst.containsKey(field)) {
        throw new NoSuchElementException();
      }
      return rangeMap.get(field);
    }

    @Nullable
    @Override
    public Long getValueCount(String field) {
      if (!schemaDst.containsKey(field)) {
        throw new NoSuchElementException();
      }
      return countMap.get(field);
    }

    public ParquetMetadata getMeta() {
      return meta;
    }
  }
}
