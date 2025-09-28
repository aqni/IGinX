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
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Shared;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import javax.annotation.Nullable;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileStorageManager extends FileStorageManager<TsFileStorageManager.TsFileTableMeta> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileStorageManager.class);

  public TsFileStorageManager(Shared shared, Path dir) {
    super(shared, dir, "tsfile");
  }

  @Override
  public String getName() {
    return super.getName() + "(tsfile)";
  }

  @Override
  protected TsFileTableMeta flush(
      TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner, Path path)
      throws IOException {
    Map<String, DataType> schema = meta.getSchema();
    Map<String, IMeasurementSchema> tsFileSchema = getTsFileSchema(schema);
    Map<String, String> deviceIds = getTsFileDeviceIds(schema.keySet());

    List<TSRecord> records = new ArrayList<>();
    try {
      while (scanner.iterate()) {
        long key = scanner.key();
        Map<String, Object> row = new HashMap<>();
        while (scanner.value().iterate()) {
          String field = scanner.value().key();
          Object value = scanner.value().value();
          row.put(field, value);
        }
        Collection<TSRecord> tsRecords = getTsRecords(key, row, tsFileSchema, deviceIds);
        records.addAll(tsRecords);
      }
    } catch (StorageException e) {
      throw new IOException(e);
    }

    long startTime = System.currentTimeMillis();
    try (TsFileWriter tsFileWriter = new TsFileWriter(path.toFile())) {
      for (String field : schema.keySet()) {
        String deviceId = deviceIds.get(field);
        IMeasurementSchema measurementSchema = tsFileSchema.get(field);
        tsFileWriter.registerTimeseries(deviceId, measurementSchema);
      }
      for (TSRecord tsRecord : records) {
        tsFileWriter.writeRecord(tsRecord);
      }
    } catch (WriteProcessException e) {
      throw new IOException(e);
    }
    long endTime = System.currentTimeMillis();
    LOGGER.info("write tsfile {} takes {} ms", path, (endTime - startTime));

    return new TsFileTableMeta(meta);
  }

  private static Collection<TSRecord> getTsRecords(
      long key,
      Map<String, Object> row,
      Map<String, IMeasurementSchema> schema,
      Map<String, String> deviceIds) {
    Map<String, TSRecord> deviceRecords = new HashMap<>();
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String field = entry.getKey();
      Object value = entry.getValue();
      if (value != null) {
        IMeasurementSchema measurementSchema = schema.get(field);
        TSDataType dataType = measurementSchema.getType();
        String measurementId = measurementSchema.getMeasurementName();
        String deviceId = deviceIds.get(field);
        TSRecord tsRecord = deviceRecords.computeIfAbsent(deviceId, d -> new TSRecord(d, key));
        switch (dataType) {
          case BOOLEAN:
            tsRecord.addPoint(measurementId, (boolean) value);
            break;
          case INT32:
            tsRecord.addPoint(measurementId, (int) value);
            break;
          case INT64:
            tsRecord.addPoint(measurementId, (long) value);
            break;
          case FLOAT:
            tsRecord.addPoint(measurementId, (float) value);
            break;
          case DOUBLE:
            tsRecord.addPoint(measurementId, (double) value);
            break;
          case TEXT:
            tsRecord.addPoint(measurementId, new String((byte[]) value));
            break;
          default:
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
      }
    }
    return deviceRecords.values();
  }

  private Map<String, String> getTsFileDeviceIds(Set<String> schema) {
    Map<String, String> deviceIds = new HashMap<>();
    for (String field : schema) {
      String deviceId = field.substring(0, field.lastIndexOf("."));
      deviceIds.put(field, deviceId);
    }
    return deviceIds;
  }

  private static Map<String, IMeasurementSchema> getTsFileSchema(Map<String, DataType> schema) {
    Map<String, IMeasurementSchema> tsFileSchema = new HashMap<>();
    for (Map.Entry<String, DataType> entry : schema.entrySet()) {
      String field = entry.getKey();
      String measurementId = field.substring(field.lastIndexOf(".") + 1);
      DataType dataType = entry.getValue();
      tsFileSchema.put(field, getMeasurementSchema(measurementId, dataType));
    }
    return tsFileSchema;
  }

  private static IMeasurementSchema getMeasurementSchema(String measurementId, DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return new MeasurementSchema(measurementId, TSDataType.BOOLEAN);
      case INTEGER:
        return new MeasurementSchema(measurementId, TSDataType.INT32);
      case LONG:
        return new MeasurementSchema(measurementId, TSDataType.INT64);
      case FLOAT:
        return new MeasurementSchema(measurementId, TSDataType.FLOAT);
      case DOUBLE:
        return new MeasurementSchema(measurementId, TSDataType.DOUBLE);
      case BINARY:
        return new MeasurementSchema(measurementId, TSDataType.TEXT);
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  @Override
  protected TsFileTableMeta readMeta(Path path) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  protected Scanner<Long, Scanner<String, Object>> scanFile(
      Path path, TsFileTableMeta meta, Set<String> fields, Filter filter) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  protected static class TsFileTableMeta implements FileStorageManager.CacheableTableMeta {
    private final TableMeta meta;

    protected TsFileTableMeta(TableMeta meta) {
      this.meta = Objects.requireNonNull(meta);
    }

    @Override
    public Map<String, DataType> getSchema() {
      return meta.getSchema();
    }

    @Override
    public Range<Long> getRange(String field) {
      return meta.getRange(field);
    }

    @Nullable
    @Override
    public Long getValueCount(String field) {
      return meta.getValueCount(field);
    }
  }
}
