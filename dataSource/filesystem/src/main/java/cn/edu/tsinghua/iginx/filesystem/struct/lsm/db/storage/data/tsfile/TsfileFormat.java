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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.tsfile;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.SparseImmutableFileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class TsfileFormat extends SparseImmutableFileFormat {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsfileFormat.class);

  private final TsfileConfig config;

  public TsfileFormat(Config config, CachePool cachePool) {
    super("tsfile", cachePool);
    this.config = TsfileConfig.of(config);
  }

  @Override
  protected void flush(Path dstWithSuffix, List<Table.SubTable> subTables) throws IOException, PhysicalException {
//    try (TsFileWriter tsFileWriter = new TsFileWriter(dstWithSuffix.toFile())) {
//      for (int subTableIndex = 0; subTableIndex < subTables.size(); subTableIndex++) {
//        String deviceId = String.format("subtable%010d", subTableIndex);
//        Table.SubTable subTable = subTables.get(subTableIndex);
//        try (RowStream rowStream = scanAll(subTable)) {
//          Header header = rowStream.getHeader();
//          List<IMeasurementSchema> schema = TypeUtils.toTsfileSchema(subTable.getMeta().getSchema());
//        }
//        List<IMeasurementSchema> schema = subTableSchemas.get(subTableIndex);
//        List<TSRecord> records = subTableRecords.get(subTableIndex);
//        tsFileWriter.registerAlignedTimeseries(deviceId, schema);
//        for (TSRecord tsRecord : records) {
//          tsFileWriter.writeRecord(tsRecord);
//        }
//      }
//      for (Map.Entry<String, List<IMeasurementSchema>> entry : groupedSchema.entrySet()) {
//        String deviceId = entry.getKey();
//        List<IMeasurementSchema> schemaList = entry.getValue();
//        tsFileWriter.registerAlignedTimeseries(deviceId, schemaList);
//      }
//
//      for (TSRecord tsRecord : records) {
//        tsFileWriter.writeRecord(tsRecord);
//      }
//    } catch (WriteProcessException e) {
//      throw new IOException(e);
//    }
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  protected Object loadFileMeta(Path srcWithSuffix) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  protected List<String> readSubTableNames(Path srcWithSuffix) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  protected Table.Meta loadMeta(Path srcWithSuffix, String subTableName) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  protected RowStream scan(Path srcWithSuffix, String subTableName, List<Field> fields, Filter predicate) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }
}


/// *
// * IGinX - the polystore system with high performance
// * Copyright (C) Tsinghua University
// * TSIGinX@gmail.com
// *
// * This program is free software; you can redistribute it and/or
// * modify it under the terms of the GNU Lesser General Public
// * License as published by the Free Software Foundation; either
// * version 3 of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// * Lesser General Public License for more details.
// *
// * You should have received a copy of the GNU Lesser General Public License
// * along with this program; if not, write to the Free Software Foundation,
// * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
// */
// package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data;
//

//
// public class TsFileStorageManager extends
// ImmutableFileStorageManager<TsFileStorageManager.TsFileTableMeta> {
//
//  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileStorageManager.class);
//
//  public TsFileStorageManager(Shared shared, Path dir) {
//    super(shared, dir, "tsfile");
//  }
//
//  @Override
//  public String getName() {
//    return super.getName() + "(tsfile)";
//  }
//
//  @Override
//  protected TsFileTableMeta flush(
//      TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner, Path path)
//      throws IOException {

//  }
//
//  private static Collection<TSRecord> getTsRecords(
//      long key,
//      Map<String, Object> row,
//      Map<String, IMeasurementSchema> schema,
//      Map<String, String> deviceIds) {
//    Map<String, TSRecord> deviceRecords = new HashMap<>();
//    for (Map.Entry<String, Object> entry : row.entrySet()) {
//      String field = entry.getKey();
//      Object value = entry.getValue();
//      if (value != null) {
//        IMeasurementSchema measurementSchema = schema.get(field);
//        TSDataType dataType = measurementSchema.getType();
//        String measurementId = measurementSchema.getMeasurementName();
//        String deviceId = deviceIds.get(field);
//        TSRecord tsRecord = deviceRecords.computeIfAbsent(deviceId, d -> new TSRecord(d, key));
//        switch (dataType) {
//          case BOOLEAN:
//            tsRecord.addPoint(measurementId, (boolean) value);
//            break;
//          case INT32:
//            tsRecord.addPoint(measurementId, (int) value);
//            break;
//          case INT64:
//            tsRecord.addPoint(measurementId, (long) value);
//            break;
//          case FLOAT:
//            tsRecord.addPoint(measurementId, (float) value);
//            break;
//          case DOUBLE:
//            tsRecord.addPoint(measurementId, (double) value);
//            break;
//          case TEXT:
//            tsRecord.addPoint(measurementId, new String((byte[]) value));
//            break;
//          default:
//            throw new IllegalArgumentException("Unsupported data type: " + dataType);
//        }
//      }
//    }
//    return deviceRecords.values();
//  }
//
//  private Map<String, String> getTsFileDeviceIds(Set<String> schema) {
//    Map<String, String> deviceIds = new HashMap<>();
//    for (String field : schema) {
//      String deviceId = field.substring(0, field.lastIndexOf("."));
//      deviceIds.put(field, deviceId);
//    }
//    return deviceIds;
//  }
//
//  private static Map<String, IMeasurementSchema> getTsFileSchema(Map<String, DataType> schema) {
//    Map<String, IMeasurementSchema> tsFileSchema = new HashMap<>();
//    for (Map.Entry<String, DataType> entry : schema.entrySet()) {
//      String field = entry.getKey();
//      String measurementId = field.substring(field.lastIndexOf(".") + 1);
//      DataType dataType = entry.getValue();
//      tsFileSchema.put(field, getMeasurementSchema(measurementId, dataType));
//    }
//    return tsFileSchema;
//  }
//
//  private static IMeasurementSchema getMeasurementSchema(String measurementId, DataType dataType)
// {
//    switch (dataType) {
//      case BOOLEAN:
//        return new MeasurementSchema(measurementId, TSDataType.BOOLEAN);
//      case INTEGER:
//        return new MeasurementSchema(measurementId, TSDataType.INT32);
//      case LONG:
//        return new MeasurementSchema(measurementId, TSDataType.INT64);
//      case FLOAT:
//        return new MeasurementSchema(measurementId, TSDataType.FLOAT);
//      case DOUBLE:
//        return new MeasurementSchema(measurementId, TSDataType.DOUBLE);
//      case BINARY:
//        return new MeasurementSchema(measurementId, TSDataType.TEXT);
//      default:
//        throw new IllegalArgumentException("Unsupported data type: " + dataType);
//    }
//  }
//
//  @Override
//  protected TsFileTableMeta readMeta(Path path) throws IOException {
//    throw new UnsupportedOperationException("unimplemented");
//  }
//
//  @Override
//  protected Scanner<Long, Scanner<String, Object>> scanFile(
//      Path path, TsFileTableMeta meta, Set<String> fields, Filter filter) throws IOException {
//    throw new UnsupportedOperationException("unimplemented");
//  }
//
//  protected static class TsFileTableMeta implements ImmutableFileStorageManager.CacheableTableMeta
// {
//    private final TableMeta meta;
//
//    protected TsFileTableMeta(TableMeta meta) {
//      this.meta = Objects.requireNonNull(meta);
//    }
//
//    @Override
//    public Map<String, DataType> getSchema() {
//      return meta.getSchema();
//    }
//
//    @Override
//    public Range<Long> getRange(String field) {
//      return meta.getRange(field);
//    }
//
//    @Nullable
//    @Override
//    public Long getValueCount(String field) {
//      return meta.getValueCount(field);
//    }
//  }
// }
