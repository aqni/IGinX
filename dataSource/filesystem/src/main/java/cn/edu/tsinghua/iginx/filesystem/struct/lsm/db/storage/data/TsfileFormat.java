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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.typesafe.config.Config;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class TsfileFormat extends ImmutableFileFormat {

  public TsfileFormat(Config fileConfig, CachePool cachePool) {
    super("tsfile");
  }

  @Override
  public void flush(Path dst, Table table) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  protected List<Table.SubTable> readSubTables(Path src) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }
}

//public class ArrowFileStorageManager
//    extends ImmutableFileStorageManager<ArrowFileStorageManager.ArrowFileTableMeta> {
//
//  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFileStorageManager.class);
//
//  private final int batchSize = 3970;
//
//  public ArrowFileStorageManager(Shared shared, Path dir) {
//    super(shared, dir, "arrow");
//  }
//
//  @Override
//  public String getName() {
//    return super.getName() + "(arrow)";
//  }
//
//  @Override
//  protected ArrowFileTableMeta flush(
//      TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner, Path path)
//      throws IOException {
//    try (RowStream rowStream = new ScannerRowStream(meta.getSchema(), scanner)) {
//      Table table = NaiveOperatorMemoryExecutor.transformToTable(rowStream);
//      long startTime = System.currentTimeMillis();
//      try (BatchStream batchStream = BatchStreams.wrap(shared.getAllocator(), table, batchSize);
//          VectorSchemaRoot root =
//              VectorSchemaRoot.create(batchStream.getSchema().raw(), shared.getAllocator());
//          WritableByteChannel channel =
//              Files.newByteChannel(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
//          ArrowFileWriter writer =
//              new ArrowFileWriter(
//                  root,
//                  null,
//                  channel,
//                  null,
//                  IpcOption.DEFAULT,
//                  new FastestCompressionFactory(),
//                  CompressionUtil.CodecType.LZ4_FRAME)) {
//        writer.start();
//        while (batchStream.hasNext()) {
//          try (Batch batch = batchStream.getNext()) {
//            VectorSchemaRoots.transfer(root, batch.getData());
//            writer.writeBatch();
//          }
//        }
//        writer.end();
//      }
//      long endTime = System.currentTimeMillis();
//      LOGGER.info("write arrow file {} takes {} ms", path, (endTime - startTime));
//    } catch (PhysicalException e) {
//      throw new IOException(e);
//    }
//    return new ArrowFileTableMeta(meta);
//  }
//
//  @Override
//  protected ArrowFileTableMeta readMeta(Path path) throws IOException {
//    throw new UnsupportedOperationException("unimplemented");
//  }
//
//  @Override
//  protected Scanner<Long, Scanner<String, Object>> scanFile(
//      Path path, ArrowFileTableMeta meta, Set<String> fields, Filter filter) throws IOException {
//    throw new UnsupportedOperationException("unimplemented");
//  }
//
//  protected static class ArrowFileTableMeta implements ImmutableFileStorageManager.CacheableTableMeta {
//    private final TableMeta meta;
//
//    protected ArrowFileTableMeta(TableMeta meta) {
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
//
//  public static class FastestCompressionFactory implements CompressionCodec.Factory {
//
//    public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
//      switch (codecType) {
//        case LZ4_FRAME:
//          return new FastestLz4CompressionCodec();
//        default:
//          return CommonsCompressionFactory.INSTANCE.createCodec(codecType);
//      }
//    }
//
//    public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel) {
//      switch (codecType) {
//        case LZ4_FRAME:
//          return new FastestLz4CompressionCodec();
//        default:
//          return CommonsCompressionFactory.INSTANCE.createCodec(codecType, compressionLevel);
//      }
//    }
//  }
//
//  public static class FastestLz4CompressionCodec extends AbstractCompressionCodec {
//
//    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
//      Preconditions.checkArgument(
//          uncompressedBuffer.writerIndex() <= 2147483647L,
//          "The uncompressed buffer size exceeds the integer limit %s.",
//          Integer.MAX_VALUE);
//
//      LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
//
//      int uncompressedLength = (int) uncompressedBuffer.writerIndex();
//      int maxCompressedLength = compressor.maxCompressedLength(uncompressedLength) + 100;
//      ArrowBuf compressedBuffer = allocator.buffer(8L + maxCompressedLength);
//
//      ByteBuffer nioUncompressedBuffer = uncompressedBuffer.nioBuffer(0, uncompressedLength);
//      ByteBuffer nioCompressedBuffer = compressedBuffer.nioBuffer(8L, maxCompressedLength);
//
//      // TODO: make it compatible with LZ4FrameOutputStream
//      compressor.compress(nioUncompressedBuffer, nioCompressedBuffer);
//      //      try(LZ4FrameOutputStream lz4FrameOutputStream = new LZ4FrameOutputStream(new
//      // ByteBufferOutputStream(nioCompressedBuffer))) {
//      //        IOUtils.copy(new ByteBufferInputStream(nioUncompressedBuffer),
//      // lz4FrameOutputStream);
//      //      } catch (IOException e) {
//      //        throw new RuntimeException(e);
//      //      }
//
//      int compressedLength = nioCompressedBuffer.position();
//      compressedBuffer.writerIndex(8L + compressedLength);
//      return compressedBuffer;
//    }
//
//    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
//      throw new UnsupportedOperationException("unimplemented");
//    }
//
//    public CompressionUtil.CodecType getCodecType() {
//      return CompressionUtil.CodecType.LZ4_FRAME;
//    }
//  }
//}

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
//package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data;
//
//import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
//import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.ImmutableFileStorageManager;
//import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.scanner.Scanner;
//import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.Shared;
//import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.StorageException;
//import cn.edu.tsinghua.iginx.thrift.DataType;
//import com.google.common.collect.Range;
//import java.io.IOException;
//import java.nio.file.Path;
//import java.util.*;
//import javax.annotation.Nullable;
//import org.apache.tsfile.enums.TSDataType;
//import org.apache.tsfile.exception.write.WriteProcessException;
//import org.apache.tsfile.write.TsFileWriter;
//import org.apache.tsfile.write.record.TSRecord;
//import org.apache.tsfile.write.schema.IMeasurementSchema;
//import org.apache.tsfile.write.schema.MeasurementSchema;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class TsFileStorageManager extends ImmutableFileStorageManager<TsFileStorageManager.TsFileTableMeta> {
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
//    Map<String, DataType> schema = meta.getSchema();
//    Map<String, IMeasurementSchema> tsFileSchema = getTsFileSchema(schema);
//    Map<String, String> deviceIds = getTsFileDeviceIds(schema.keySet());
//
//    List<TSRecord> records = new ArrayList<>();
//    try {
//      while (scanner.iterate()) {
//        long key = scanner.key();
//        Map<String, Object> row = new HashMap<>();
//        while (scanner.value().iterate()) {
//          String field = scanner.value().key();
//          Object value = scanner.value().value();
//          row.put(field, value);
//        }
//        Collection<TSRecord> tsRecords = getTsRecords(key, row, tsFileSchema, deviceIds);
//        records.addAll(tsRecords);
//      }
//    } catch (StorageException e) {
//      throw new IOException(e);
//    }
//
//    Map<String, List<IMeasurementSchema>> groupedSchema = new HashMap<>();
//    for (String field : schema.keySet()) {
//      String deviceId = deviceIds.get(field);
//      IMeasurementSchema measurementSchema = tsFileSchema.get(field);
//      groupedSchema.computeIfAbsent(deviceId, k -> new ArrayList<>()).add(measurementSchema);
//    }
//
//    long startTime = System.currentTimeMillis();
//    try (TsFileWriter tsFileWriter = new TsFileWriter(path.toFile())) {
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
//    long endTime = System.currentTimeMillis();
//    LOGGER.info("write tsfile {} takes {} ms", path, (endTime - startTime));
//
//    return new TsFileTableMeta(meta);
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
//  private static IMeasurementSchema getMeasurementSchema(String measurementId, DataType dataType) {
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
//  protected static class TsFileTableMeta implements ImmutableFileStorageManager.CacheableTableMeta {
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
//}

