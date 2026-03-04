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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.DenseImmutableFileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.typesafe.config.Config;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class ParquetFormat extends DenseImmutableFileFormat {

  public ParquetFormat(Config fileConfig, CachePool cachePool) {
    super("parquet", cachePool);
  }

  @Override
  protected void flush(Path dst, Table.SubTable subTable) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  protected Table.Meta loadMeta(Path src) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  protected RowStream scan(Path src, List<Field> fields, Filter predicate) throws IOException {
    throw new UnsupportedOperationException("unimplemented");
  }
}

// public class ArrowFileStorageManager
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
//  protected static class ArrowFileTableMeta implements
// ImmutableFileStorageManager.CacheableTableMeta {
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
//    public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel)
// {
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
// }
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
// import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
// import cn.edu.tsinghua.iginx.filesystem.format.parquet.IParquetReader;
// import cn.edu.tsinghua.iginx.filesystem.format.parquet.IParquetWriter;
// import cn.edu.tsinghua.iginx.filesystem.format.parquet.IRecord;
// import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.manager.dummy.Storer;
// import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.ImmutableFileStorageManager;
// import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.scanner.IteratorScanner;
// import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.scanner.Scanner;
// import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Constants;
// import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.Shared;
// import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.StorageException;
// import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.exception.StorageRuntimeException;
// import cn.edu.tsinghua.iginx.thrift.DataType;
// import com.google.common.collect.Range;
// import java.io.IOException;
// import java.nio.file.*;
// import java.util.*;
// import javax.annotation.Nullable;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import shaded.iginx.org.apache.parquet.hadoop.metadata.ColumnPath;
// import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
// import shaded.iginx.org.apache.parquet.schema.MessageType;
// import shaded.iginx.org.apache.parquet.schema.Type;
//
// public class ParquetFileStorageManager
//    extends ImmutableFileStorageManager<ParquetFileStorageManager.ParquetTableMeta> {
//
//  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFileStorageManager.class);
//
//  public ParquetFileStorageManager(Shared shared, Path dir) {
//    super(shared, dir, "parquet");
//  }
//
//  @Override
//  public String getName() {
//    return super.getName() + "(parquet)";
//  }
//
//  @Override
//  protected ParquetTableMeta flush(
//      TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner, Path path)
//      throws IOException {
//    MessageType parquetSchema = getMessageType(meta.getSchema());
//    int maxBufferSize = shared.getStorageProperties().getParquetOutputBufferMaxSize();
//    IParquetWriter.Builder builder = IParquetWriter.builder(path, parquetSchema, maxBufferSize);
//    builder.withRowGroupSize(shared.getStorageProperties().getParquetRowGroupSize());
//    builder.withPageSize((int) shared.getStorageProperties().getParquetPageSize());
//    builder.withCompressionCodec(shared.getStorageProperties().getParquetCompression());
//
//    List<IRecord> allRecords = new ArrayList<>();
//    try {
//      while (scanner.iterate()) {
//        IRecord record = getRecord(parquetSchema, scanner.key(), scanner.value());
//        allRecords.add(record);
//      }
//    } catch (Exception e) {
//      throw new IOException("failed to write " + path, e);
//    }
//
//    ParquetTableMeta tableMeta;
//
//    long startTime = System.currentTimeMillis();
//    try (IParquetWriter writer = builder.build()) {
//      for (IRecord record : allRecords) {
//        writer.write(record);
//      }
//      ParquetMetadata parquetMeta = writer.flush();
//      tableMeta = ParquetTableMeta.of(parquetMeta);
//    } catch (Exception e) {
//      throw new IOException("failed to write " + path, e);
//    }
//    long endTime = System.currentTimeMillis();
//    LOGGER.info("write parquet file {} takes {} ms", path, (endTime - startTime));
//
//    return tableMeta;
//  }
//
//  public static IRecord getRecord(MessageType schema, Long key, Scanner<String, Object> value)
//      throws StorageException {
//    IRecord record = new IRecord();
//    record.add(
//        schema.getFieldIndex(
//            cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.Constants.KEY_FIELD_NAME),
//        key);
//    while (value.iterate()) {
//      record.add(schema.getFieldIndex(value.key()), value.value());
//    }
//    record.sort();
//    return record;
//  }
//
//  private static MessageType getMessageType(Map<String, DataType> schema) {
//    List<Type> fields = new ArrayList<>();
//    fields.add(
//        Storer.getParquetType(Constants.KEY_FIELD_NAME, DataType.LONG, Type.Repetition.REQUIRED));
//    for (Map.Entry<String, DataType> entry : schema.entrySet()) {
//      String name = entry.getKey();
//      DataType type = entry.getValue();
//      fields.add(Storer.getParquetType(name, type, Type.Repetition.OPTIONAL));
//    }
//    MessageType parquetSchema = new MessageType(Constants.RECORD_FIELD_NAME, fields);
//    return parquetSchema;
//  }
//
//  @Override
//  protected ParquetTableMeta readMeta(Path path) throws IOException {
//    try (IParquetReader reader = IParquetReader.builder(path).build()) {
//      ParquetMetadata meta = reader.getMeta();
//      return ParquetTableMeta.of(meta);
//    } catch (Exception e) {
//      throw new StorageRuntimeException(e);
//    }
//  }
//
//  @Override
//  protected Scanner<Long, Scanner<String, Object>> scanFile(
//      Path path, ParquetTableMeta meta, Set<String> fields, Filter filter) throws IOException {
//    IParquetReader.Builder builder = IParquetReader.builder(path);
//    builder.project(fields);
//    builder.filter(filter);
//
//    IParquetReader reader = builder.build(meta.getMeta());
//
//    Scanner<Long, Scanner<String, Object>> scanner = new ParquetScanner(reader);
//    return scanner;
//  }
//
//  private static class ParquetScanner implements Scanner<Long, Scanner<String, Object>> {
//    private final IParquetReader reader;
//    private Long key;
//    private Scanner<String, Object> rowScanner;
//
//    public ParquetScanner(IParquetReader reader) {
//      this.reader = reader;
//    }
//
//    @Override
//    public Long key() throws NoSuchElementException {
//      if (key == null) {
//        throw new NoSuchElementException();
//      }
//      return key;
//    }
//
//    @Override
//    public Scanner<String, Object> value() throws NoSuchElementException {
//      if (rowScanner == null) {
//        throw new NoSuchElementException();
//      }
//      return rowScanner;
//    }
//
//    @Override
//    public boolean iterate() throws StorageException {
//      try {
//        IRecord record = reader.read();
//        if (record == null) {
//          key = null;
//          rowScanner = null;
//          return false;
//        }
//        Map<String, Object> map = new HashMap<>();
//        MessageType parquetSchema = reader.getSchema();
//        int keyIndex = parquetSchema.getFieldIndex(Constants.KEY_FIELD_NAME);
//        for (Map.Entry<Integer, Object> entry : record) {
//          int index = entry.getKey();
//          Object value = entry.getValue();
//          if (index == keyIndex) {
//            key = (Long) value;
//          } else {
//            map.put(parquetSchema.getType(index).getName(), value);
//          }
//        }
//        rowScanner = new IteratorScanner<>(map.entrySet().iterator());
//        return true;
//      } catch (Exception e) {
//        throw new StorageException("failed to read", e);
//      }
//    }
//
//    @Override
//    public void close() throws StorageException {
//      try {
//        reader.close();
//      } catch (Exception e) {
//        throw new StorageException("failed to close", e);
//      }
//    }
//  }
//
//  protected static class ParquetTableMeta implements
// ImmutableFileStorageManager.CacheableTableMeta {
//    private final Map<String, DataType> schemaDst;
//    private final Map<String, Range<Long>> rangeMap;
//    private final Map<String, Long> countMap;
//    private final ParquetMetadata meta;
//
//    public static ParquetTableMeta of(ParquetMetadata meta) {
//      Map<String, DataType> schemaDst = new HashMap<>();
//      Map<String, Range<Long>> rangeMap = new HashMap<>();
//      Map<String, Long> countMap = new HashMap<>();
//      MessageType parquetSchema = meta.getFileMetaData().getSchema();
//
//      Range<Long> ranges = IParquetReader.getRangeOf(meta);
//
//      for (int i = 0; i < parquetSchema.getFieldCount(); i++) {
//        Type type = parquetSchema.getType(i);
//        if (type.getName().equals(Constants.KEY_FIELD_NAME)) {
//          continue;
//        }
//        DataType iginxType = IParquetReader.toIginxType(type.asPrimitiveType());
//        schemaDst.put(type.getName(), iginxType);
//        rangeMap.put(type.getName(), ranges);
//      }
//
//      Map<ColumnPath, Long> columnPathMap = IParquetReader.getCountsOf(meta);
//      columnPathMap.forEach(
//          (columnPath, count) -> {
//            String[] columnPathArray = columnPath.toArray();
//            if (columnPathArray.length != 1) {
//              throw new IllegalStateException("invalid column path: " + columnPath);
//            }
//            String name = columnPath.toArray()[0];
//            countMap.put(name, count);
//          });
//
//      return new ParquetTableMeta(schemaDst, rangeMap, countMap, meta);
//    }
//
//    ParquetTableMeta(
//        Map<String, DataType> schemaDst,
//        Map<String, Range<Long>> rangeMap,
//        Map<String, Long> countMap,
//        ParquetMetadata meta) {
//      this.schemaDst = schemaDst;
//      this.rangeMap = rangeMap;
//      this.countMap = countMap;
//      this.meta = meta;
//    }
//
//    @Override
//    public Map<String, DataType> getSchema() {
//      return schemaDst;
//    }
//
//    @Override
//    public Range<Long> getRange(String field) {
//      if (!schemaDst.containsKey(field)) {
//        throw new NoSuchElementException();
//      }
//      return rangeMap.get(field);
//    }
//
//    @Nullable
//    @Override
//    public Long getValueCount(String field) {
//      if (!schemaDst.containsKey(field)) {
//        throw new NoSuchElementException();
//      }
//      return countMap.get(field);
//    }
//
//    public ParquetMetadata getMeta() {
//      return meta;
//    }
//  }
// }

//  private void cleanTempFiles() {
//    try (DirectoryStream<Path> stream =
//             Files.newDirectoryStream(dir, path -> path.endsWith(tempFileSuffixName))) {
//      for (Path path : stream) {
//        LOGGER.info("remove temp file {}", path);
//        Files.deleteIfExists(path);
//      }
//    } catch (NoSuchFileException ignored) {
//      LOGGER.debug("no dir named {}", dir);
//    } catch (IOException e) {
//      LOGGER.error("failed to clean temp files", e);
//    }
//  }
//
//  @Override
//  public String getName() {
//
//  }
//
//
//  public void flush(
//      String tableName, TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner)
//      throws IOException {
//    Path path = getPath(tableName);
//    Path tempPath = dir.resolve(tableName + tempFileSuffixName);
//    Files.createDirectories(path.getParent());
//
//    LOGGER.debug("flushing into {}", tempPath);
//
//    long startTime = System.currentTimeMillis();
//    CACHED cachedMeta = flush(meta, scanner, tempPath);
//    long endTime = System.currentTimeMillis();
//    LOGGER.info("flush {} into {} costs {} ms", tableName, tempPath, endTime - startTime);
//    setParquetTableMeta(path.toString(), cachedMeta);
//
//    LOGGER.debug("rename temp file to {}", path);
//    if (Files.exists(path)) {
//      LOGGER.warn("file {} already exists, will be replaced", path);
//    }
//    Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING);
//  }
//
//  protected interface CacheableTableMeta extends TableMeta, CachePool.Cacheable {
//  }
//
//  protected abstract CACHED readMeta(Path path) throws IOException;
//
//  @Override
//  public TableMeta readMeta(String tableName) {
//    Path path = getPath(tableName);
//    CacheableTableMeta tableMeta = getCachedTableMeta(path.toString());
//    AreaSet<Long, String> tombstone = tombstoneStorage.get(tableName);
//    if (tombstone == null || tombstone.isEmpty()) {
//      return tableMeta;
//    }
//    return new DeletedTableMeta(tableMeta, tombstone);
//  }
//
//  @SuppressWarnings("unchecked")
//  private CACHED getCachedTableMeta(String fileName) {
//    Path path = Paths.get(fileName);
//    CachePool.Cacheable cacheable =
//        shared
//            .getCachePool()
//            .asMap()
//            .computeIfAbsent(
//                fileName,
//                f -> {
//                  try {
//                    return this.readMeta(path);
//                  } catch (Exception e) {
//                    throw new StorageRuntimeException(e);
//                  }
//                });
//    return (CACHED) cacheable;
//  }
//
//  private void setParquetTableMeta(String fileName, CACHED tableMeta) {
//    shared.getCachePool().asMap().put(fileName, tableMeta);
//  }
//
//  protected abstract Scanner<Long, Scanner<String, Object>> scanFile(
//      Path path, CACHED meta, Set<String> fields, Filter filter) throws IOException;
//
//  @Override
//  public Scanner<Long, Scanner<String, Object>> scanData(
//      String name, Set<String> fields, RangeSet<Long> ranges, Filter predicate) throws IOException
// {
//    Path path = getPath(name);
//
//    Filter rangeFilter = FilterRangeUtils.filterOf(ranges);
//
//    Filter unionFilter;
//    if (predicate == null) {
//      unionFilter = rangeFilter;
//    } else {
//      unionFilter = new AndFilter(Arrays.asList(rangeFilter, predicate));
//    }
//
//    CACHED parquetTableMeta = getCachedTableMeta(path.toString());
//
//    Scanner<Long, Scanner<String, Object>> scanner =
//        scanFile(path, parquetTableMeta, fields, unionFilter);
//
//    AreaSet<Long, String> tombstone = tombstoneStorage.get(name);
//    if (tombstone == null || tombstone.isEmpty()) {
//      return scanner;
//    }
//    return new AreaFilterScanner<>(scanner, tombstone);
//  }
//
//  @Override
//  public void delete(String name, AreaSet<Long, String> areas) throws IOException {
//    tombstoneStorage.delete(Collections.singleton(name), oldAreas -> oldAreas.addAll(areas));
//  }
//
//  @Override
//  public void delete(String name) {
//    Path path = getPath(name);
//    try {
//      Files.deleteIfExists(path);
//      shared.getCachePool().asMap().remove(path.toString());
//      tombstoneStorage.removeTable(name);
//    } catch (IOException e) {
//      throw new StorageRuntimeException(e);
//    }
//  }
//
//  private Path getPath(String name) {
//    Path path = dir.resolve(name + this.fileSuffixName);
//    return path;
//  }
//
//  private String getTableName(String fileName) {
//    return fileName.substring(0, fileName.length() - this.fileSuffixName.length());
//  }
