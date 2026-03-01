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

import java.io.IOException;
import java.nio.file.Path;

public class TsfileFormat extends ImmutableFileFormat {

  public TsfileFormat() {
    super("tsfile");
  }

  @Override
  public void flush(Path dst, Table table) throws IOException {

  }

  @Override
  public Table read(Path src) throws IOException {
    return null;
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
