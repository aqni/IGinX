package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.arrow;

import com.github.luben.zstd.Zstd;
import org.apache.arrow.compression.ZstdCompressionCodec;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ArrowFormatTest {
  @Test
  public void testLz4() {
    BufferAllocator allocator = new RootAllocator();
    BigIntVector vector = new BigIntVector("test", allocator);
    for (int i = 0; i < 3970; i++) {
      vector.setSafe(i, i);
    }
    vector.setValueCount(3970);
    CompressionCodec codec = new FastestLz4CompressionCodec();
    ArrowBuf compressed = codec.compress(allocator, vector.getDataBuffer());
    ArrowBuf uncompressed = codec.decompress(allocator, compressed);
    for (int i = 0; i < 3970; i++) {
      assertEquals(vector.getDataBuffer().getLong(i * 8), uncompressed.getLong(i * 8));
    }
  }

  @Test
  public void testZstd() {
    BufferAllocator allocator = new RootAllocator();
    BigIntVector vector = new BigIntVector("test", allocator);
    for (int i = 0; i < 3970; i++) {
      vector.setSafe(i, i);
    }
    vector.setValueCount(3970);
    CompressionCodec codec = new ZstdCompressionCodec(Zstd.maxCompressionLevel());
    ArrowBuf compressed = codec.compress(allocator, vector.getDataBuffer());
    ArrowBuf uncompressed = codec.decompress(allocator, compressed);
    for (int i = 0; i < 3970; i++) {
      assertEquals(vector.getDataBuffer().getLong(i * 8), uncompressed.getLong(i * 8));
    }
  }
}