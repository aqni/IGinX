package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet.ParquetFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.typesafe.config.Config;

import java.util.Objects;
import java.util.function.BiFunction;

public enum FileFormatType {
  PARQUET(ParquetFormat::new),
  TSFILE(TsfileFormat::new),
  ARROW(ArrowFormat::new);

  private final BiFunction<Config, CachePool, ImmutableFileFormat> factory;

  FileFormatType(BiFunction<Config, CachePool, ImmutableFileFormat> factory) {
    this.factory = Objects.requireNonNull(factory);
  }

  public ImmutableFileFormat create(Config config, CachePool cachePool) {
    return factory.apply(config, cachePool);
  }
}
