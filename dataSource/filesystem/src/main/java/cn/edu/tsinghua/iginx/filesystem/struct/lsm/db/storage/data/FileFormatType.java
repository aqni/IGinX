package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data;

import java.util.Objects;
import java.util.function.Supplier;

public enum FileFormatType {
  PARQUET(ParquetFormat::new),
  TSFILE(TsfileFormat::new),
  ARROW(ArrowFormat::new);

  private final Supplier<ImmutableFileFormat> factory;

  FileFormatType(Supplier<ImmutableFileFormat> factory) {
    this.factory = Objects.requireNonNull(factory);
  }

  public ImmutableFileFormat create() {
    return factory.get();
  }
}
