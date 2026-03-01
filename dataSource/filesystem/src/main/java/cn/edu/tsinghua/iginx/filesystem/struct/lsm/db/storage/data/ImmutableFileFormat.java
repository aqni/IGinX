package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public abstract class ImmutableFileFormat {

  private final String name;

  public ImmutableFileFormat(String name) {
    this.name = Objects.requireNonNull(name);
  }

  @Override
  public String toString() {
    return name;
  }

  public abstract void flush(Path dst, Table table) throws IOException;

  public abstract Table read(Path src) throws IOException;

}
