package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table.AbstractTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public abstract class ImmutableFileFormat {

  protected final String name;

  public ImmutableFileFormat(String name) {
    this.name = Objects.requireNonNull(name);
  }

  @Override
  public String toString() {
    return name;
  }

  public abstract void flush(Path dst, Table table) throws IOException;

  protected abstract List<Table.SubTable> readSubTables(Path src) throws IOException;

  public Table read(Path src) throws IOException {
    return new ImmutableFileFormatTable(src);
  }

  private class ImmutableFileFormatTable extends AbstractTable {

    private final Path path;

    private ImmutableFileFormatTable(Path path) {
      this.path = Objects.requireNonNull(path);
    }

    @Override
    public List<SubTable> getSubTables() throws IOException {
      return readSubTables(path);
    }
  }

}
