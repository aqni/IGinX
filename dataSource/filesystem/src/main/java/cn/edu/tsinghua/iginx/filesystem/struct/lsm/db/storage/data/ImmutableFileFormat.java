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

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.table.AbstractTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.table.Table;
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
