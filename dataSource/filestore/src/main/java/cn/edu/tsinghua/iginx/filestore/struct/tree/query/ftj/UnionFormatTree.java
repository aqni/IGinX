/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder.Factory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.Nullable;

public class UnionFormatTree implements Factory {

  private final Factory forRegularFile = new FormatQuerierBuilderFactory();
  private final Factory forDirectory = new UnionDirectoryQuerierBuilderFactory(this);

  @Override
  public Builder create(@Nullable String prefix, Path path, FileTreeConfig config)
      throws IOException {
    if (Files.isDirectory(path)) {
      return forDirectory.create(prefix, path, config);
    } else if (Files.isRegularFile(path)) {
      return forRegularFile.create(prefix, path, config);
    } else if (!Files.exists(path)) {
      throw new IOException("file does not exist: " + path);
    } else {
      throw new IllegalArgumentException("Unsupported file type: " + path);
    }
  }
}