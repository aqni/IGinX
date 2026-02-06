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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.struct.FileManager;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.Database;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.OneTierDB;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.ParquetFileStorageManager;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.StorageManager;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Shared;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageException;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLsmManager implements FileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLsmManager.class);

  private final Shared shared;
  private final Path path;
  private final Database db;

  public FileLsmManager(Shared shared, Path path) throws IOException {
    this.shared = shared;
    this.path = path;
    StorageManager storageManager = new ParquetFileStorageManager(shared, path); // tpch
    //    StorageManager storageManager = new TsFileStorageManager(shared, path); // tsbs
    //    StorageManager storageManager = new ArrowFileStorageManager(shared, path); // tpch
    this.db = new OneTierDB(path.toString(), shared, storageManager);
  }

  @Override
  public DataBoundary getBoundary(@Nullable String prefix) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public RowStream query(DataTarget target, @Nullable AggregateType aggregate) throws IOException {
    if (aggregate != null) {
      throw new UnsupportedOperationException("aggregation is not supported");
    }
    try {
      Filter filter = target.getFilter();
      if (Filters.isTrue(filter)) {
        filter = new BoolFilter(true);
      }

      List<String> patterns = target.getPatterns();
      if (Patterns.isAll(patterns)) {
        patterns = Collections.singletonList("*");
      }

      TagFilter tagFilter = target.getTagFilter();

      if (Filters.isFalse(target.getFilter())) {
        List<Field> fields =
            db.schema(patterns, tagFilter).stream()
                .map(ArrowFields::toIginxField)
                .collect(Collectors.toList());
        Header header = new Header(Field.KEY, fields);
        return new Table(header, Collections.emptyList());
      }

      return db.query(patterns, tagFilter, filter);
    } catch (PhysicalException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(DataTarget target) throws IOException {
    try {
      RangeSet<Long> rangeSet = Filters.toRangeSet(target.getFilter());
      List<String> patterns = target.getPatterns();
      if (Patterns.isAll(patterns)) {
        patterns = Collections.singletonList("*");
      }
      db.delete(patterns, target.getTagFilter(), rangeSet);
    } catch (PhysicalException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void insert(DataView data) throws IOException {
    try {
      db.insert(data);
    } catch (StorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      db.close();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
