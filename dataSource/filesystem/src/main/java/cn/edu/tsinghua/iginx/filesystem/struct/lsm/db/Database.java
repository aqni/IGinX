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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageException;
import com.google.common.collect.RangeSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.arrow.vector.types.pojo.Field;

public interface Database extends AutoCloseable {

  RowStream query(List<String> pattern, @Nullable TagFilter tagFilter, Filter filter)
      throws StorageException;

  List<Field> schema(List<String> patterns, @Nullable TagFilter tagFilter) throws StorageException;

  void insert(DataView data) throws StorageException, InterruptedException;

  void delete(List<String> patterns, @Nullable TagFilter tagFilter, RangeSet<Long> ranges)
      throws StorageException, InterruptedException;
}
