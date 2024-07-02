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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import java.io.IOException;
import javax.annotation.Nonnull;

public interface FileIndex {

  /**
   * estimate size of data index
   *
   * @return size of data index in bytes
   */
  long size();

  /**
   * detect row ranges
   *
   * @param filter filter
   * @return row ranges. key is start row offset, value is row number.
   * @throws IOException if an I/O error occurs
   */
  @Nonnull
  Scanner<Long, Long> detect(@Nonnull Filter filter) throws IOException;
}
