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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.DataBuffer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.StorageManager;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table.FileTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table.MemoryTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.scanner.Scanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Shared;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.TypeConflictedException;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

public class TableStorage implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStorage.class);

  private final Catalog catalog;
  private final StorageManager storageManager;
  private long sqnBase;

  public TableStorage(Shared shared, Catalog index, StorageManager storageManager)
      throws IOException {
    this.catalog = index;
    this.storageManager = storageManager;

    Iterable<String> tableNames = storageManager.reload();
    String last =
        StreamSupport.stream(tableNames.spliterator(), false)
            .max(Comparator.naturalOrder())
            .orElse("0-0");
    this.sqnBase = getSeq(last) + 1;

    try {
      for (String tableName : tableNames) {
        LOGGER.debug("rebuilt table index for table: {}", tableName);
        StorageManager.TableMeta meta = getMeta(tableName);
        catalog.addTable(tableName, meta);
      }
    } catch (IOException | TypeConflictedException e) {
      throw new StorageRuntimeException(e);
    }
  }

  static long getSeq(String tableName) {
    Pattern pattern = Pattern.compile("^(\\d+)-.*$");
    Matcher matcher = pattern.matcher(tableName);
    if (matcher.find()) {
      return Long.parseLong(matcher.group(1));
    } else {
      throw new IllegalArgumentException("invalid table name: " + tableName);
    }
  }

  public String flush(long sqn, MemoryTable table) throws IOException, StorageException {
    String name = String.format("%019d", sqnBase + sqn);
    StorageManager.TableMeta meta = table.getMeta();
    try (Scanner<Long, Scanner<String, Object>> scanner =
             table.scan(meta.getSchema().keySet(), ImmutableRangeSet.of(Range.all()))) {
      storageManager.flush(name, meta, scanner);
    }
    return name;
  }

  public void commit(String table, boolean toRemove) {
    try {
      if (toRemove) {
        storageManager.delete(table);
        return;
      }
      StorageManager.TableMeta meta = storageManager.readMeta(table);
      catalog.addTable(table, meta);
    } catch (IOException | TypeConflictedException e) {
      LOGGER.error("commit table {} failed", table, e);
    }
  }

  public void clear() {
    sqnBase = 0;
    try {
      storageManager.clear();
    } catch (IOException e) {
      LOGGER.error("clear failed", e);
    }
  }

  public StorageManager.TableMeta getMeta(String tableName) throws IOException {
    return new FileTable(tableName, storageManager).getMeta();
  }

  public void delete(AreaSet<Long, String> areas) throws IOException {
    Set<String> tables = catalog.find(areas);
    for (String tableName : tables) {
      storageManager.delete(tableName, areas);
    }
  }

  @Override
  public void close() {
  }

  public DataBuffer<Long, String, Object> query(
      Set<String> fields, RangeSet<Long> ranges, Filter filter)
      throws StorageException, IOException {

    AreaSet<Long, String> areas = new AreaSet<>();
    areas.add(fields, ranges);
    DataBuffer<Long, String, Object> buffer = new DataBuffer<>();

    Set<String> tables = catalog.find(areas);
    List<String> sortedTableNames = new ArrayList<>(tables);
    sortedTableNames.sort(Comparator.naturalOrder());

    for (String tableName : sortedTableNames) {
      try (Scanner<Long, Scanner<String, Object>> scanner = scan(tableName, fields, ranges)) {
        buffer.putRows(scanner);
      }
    }

    return buffer;
  }

  private Scanner<Long, Scanner<String, Object>> scan(
      String tableName, Set<String> fields, RangeSet<Long> ranges) throws IOException {
    return new FileTable(tableName, storageManager).scan(fields, ranges);
  }
}
