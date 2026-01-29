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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.buffer.DataBuffer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.StorageManager;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.scanner.Scanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Shared;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.TypeConflictedException;
import com.google.common.collect.*;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableStorage implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStorage.class);

  private final TableIndex tableIndex;
  private final StorageManager storageManager;
  private long sqnBase;

  public TableStorage(Shared shared, TableIndex index, StorageManager storageManager)
      throws IOException {
    this.tableIndex = index;
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
        tableIndex.declareFields(meta.getSchema());
        tableIndex.addTable(tableName, meta);
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

  public String getTableName(long sqn, String suffix) {
    return String.format("%019d-%s", sqnBase + sqn, suffix);
  }

  public List<String> flush(long sqn, String suffix, MemoryTable table)
      throws InterruptedException {
    if (table.isEmpty()) {
      return Collections.emptyList();
    }
    String name = getTableName(sqn, suffix);
    StorageManager.TableMeta meta = table.getMeta();
    try (Scanner<Long, Scanner<String, Object>> scanner =
        table.scan(meta.getSchema().keySet(), ImmutableRangeSet.of(Range.all()))) {
      storageManager.flush(name, meta, scanner);
    } catch (IOException | StorageException e) {
      LOGGER.error("flush table {} failed", name, e);
    }
    return Collections.singletonList(name);
  }

  public void commit(String table, AreaSet<Long, Field> tombstone) {
    AreaSet<Long, String> innerTombstone = ArrowFields.toInnerAreas(tombstone);
    try {
      if (innerTombstone.isAll()) {
        storageManager.delete(table);
        return;
      }
      if (!innerTombstone.isEmpty()) {
        storageManager.delete(table, innerTombstone);
      }
      StorageManager.TableMeta meta = storageManager.readMeta(table);
      tableIndex.addTable(table, meta);
    } catch (IOException e) {
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
    Set<String> tables = tableIndex.find(areas);
    for (String tableName : tables) {
      storageManager.delete(tableName, areas);
    }
  }

  @Override
  public void close() {}

  public DataBuffer<Long, String, Object> query(
      Set<String> fields, RangeSet<Long> ranges, Filter filter)
      throws StorageException, IOException {

    AreaSet<Long, String> areas = new AreaSet<>();
    areas.add(fields, ranges);
    DataBuffer<Long, String, Object> buffer = new DataBuffer<>();

    Set<String> tables = tableIndex.find(areas);
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
