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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.storage;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.lsm.table.DeletedTableMeta;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.iterator.AreaFilterScanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.manager.data.TombstoneStorage;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.manager.utils.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.CachePool;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Constants;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.Shared;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.StorageRuntimeException;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class FileStorageManager<META extends FileStorageManager.CacheableTableMeta>
    implements StorageManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageManager.class);

  protected final Shared shared;

  private final Path dir;

  private final TombstoneStorage tombstoneStorage;

  private final String fileSuffixName;

  private final String tempFileSuffixName = Constants.SUFFIX_FILE_TEMP;

  public FileStorageManager(Shared shared, Path dir, String fileSuffixName) {
    assert !Objects.equals(fileSuffixName, Constants.DIR_NAME_TOMBSTONE);
    this.shared = shared;
    this.dir = dir;
    this.tombstoneStorage = new TombstoneStorage(shared, dir.resolve(Constants.DIR_NAME_TOMBSTONE));
    this.fileSuffixName = "." + fileSuffixName;
    cleanTempFiles();
  }

  private void cleanTempFiles() {
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(dir, path -> path.endsWith(tempFileSuffixName))) {
      for (Path path : stream) {
        LOGGER.info("remove temp file {}", path);
        Files.deleteIfExists(path);
      }
    } catch (NoSuchFileException ignored) {
      LOGGER.debug("no dir named {}", dir);
    } catch (IOException e) {
      LOGGER.error("failed to clean temp files", e);
    }
  }

  @Override
  public String getName() {
    return dir.toString();
  }

  protected abstract META flush(
      TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner, Path path) throws IOException;

  @Override
  public void flush(
      String tableName, TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner)
      throws IOException {
    Path path = getPath(tableName);
    Path tempPath = dir.resolve(tableName + tempFileSuffixName);
    Files.createDirectories(path.getParent());

    LOGGER.debug("flushing into {}", tempPath);

    long startTime = System.currentTimeMillis();
    META cachedMeta = flush(meta, scanner, tempPath);
    long endTime = System.currentTimeMillis();
    LOGGER.info("flush {} into {} costs {} ms", tableName, tempPath, endTime - startTime);
    setParquetTableMeta(path.toString(), cachedMeta);

    LOGGER.debug("rename temp file to {}", path);
    if (Files.exists(path)) {
      LOGGER.warn("file {} already exists, will be replaced", path);
    }
    Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING);
  }

  protected interface CacheableTableMeta extends TableMeta, CachePool.Cacheable {}

  protected abstract META readMeta(Path path) throws IOException;

  @Override
  public TableMeta readMeta(String tableName) {
    Path path = getPath(tableName);
    CacheableTableMeta tableMeta = getCachedTableMeta(path.toString());
    AreaSet<Long, String> tombstone = tombstoneStorage.get(tableName);
    if (tombstone == null || tombstone.isEmpty()) {
      return tableMeta;
    }
    return new DeletedTableMeta(tableMeta, tombstone);
  }

  @SuppressWarnings("unchecked")
  private META getCachedTableMeta(String fileName) {
    Path path = Paths.get(fileName);
    CachePool.Cacheable cacheable =
        shared
            .getCachePool()
            .asMap()
            .computeIfAbsent(
                fileName,
                f -> {
                  try {
                    return this.readMeta(path);
                  } catch (Exception e) {
                    throw new StorageRuntimeException(e);
                  }
                });
    return (META) cacheable;
  }

  private void setParquetTableMeta(String fileName, META tableMeta) {
    shared.getCachePool().asMap().put(fileName, tableMeta);
  }

  protected abstract Scanner<Long, Scanner<String, Object>> scanFile(
      Path path, META meta, Set<String> fields, Filter filter) throws IOException;

  @Override
  public Scanner<Long, Scanner<String, Object>> scanData(
      String name, Set<String> fields, RangeSet<Long> ranges, Filter predicate) throws IOException {
    Path path = getPath(name);

    Filter rangeFilter = FilterRangeUtils.filterOf(ranges);

    Filter unionFilter;
    if (predicate == null) {
      unionFilter = rangeFilter;
    } else {
      unionFilter = new AndFilter(Arrays.asList(rangeFilter, predicate));
    }

    META parquetTableMeta = getCachedTableMeta(path.toString());

    Scanner<Long, Scanner<String, Object>> scanner =
        scanFile(path, parquetTableMeta, fields, unionFilter);

    AreaSet<Long, String> tombstone = tombstoneStorage.get(name);
    if (tombstone == null || tombstone.isEmpty()) {
      return scanner;
    }
    return new AreaFilterScanner<>(scanner, tombstone);
  }

  @Override
  public void delete(String name, AreaSet<Long, String> areas) throws IOException {
    tombstoneStorage.delete(Collections.singleton(name), oldAreas -> oldAreas.addAll(areas));
  }

  @Override
  public void delete(String name) {
    Path path = getPath(name);
    try {
      Files.deleteIfExists(path);
      shared.getCachePool().asMap().remove(path.toString());
      tombstoneStorage.removeTable(name);
    } catch (IOException e) {
      throw new StorageRuntimeException(e);
    }
  }

  @Override
  public Iterable<String> reload() throws IOException {
    List<String> names = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*" + this.fileSuffixName)) {
      for (Path path : stream) {
        shared.getCachePool().asMap().remove(path.toString());
        String fileName = path.getFileName().toString();
        String tableName = getTableName(fileName);
        names.add(tableName);
      }
    } catch (NoSuchFileException ignored) {
      LOGGER.debug("dir {} not existed.", dir);
    }
    tombstoneStorage.reload();
    return names;
  }

  private Path getPath(String name) {
    Path path = dir.resolve(name + this.fileSuffixName);
    return path;
  }

  private String getTableName(String fileName) {
    return fileName.substring(0, fileName.length() - this.fileSuffixName.length());
  }

  @Override
  public void clear() throws IOException {
    LOGGER.info("clearing data of {}", dir);
    try {
      try (DirectoryStream<Path> stream =
          Files.newDirectoryStream(dir, "*" + this.fileSuffixName)) {
        for (Path path : stream) {
          Files.deleteIfExists(path);
          String fileName = path.toString();
          shared.getCachePool().asMap().remove(fileName);
        }
      }
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*" + tempFileSuffixName)) {
        for (Path path : stream) {
          Files.deleteIfExists(path);
        }
      }
      tombstoneStorage.clear();
      Files.deleteIfExists(dir);
    } catch (NoSuchFileException e) {
      LOGGER.trace("Not a directory to clear: {}", dir);
    } catch (DirectoryNotEmptyException e) {
      LOGGER.warn("directory not empty to clear: {}", dir);
    }
  }
}
