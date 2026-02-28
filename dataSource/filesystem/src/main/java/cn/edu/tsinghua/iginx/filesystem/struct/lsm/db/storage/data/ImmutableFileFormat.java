package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table.Table;

import java.io.IOException;
import java.nio.file.Path;

public abstract class ImmutableFileFormat {
  public abstract String getName();

  public abstract void flush(Path dst, Table table) throws IOException;

  public abstract Table read(Path src) throws IOException;

  //  private void cleanTempFiles() {
//    try (DirectoryStream<Path> stream =
//             Files.newDirectoryStream(dir, path -> path.endsWith(tempFileSuffixName))) {
//      for (Path path : stream) {
//        LOGGER.info("remove temp file {}", path);
//        Files.deleteIfExists(path);
//      }
//    } catch (NoSuchFileException ignored) {
//      LOGGER.debug("no dir named {}", dir);
//    } catch (IOException e) {
//      LOGGER.error("failed to clean temp files", e);
//    }
//  }
//
//  @Override
//  public String getName() {
//
//  }
//
//
//  public void flush(
//      String tableName, TableMeta meta, Scanner<Long, Scanner<String, Object>> scanner)
//      throws IOException {
//    Path path = getPath(tableName);
//    Path tempPath = dir.resolve(tableName + tempFileSuffixName);
//    Files.createDirectories(path.getParent());
//
//    LOGGER.debug("flushing into {}", tempPath);
//
//    long startTime = System.currentTimeMillis();
//    CACHED cachedMeta = flush(meta, scanner, tempPath);
//    long endTime = System.currentTimeMillis();
//    LOGGER.info("flush {} into {} costs {} ms", tableName, tempPath, endTime - startTime);
//    setParquetTableMeta(path.toString(), cachedMeta);
//
//    LOGGER.debug("rename temp file to {}", path);
//    if (Files.exists(path)) {
//      LOGGER.warn("file {} already exists, will be replaced", path);
//    }
//    Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING);
//  }
//
//  protected interface CacheableTableMeta extends TableMeta, CachePool.Cacheable {
//  }
//
//  protected abstract CACHED readMeta(Path path) throws IOException;
//
//  @Override
//  public TableMeta readMeta(String tableName) {
//    Path path = getPath(tableName);
//    CacheableTableMeta tableMeta = getCachedTableMeta(path.toString());
//    AreaSet<Long, String> tombstone = tombstoneStorage.get(tableName);
//    if (tombstone == null || tombstone.isEmpty()) {
//      return tableMeta;
//    }
//    return new DeletedTableMeta(tableMeta, tombstone);
//  }
//
//  @SuppressWarnings("unchecked")
//  private CACHED getCachedTableMeta(String fileName) {
//    Path path = Paths.get(fileName);
//    CachePool.Cacheable cacheable =
//        shared
//            .getCachePool()
//            .asMap()
//            .computeIfAbsent(
//                fileName,
//                f -> {
//                  try {
//                    return this.readMeta(path);
//                  } catch (Exception e) {
//                    throw new StorageRuntimeException(e);
//                  }
//                });
//    return (CACHED) cacheable;
//  }
//
//  private void setParquetTableMeta(String fileName, CACHED tableMeta) {
//    shared.getCachePool().asMap().put(fileName, tableMeta);
//  }
//
//  protected abstract Scanner<Long, Scanner<String, Object>> scanFile(
//      Path path, CACHED meta, Set<String> fields, Filter filter) throws IOException;
//
//  @Override
//  public Scanner<Long, Scanner<String, Object>> scanData(
//      String name, Set<String> fields, RangeSet<Long> ranges, Filter predicate) throws IOException {
//    Path path = getPath(name);
//
//    Filter rangeFilter = FilterRangeUtils.filterOf(ranges);
//
//    Filter unionFilter;
//    if (predicate == null) {
//      unionFilter = rangeFilter;
//    } else {
//      unionFilter = new AndFilter(Arrays.asList(rangeFilter, predicate));
//    }
//
//    CACHED parquetTableMeta = getCachedTableMeta(path.toString());
//
//    Scanner<Long, Scanner<String, Object>> scanner =
//        scanFile(path, parquetTableMeta, fields, unionFilter);
//
//    AreaSet<Long, String> tombstone = tombstoneStorage.get(name);
//    if (tombstone == null || tombstone.isEmpty()) {
//      return scanner;
//    }
//    return new AreaFilterScanner<>(scanner, tombstone);
//  }
//
//  @Override
//  public void delete(String name, AreaSet<Long, String> areas) throws IOException {
//    tombstoneStorage.delete(Collections.singleton(name), oldAreas -> oldAreas.addAll(areas));
//  }
//
//  @Override
//  public void delete(String name) {
//    Path path = getPath(name);
//    try {
//      Files.deleteIfExists(path);
//      shared.getCachePool().asMap().remove(path.toString());
//      tombstoneStorage.removeTable(name);
//    } catch (IOException e) {
//      throw new StorageRuntimeException(e);
//    }
//  }
//
//  private Path getPath(String name) {
//    Path path = dir.resolve(name + this.fileSuffixName);
//    return path;
//  }
//
//  private String getTableName(String fileName) {
//    return fileName.substring(0, fileName.length() - this.fileSuffixName.length());
//  }
}
