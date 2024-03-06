/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.manager.dummy;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalRuntimeException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetReader;
import cn.edu.tsinghua.iginx.parquet.io.parquet.ParquetRecordIterator;
import cn.edu.tsinghua.iginx.parquet.manager.Manager;
import cn.edu.tsinghua.iginx.parquet.manager.util.RangeUtils;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.parquet.util.record.RecordIterator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static cn.edu.tsinghua.iginx.parquet.util.Constants.SUFFIX_FILE_PARQUET;

public class DummyManager implements Manager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DummyManager.class);

  private final Path dir;

  private final String prefix;

  public DummyManager(Path dummyDir, String prefix) {
    this.dir = dummyDir;
    this.prefix = prefix;
  }

  @Override
  public RowStream project(List<String> paths, TagFilter tagFilter, Filter filter)
      throws PhysicalException {
    LOGGER.debug("project paths: {}", paths);

    Set<String> projectedPath = new HashSet<>();
    for (Path path : getFilePaths()) {
      Set<String> pathsInFile;
      try {
        pathsInFile =
            new Loader(path)
                .getHeader().stream()
                .map(Field::getName)
                .map(s -> prefix + "." + s)
                .collect(Collectors.toSet());
      } catch (IOException e) {
        throw new PhysicalException("failed to load schema from " + path + " : " + e);
      }
      LOGGER.debug("paths in {}: {}", path, pathsInFile);

      List<String> filePaths = determinePathList(pathsInFile, paths, tagFilter);
      filePaths.replaceAll(s -> s.substring(s.indexOf(".") + 1));
      if (!filePaths.isEmpty()) {
        // TODO: filter, project
        try {
          new Loader(path).load(table);
        } catch (IOException e) {
          throw new PhysicalException("failed to load data from " + path + " : " + e);
        }
      }
      projectedPath.addAll(filePaths);
    }
    List<cn.edu.tsinghua.iginx.parquet.manager.dummy.Column> columns =
        table.toColumns().stream()
            .filter(column -> projectedPath.contains(column.getPathName()))
            .collect(Collectors.toList());
    columns.forEach(
        column -> {
          column.setPathName(prefix + "." + column.getPathName());
          LOGGER.debug(
              "return column {}, records={}", column.getPathName(), column.getData().size());
        });
    return new NewQueryRowStream(columns);
  }

  private List<String> determinePathList(
      Set<String> paths, List<String> patterns, TagFilter tagFilter) {
    List<String> ret = new ArrayList<>();
    for (String path : paths) {
      for (String pattern : patterns) {
        Pair<String, Map<String, String>> pair = TagKVUtils.fromFullName(path);
        if (tagFilter == null) {
          if (StringUtils.match(pair.getK(), pattern)) {
            ret.add(path);
            break;
          }
        } else {
          if (StringUtils.match(pair.getK(), pattern)
              && cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils.match(
              pair.getV(), tagFilter)) {
            ret.add(path);
            break;
          }
        }
      }
    }
    return ret;
  }

  @Override
  public void insert(DataView dataView) throws PhysicalException {
    throw new StorageRuntimeException("DummyManager does not support insert");
  }

  @Override
  public void delete(List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter)
      throws PhysicalException {
    throw new StorageRuntimeException("DummyManager does not support delete");
  }

  @Override
  public List<Column> getColumns() throws PhysicalException {
    List<Column> columns = new ArrayList<>();
    List<Path> filePaths = getFilePaths();
    filePaths.sort(Comparator.naturalOrder());
    Map<String, DataType> allSchema = new HashMap<>();
    for (Path path : filePaths) {
      IParquetReader.Builder builder = IParquetReader.builder(path);
      try (IParquetReader reader = builder.build()) {
        ParquetRecordIterator iterator = new ParquetRecordIterator(reader);
        RecordIterator.Header header = iterator.getHeader();
        for (int i = 0; i < header.size(); i++) {
          String name = header.getName(i);
          DataType type = header.getType(i);
          allSchema.put(name, type);
        }
      } catch (Exception e) {
        throw new PhysicalRuntimeException("failed to load schema from " + path, e);
      }
    }

    // add prefix
    for (Map.Entry<String, DataType> entry : allSchema.entrySet()) {
      String withPrefix = prefix + "." + entry.getKey();
      columns.add(new Column(withPrefix, entry.getValue(), null, true));
    }
    return columns;
  }

  @Override
  public KeyInterval getKeyInterval() throws PhysicalException {
    TreeRangeSet<Long> rangeSet = TreeRangeSet.create();

    long offset = 0;
    for (Path path : getFilePaths()) {
      IParquetReader.Builder builder = IParquetReader.builder(path);
      try (IParquetReader reader = builder.build()) {
        if (reader.isIginxData()) {
          rangeSet.add(reader.getRange());
        } else {
          long nextOffset = offset + reader.getRowCount();
          rangeSet.add(Range.closedOpen(offset, nextOffset));
          offset = nextOffset;
        }
        rangeSet.add(reader.getRange());
      } catch (Exception e) {
        throw new PhysicalException("failed to get range from " + path + ": " + e, e);
      }
    }

    if (rangeSet.isEmpty()) {
      return new KeyInterval(0, 0);
    }
    Range<Long> span = rangeSet.span();
    return RangeUtils.toKeyInterval(span);
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("{} closed", this);
  }

  @Override
  public String toString() {
    return "DummyManager{" + "dummyDir=" + dir + '}';
  }

  private List<Path> getFilePaths() throws PhysicalException {
    try (Stream<Path> pathStream = Files.list(dir)) {
      return pathStream
          .filter(path -> path.toString().endsWith(SUFFIX_FILE_PARQUET))
          .filter(Files::isRegularFile)
          .collect(Collectors.toList());
    } catch (NoSuchFileException e) {
      LOGGER.warn("no parquet file in {}", dir);
      return Collections.emptyList();
    } catch (IOException e) {
      throw new PhysicalException("failed to list parquet file in " + dir + ": " + e, e);
    }
  }
}
