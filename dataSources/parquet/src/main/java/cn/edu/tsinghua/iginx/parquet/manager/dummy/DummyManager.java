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
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.io.parquet.IParquetReader;
import cn.edu.tsinghua.iginx.parquet.io.parquet.ParquetRecordIterator;
import cn.edu.tsinghua.iginx.parquet.io.parquet.ParquetSchema;
import cn.edu.tsinghua.iginx.parquet.manager.Manager;
import cn.edu.tsinghua.iginx.parquet.manager.util.FilterRangeUtils;
import cn.edu.tsinghua.iginx.parquet.manager.util.RangeUtils;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.parquet.util.exception.UnsupportedTypeException;
import cn.edu.tsinghua.iginx.parquet.util.record.MergedRecordIterator;
import cn.edu.tsinghua.iginx.parquet.util.record.RecordsRowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static cn.edu.tsinghua.iginx.parquet.util.Constants.SUFFIX_FILE_PARQUET;

public class DummyManager implements Manager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DummyManager.class);

  private final Path dir;

  private final List<String> prefix;

  public DummyManager(Path dummyDir, String prefix) {
    this.dir = dummyDir;
    this.prefix = Collections.singletonList(prefix);
  }

  @Override
  public RowStream project(List<String> paths, TagFilter tagFilter, Filter filter)
      throws PhysicalException {
    RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(filter);
    LOGGER.debug("push down filter: {}", rangeSet);

    List<ParquetRecordIterator> iterators;
    List<ParquetRecordIterator> reversedIterators = new ArrayList<>();
    try {
      Map<String, DataType> declaredTypes = new HashMap<>();

      List<Path> filePaths = getFilePaths();
      filePaths.sort(Comparator.reverseOrder());
      for (Path path : filePaths) {
        LOGGER.debug("read file: {}", path);
        IParquetReader reader = null;
        try {
          reader = readFile(path, prefix, paths, tagFilter, rangeSet, declaredTypes);
          ParquetRecordIterator iterator = new ParquetRecordIterator(reader, prefix);
          reversedIterators.add(iterator);
          reader = null;
        } catch (IOException e) {
          throw new StorageRuntimeException("failed to get reader of " + path, e);
        } finally {
          if (reader != null) {
            try {
              reader.close();
            } catch (Exception e) {
              LOGGER.warn("failed to close reader: {}", reader, e);
            }
          }
        }
      }

      iterators = new ArrayList<>(reversedIterators);
      reversedIterators.clear();
      Collections.reverse(iterators);
    } finally {
      for (ParquetRecordIterator iterator : reversedIterators) {
        try {
          iterator.close();
        } catch (Exception ex) {
          LOGGER.warn("failed to close iterator: {}", iterator, ex);
        }
      }
    }

    MergedRecordIterator mergedRecordIterator = new MergedRecordIterator(iterators);
    return new RecordsRowStream(mergedRecordIterator);
  }

  @Nonnull
  private static IParquetReader readFile(
      Path path,
      List<String> prefix,
      List<String> patterns,
      TagFilter tagFilter,
      RangeSet<Long> ranges,
      Map<String, DataType> declaredTypes) throws IOException {

    AtomicBoolean isIginxData = new AtomicBoolean(false);
    IParquetReader.Builder builder = IParquetReader.builder(path);
    builder.withSchemaConverter((messageType, extra) -> {
      ParquetSchema parquetSchema;
      try {
        parquetSchema = new ParquetSchema(messageType, extra, prefix);
      } catch (UnsupportedTypeException e) {
        throw new StorageRuntimeException(e);
      }

      isIginxData.set(parquetSchema.getKeyIndex() != null);
      List<Pair<String, DataType>> rawHeader = parquetSchema.getRawHeader();
      List<String> rawNames = rawHeader.stream().map(Pair::getK).collect(Collectors.toList());
      List<DataType> rawTypes = rawHeader.stream().map(Pair::getV).collect(Collectors.toList());
      List<Field> header = parquetSchema.getHeader();

      Set<String> projectedPath = new HashSet<>();
      int size = rawNames.size();
      for (int i = 0; i < size; i++) {
        if (!rawTypes.get(i).equals(declaredTypes.get(header.get(i).getFullName()))) {
          continue;
        }

        Field field = header.get(i);
        if (parquetSchema.getKeyIndex() != null && tagFilter != null) {
          if (!TagKVUtils.match(field.getTags(), tagFilter)) {
            continue;
          }
        }

        if (!patterns.stream().anyMatch(s -> StringUtils.match(field.getName(), s))) {
          continue;
        }

        projectedPath.add(rawNames.get(i));
      }

      if (isIginxData.get()) {
        projectedPath.add(Constants.KEY_FIELD_NAME);
      }

      return IParquetReader.project(messageType, projectedPath);
    });

    if (isIginxData.get()) {
      builder.filter(FilterRangeUtils.filterOf(ranges));
    } else {
      if (!ranges.isEmpty()) {
        builder.range(0L, 0L);
      }
      KeyInterval keyInterval = RangeUtils.toKeyInterval(ranges.span());
      builder.range(keyInterval.getStartKey(), keyInterval.getEndKey());
    }

    return builder.build();
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
    Set<Field> allSchema = new HashSet<>();

    List<Path> filePaths = getFilePaths();
    filePaths.sort(Comparator.naturalOrder());
    for (Path path : filePaths) {
      IParquetReader.Builder builder = IParquetReader.builder(path);
      try (IParquetReader reader = builder.build()) {
        ParquetRecordIterator iterator = new ParquetRecordIterator(reader, prefix);
        List<Field> fields = iterator.header();
        allSchema.addAll(fields);
      } catch (Exception e) {
        throw new PhysicalRuntimeException("failed to load schema from " + path, e);
      }
    }

    List<Column> columns = new ArrayList<>();
    for (Field field : allSchema) {
      columns.add(new Column(field.getName(), field.getType(), field.getTags(), true));
    }
    return columns;
  }

  @Override
  public KeyInterval getKeyInterval() throws PhysicalException {
    TreeRangeSet<Long> rangeSet = TreeRangeSet.create();

    for (Path path : getFilePaths()) {
      IParquetReader.Builder builder = IParquetReader.builder(path);
      try (IParquetReader reader = builder.build()) {
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
