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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.DenseImmutableFileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeSet;
import com.typesafe.config.Config;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.*;
import org.apache.paimon.format.parquet.ParquetFileFormat;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.shade.org.apache.parquet.column.statistics.Statistics;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

// ref: paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java

public class ParquetFormat extends DenseImmutableFileFormat {

  private final ParquetConfig config;
  private final ParquetFileFormat format;

  public ParquetFormat(Config fileConfig, CachePool cachePool) {
    super("parquet", cachePool);
    this.config = ParquetConfig.of(fileConfig);
    this.format = new ParquetFileFormat(new FileFormatFactory.FormatContext(
        new Options(),
        config.readBatchSize,
        config.writeBatchSize,
        MemorySize.ofBytes(config.writeBatchMemory.toBytes()),
        config.getZstdLevel(),
        null
    ));
  }

  @Override
  protected void flush(Path dst, Table.SubTable subTable) throws IOException, PhysicalException {
    org.apache.paimon.fs.Path paimonDst = new org.apache.paimon.fs.Path(dst.toUri());
    try (RowStream rowStream = scanAll(subTable)) {
      RowType schema = TypeUtils.toRowType(rowStream.getHeader());
      FormatWriterFactory writerFactory = format.createWriterFactory(schema);
      try (LocalFileIO localFileIO = LocalFileIO.create();
           PositionOutputStream outputStream = localFileIO.newOutputStream(paimonDst, false);
           FormatWriter writer = writerFactory.create(outputStream, config.getCompression().name())) {
        while (rowStream.hasNext()) {
          Row row = rowStream.next();
          InternalRow paimonRow = TypeUtils.toInternalRow(row);
          writer.addElement(paimonRow);
        }
      }
    }
  }

  @Override
  protected Table.Meta loadMeta(Path src) throws IOException {
    org.apache.paimon.fs.Path paimonDst = new org.apache.paimon.fs.Path(src.toUri());
    try (LocalFileIO localFileIO = LocalFileIO.create()) {
      FileStatus fileStatus = localFileIO.getFileStatus(paimonDst);
      Pair<Map<String, Statistics<?>>, SimpleStatsExtractor.FileInfo> stats =
          ParquetUtil.extractColumnStats(localFileIO, paimonDst, fileStatus.getLen());
      ImmutableMap<Field, Table.Statistic> statisticMap = TypeUtils.toStatisticMap(stats.getLeft());
      return new Table.Meta(statisticMap);
    }
  }

  @Override
  protected RowStream scan(Path src, List<Field> fields, Filter predicate) throws IOException {
    org.apache.paimon.fs.Path paimonSrc = new org.apache.paimon.fs.Path(src.toUri());
    Header header = new Header(Field.KEY, fields);
    RowType projectedSchema = TypeUtils.toRowType(header);
    InternalRow.FieldGetter[] fieldGetters = IntStream.range(0, projectedSchema.getFieldCount())
        .mapToObj(i -> InternalRow.createFieldGetter(projectedSchema.getTypeAt(i), i))
        .toArray(InternalRow.FieldGetter[]::new);

    RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(predicate);
    Predicate paimonPredicate = FilterUtils.toPaimonPredicate(rangeSet, projectedSchema);
    List<Predicate> predicates = null;
    if (paimonPredicate != null) {
      predicates = Collections.singletonList(paimonPredicate);
    }
    FormatReaderFactory readerFactory = format.createReaderFactory(null, projectedSchema, predicates);

    List<Row> rows = new ArrayList<>();
    try (LocalFileIO localFileIO = LocalFileIO.create()) {
      FileStatus fileStatus = localFileIO.getFileStatus(paimonSrc);
      FormatReaderContext context = new FormatReaderContext(
          localFileIO,
          paimonSrc,
          fileStatus.getLen(),
          null
      );
      try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context)) {
        while (true) {
          FileRecordIterator<InternalRow> paimonBatch = reader.readBatch();
          if (paimonBatch == null) {
            break;
          }
          while (true) {
            InternalRow paimonRow = paimonBatch.next();
            if (paimonRow == null) {
              break;
            }
            Row row = TypeUtils.toRow(paimonRow, header, fieldGetters);
            rows.add(row);
          }
        }
      }
    }

    RowStream rowStream = new cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table(header, rows);
    if (!Filters.isTrue(predicate)) {
      rowStream = new FilterRowStreamWrapper(rowStream, predicate);
    }
    return rowStream;
  }
}
