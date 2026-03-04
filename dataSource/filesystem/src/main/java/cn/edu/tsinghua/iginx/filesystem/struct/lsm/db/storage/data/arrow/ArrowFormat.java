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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.arrow;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.naive.NaiveOperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.row.BatchStreamToRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.DenseImmutableFileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.table.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ArrowFormat extends DenseImmutableFileFormat {

  private final Logger LOGGER = LoggerFactory.getLogger(ArrowFormat.class);
  private final ArrowConfig config;

  public ArrowFormat(Config config, CachePool cachePool) {
    super("arrow", cachePool);
    this.config = ArrowConfig.of(config);
  }

  @Override
  protected void flush(Path path, Table.SubTable subTable) throws IOException {
    Table.Meta meta = subTable.getMeta();
    List<Field> fields = new ArrayList<>(meta.getFieldStats().keySet());
    try (RowStream rowStream = subTable.scan(fields, new BoolFilter(true))) {
      RowStream toRead = NaiveOperatorMemoryExecutor.transformToTable(rowStream);
      long startTime = System.currentTimeMillis();
      try (BufferAllocator allocator = new RootAllocator();
           BatchStream batchStream =
               BatchStreams.wrap(allocator, toRead, BaseValueVector.INITIAL_VALUE_ALLOCATION);
           VectorSchemaRoot root =
               VectorSchemaRoot.create(batchStream.getSchema().raw(), allocator);
           WritableByteChannel channel =
               Files.newByteChannel(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
           ArrowFileWriter writer =
               new ArrowFileWriter(
                   root,
                   null,
                   channel,
                   null,
                   IpcOption.DEFAULT,
                   FastestCompressionFactory.INSTANCE,
                   config.getCodec(),
                   Optional.ofNullable(config.getCompressionLevel()))) {
        writer.start();
        while (batchStream.hasNext()) {
          try (Batch batch = batchStream.getNext()) {
            VectorSchemaRoots.transfer(root, batch.getData());
            writer.writeBatch();
          }
        }
        writer.end();
      }
      long endTime = System.currentTimeMillis();
      LOGGER.debug("write arrow file {} takes {} ms", path, (endTime - startTime));
    } catch (PhysicalException e) {
      throw new IOException(e);
    }
    flushMeta(getMetaPath(path), meta);
  }

  private Path getMetaPath(Path path) {
    return path.resolveSibling(path.getFileName().toString() + ".meta");
  }

  private void flushMeta(Path path, Table.Meta meta) throws IOException {
    List<Table.Statistic> stats = new ArrayList<>(meta.getFieldStats().values());
    try (ObjectOutputStream oos =
             new ObjectOutputStream(
                 Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
      oos.writeObject(stats);
    }
  }

  @Override
  protected Table.Meta loadMeta(Path src) throws IOException {
    List<Field> originFields;
    try (BufferAllocator allocator = new RootAllocator();
         SeekableByteChannel channel = Files.newByteChannel(src, StandardOpenOption.READ);
         ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
      originFields = reader.getVectorSchemaRoot().getSchema().getFields();
    }
    List<Field> fields =
        originFields.stream().filter(f -> !BatchSchema.KEY.equals(f)).collect(Collectors.toList());
    try (ObjectInputStream ois =
             new ObjectInputStream(Files.newInputStream(getMetaPath(src), StandardOpenOption.READ))) {
      @SuppressWarnings("unchecked")
      List<Table.Statistic> stats = (List<Table.Statistic>) ois.readObject();
      if (fields.size() != stats.size()) {
        throw new IOException(
            "Meta file for " + src + " is corrupted: field count does not match statistic count");
      }
      return new Table.Meta(
          IntStream.range(0, fields.size())
              .boxed()
              .collect(ImmutableMap.toImmutableMap(fields::get, stats::get)));
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to read meta file for " + src, e);
    }
  }

  @Override
  protected RowStream scan(Path src, List<Field> fields, Filter predicate) throws IOException {
    Header header;
    List<Row> rows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
         SeekableByteChannel channel = Files.newByteChannel(src, StandardOpenOption.READ);
         ArrowFileReader reader =
             new ArrowFileReader(channel, allocator, FastestCompressionFactory.INSTANCE)) {
      Schema schema =
          new Schema(Iterables.concat(Collections.singletonList(BatchSchema.KEY), fields));
      header = BatchStreamToRowStreamWrapper.getHeader(BatchSchema.of(schema));

      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        List<FieldVector> allVectors = root.getFieldVectors();

        BigIntVector keyVector = (BigIntVector) allVectors.get(0);
        FieldVector[] valueVectors = new FieldVector[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          Field field = fields.get(i);
          FieldVector vector = root.getVector(field);
          if (vector == null) {
            throw new IOException("Field " + field + " does not exist in file " + src);
          }
          valueVectors[i] = vector;
        }

        int rowCount = root.getRowCount();
        for (int i = 0; i < rowCount; i++) {
          long key = keyVector.get(i);
          Object[] values = new Object[valueVectors.length];
          for (int j = 0; j < valueVectors.length; j++) {
            values[j] = valueVectors[j].getObject(i);
          }
          rows.add(new Row(header, key, values));
        }
      }
    }

    RowStream result = new cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table(header, rows);
    if (!Filters.isTrue(predicate)) {
      result = new FilterRowStreamWrapper(result, predicate);
    }
    return result;
  }


}


