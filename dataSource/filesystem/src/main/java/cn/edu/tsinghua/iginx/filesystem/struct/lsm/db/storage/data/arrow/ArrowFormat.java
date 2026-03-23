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
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.Indexer;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.DenseImmutableFileFormat;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CachePool;
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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ArrowFormat extends DenseImmutableFileFormat {

  private final ArrowConfig arrowConfig;

  public ArrowFormat(Config config, CachePool cachePool, Indexer indexer) {
    super("arrow", ArrowConfig.of(config), cachePool);
    this.arrowConfig = (ArrowConfig) this.config;
  }

  @Override
  protected void flush(Path dst, Table.SubTable subTable) throws IOException, PhysicalException {
    try (RowStream rowStream = scanAll(subTable)) {
      try (BufferAllocator allocator = new RootAllocator();
           BatchStream batchStream =
               BatchStreams.wrap(allocator, rowStream, BaseValueVector.INITIAL_VALUE_ALLOCATION);
           VectorSchemaRoot root =
               VectorSchemaRoot.create(batchStream.getSchema().raw(), allocator);
           WritableByteChannel channel =
               Files.newByteChannel(dst, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
           ArrowFileWriter writer =
               new ArrowFileWriter(
                   root,
                   null,
                   channel,
                   null,
                   IpcOption.DEFAULT,
                   FastestCompressionFactory.INSTANCE,
                   arrowConfig.getCompression(),
                   Optional.ofNullable(arrowConfig.getCompressionLevel()))) {
        writer.start();
        while (batchStream.hasNext()) {
          try (Batch batch = batchStream.getNext()) {
            VectorSchemaRoots.transfer(root, batch.getData());
            writer.writeBatch();
          }
        }
        writer.end();
      }
    }
    flushMeta(getMetaPath(dst), subTable.getMeta());
  }

  private Path getMetaPath(Path path) {
    return path.resolveSibling(path.getFileName().toString() + ".meta");
  }

  private void flushMeta(Path path, Table.Meta meta) throws IOException {
    try (ObjectOutputStream oos =
             new ObjectOutputStream(
                 Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
      oos.writeObject(meta);
    }
  }

  @Override
  protected Table.Meta loadMeta(Path src) throws IOException {
    try (ObjectInputStream ois =
             new ObjectInputStream(Files.newInputStream(getMetaPath(src), StandardOpenOption.READ))) {
      return (Table.Meta) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to read meta file for " + src, e);
    }
  }

  @Override
  protected RowStream scan(Path src, List<Field> fields, Filter predicate) throws IOException {
    Header header = new Header(Field.KEY, fields);
    BatchSchema.Builder schemaBuilder = BatchSchema.builder();
    for (Field field : header.getFields()) {
      schemaBuilder.addField(field.getName(), field.getType(), field.getTags());
    }
    List<org.apache.arrow.vector.types.pojo.Field> arrowFields = schemaBuilder.build().raw().getFields();

    List<Row> rows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
         SeekableByteChannel channel = Files.newByteChannel(src, StandardOpenOption.READ);
         ArrowFileReader reader =
             new ArrowFileReader(channel, allocator, FastestCompressionFactory.INSTANCE)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        BigIntVector keyVector = (BigIntVector) root.getVector(BatchSchema.KEY);
        FieldVector[] valueVectors = new FieldVector[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          org.apache.arrow.vector.types.pojo.Field field = arrowFields.get(i);
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


