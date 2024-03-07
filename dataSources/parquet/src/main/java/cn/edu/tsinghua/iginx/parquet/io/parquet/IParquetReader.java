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

package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.format.parquet.ParquetReadOptions;
import cn.edu.tsinghua.iginx.format.parquet.ParquetRecordReader;
import cn.edu.tsinghua.iginx.parquet.util.Constants;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageRuntimeException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.common.collect.Range;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

public class IParquetReader implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IParquetReader.class);

  private final ParquetRecordReader<IRecord> internalReader;
  private final MessageType schema;
  private final ParquetMetadata metadata;

  private long count = 0;

  private IParquetReader(
      ParquetRecordReader<IRecord> internalReader, MessageType schema, ParquetMetadata metadata) {
    this.internalReader = internalReader;
    this.schema = schema;
    this.metadata = metadata;
  }

  public static Builder builder(Path path) {
    return new Builder(new LocalInputFile(path));
  }

  public boolean isIginxData() {
    return Constants.PARQUET_OBJECT_MODEL_NAME_VALUE.equals(metadata.getFileMetaData().getKeyValueMetaData().get(Constants.PARQUET_OBJECT_MODEL_NAME_PROP));
  }

  public MessageType getSchema() {
    return schema;
  }

  public IRecord read() throws IOException {
    if (internalReader == null) {
      return null;
    }
    if (!internalReader.nextKeyValue()) {
      return null;
    }
    count++;
    return internalReader.getCurrentValue();
  }

  @Override
  public void close() throws IOException {
    if (internalReader != null) {
      internalReader.close();
    }
    LOGGER.debug("read {} records", count);
  }

  public long getCurrentRowIndex() {
    return internalReader.getCurrentRowIndex();
  }

  public long getRowCount() {
    return metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
  }

  public Range<Long> getRange() {
    MessageType schema = metadata.getFileMetaData().getSchema();
    if (isIginxData()) {
      Type type = schema.getType(Constants.KEY_FIELD_NAME);
      if (type.isPrimitive()) {
        PrimitiveType primitiveType = type.asPrimitiveType();
        if (primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64) {
          return getKeyRange();
        }
      }
    }
    return Range.closedOpen(0L, getRowCount());
  }

  private Range<Long> getKeyRange() {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;

    for (BlockMetaData block : metadata.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        if (column.getPath().toDotString().equals(Constants.KEY_FIELD_NAME)) {
          LongStatistics statistics = (LongStatistics) column.getStatistics();
          min = Math.min(min, statistics.genericGetMin());
          max = Math.max(max, statistics.genericGetMax());
        }
      }
    }

    if (min == Long.MAX_VALUE || max == Long.MIN_VALUE) {
      return Range.closedOpen(0L, 0L);
    }

    return Range.closed(min, max);
  }

  public ParquetMetadata getMeta() {
    return metadata;
  }

  public static class Builder {

    private final ParquetReadOptions.Builder optionsBuilder = ParquetReadOptions.builder();
    private final InputFile localInputfile;
    private boolean skip = false;
    private Set<String> fields;
    private BiFunction<MessageType, Map<String, String>, MessageType> schemaConverter;

    public Builder(LocalInputFile localInputFile) {
      this.localInputfile = localInputFile;
    }

    public IParquetReader build() throws IOException {
      ParquetReadOptions options = optionsBuilder.build();
      ParquetMetadata footer;
      try (SeekableInputStream in = localInputfile.newStream()) {
        footer = ParquetFileReader.readFooter(localInputfile, options, in);
      }

      return build(footer, options);
    }

    public IParquetReader build(ParquetMetadata footer) throws IOException {
      return build(footer, optionsBuilder.build());
    }

    @Nonnull
    private IParquetReader build(ParquetMetadata footer, ParquetReadOptions options)
        throws IOException {
      MessageType schema = footer.getFileMetaData().getSchema();
      MessageType requestedSchema;
      if (fields == null) {
        requestedSchema = schema;
      } else {
        requestedSchema = ProjectUtils.projectMessageType(schema, fields);
        LOGGER.debug("project schema with {} as {}", fields, requestedSchema);
      }

      if (schemaConverter != null) {
        requestedSchema = schemaConverter.apply(requestedSchema, footer.getFileMetaData().getKeyValueMetaData());
      }

      if (skip) {
        return new IParquetReader(null, requestedSchema, footer);
      }

      ParquetFileReader reader = new ParquetFileReader(localInputfile, footer, options);
      reader.setRequestedSchema(requestedSchema);
      ParquetRecordReader<IRecord> internalReader =
          new ParquetRecordReader<>(new IRecordMaterializer(requestedSchema), reader, options);
      return new IParquetReader(internalReader, requestedSchema, footer);
    }

    public Builder project(Set<String> fields) {
      this.fields = Objects.requireNonNull(fields);
      return this;
    }

    public Builder withSchemaConverter(BiFunction<MessageType, Map<String, String>, MessageType> schemaConverter) {
      this.schemaConverter = schemaConverter;
      return this;
    }

    public Builder filter(Filter filter) {
      Pair<FilterPredicate, Boolean> filterPredicate =
          FilterUtils.toFilterPredicate(Objects.requireNonNull(filter));
      if (filterPredicate.k != null) {
        optionsBuilder.withRecordFilter(FilterCompat.get(filterPredicate.k));
      } else {
        skip = !filterPredicate.v;
      }
      return this;
    }

    public Builder withCodecFactory(int lz4BufferSize) {
      optionsBuilder.withCodecFactory(
          new CodecFactory(
              lz4BufferSize, CodecFactory.DEFAULT_ZSTD_LEVEL, CodecFactory.DEFAULT_ZSTD_WORKERS));
      return this;
    }

    public Builder range(long begin, long end) {
      optionsBuilder.withRange(begin, end);
      return this;
    }
  }

  public static DataType toIginxType(PrimitiveType type) {

    PrimitiveType primitiveType = type.asPrimitiveType();
    if (!primitiveType.getRepetition().equals(PrimitiveType.Repetition.REPEATED)) {
      switch (primitiveType.getPrimitiveTypeName()) {
        case BOOLEAN:
          return DataType.BOOLEAN;
        case INT32:
          return DataType.INTEGER;
        case INT64:
          return DataType.LONG;
        case FLOAT:
          return DataType.FLOAT;
        case DOUBLE:
          return DataType.DOUBLE;
        case BINARY:
          return DataType.BINARY;
      }
    }
    throw new StorageRuntimeException("unsupported parquet type: " + type);
  }

  public static MessageType project(MessageType schema, Set<String> fields) {
    Set<String> schemaFields = new HashSet<>(Objects.requireNonNull(fields));
    schemaFields.add(Constants.KEY_FIELD_NAME);

    Types.MessageTypeBuilder builder = Types.buildMessage();
    for (String field : schemaFields) {
      if (schema.containsField(field)) {
        Type type = schema.getType(field);
        if (!type.isPrimitive()) {
          throw new IllegalArgumentException("not primitive type is not supported: " + field);
        }
        builder.addField(schema.getType(field));
      }
    }

    return builder.named(schema.getName());
  }
}