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

package cn.edu.tsinghua.iginx.parquet.util;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

/** The properties of storage engine */
public class StorageProperties {
  private final long writeBufferSize;
  private final long writeBatchSize;
  private final int compactPermits;
  private final long cacheCapacity;
  private final Duration cacheTimeout;
  private final boolean cacheSoftValues;
  private final boolean poolBufferRecycleEnable;
  private final int poolBufferRecycleAlign;
  private final int poolBufferRecycleLimit;
  private final long parquetRowGroupSize;
  private final long parquetPageSize;
  private final String parquetCompression;
  private final int zstdLevel;
  private final int zstdWorkers;
  private final int parquetLz4BufferSize;

  public StorageProperties(
      long writeBufferSize,
      long writeBatchSize,
      int compactPermits,
      long cacheCapacity,
      Duration cacheTimeout,
      boolean cacheSoftValues,
      boolean poolBufferRecycleEnable,
      int poolBufferRecycleAlign,
      int poolBufferRecycleLimit,
      long parquetRowGroupSize,
      long parquetPageSize,
      String parquetCompression,
      int zstdLevel,
      int zstdWorkers,
      int parquetLz4BufferSize) {
    this.writeBufferSize = writeBufferSize;
    this.writeBatchSize = writeBatchSize;
    this.compactPermits = compactPermits;
    this.cacheCapacity = cacheCapacity;
    this.cacheTimeout = cacheTimeout;
    this.cacheSoftValues = cacheSoftValues;
    this.poolBufferRecycleEnable = poolBufferRecycleEnable;
    this.poolBufferRecycleAlign = poolBufferRecycleAlign;
    this.poolBufferRecycleLimit = poolBufferRecycleLimit;
    this.parquetRowGroupSize = parquetRowGroupSize;
    this.parquetPageSize = parquetPageSize;
    this.parquetCompression = parquetCompression;
    this.zstdLevel = zstdLevel;
    this.zstdWorkers = zstdWorkers;
    this.parquetLz4BufferSize = parquetLz4BufferSize;
  }

  /**
   * Get the size of write buffer in bytes
   *
   * @return the size of write buffer, bytes
   */
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * Get the size of write batch in bytes
   *
   * @return the size of write batch, bytes
   */
  public long getWriteBatchSize() {
    return writeBatchSize;
  }

  /**
   * Get the shared permits allocator of flusher, which is used to control the total number of
   * flusher
   *
   * @return the shared permits allocator of flusher
   */
  public int getCompactPermits() {
    return compactPermits;
  }

  /**
   * Get the capacity of cache in bytes
   *
   * @return the capacity of cache, bytes
   */
  public long getCacheCapacity() {
    return cacheCapacity;
  }

  /**
   * Get the expiry timeout of cache
   *
   * @return the expiry timeout of cache
   */
  public Optional<Duration> getCacheTimeout() {
    return Optional.ofNullable(cacheTimeout);
  }

  /**
   * Get whether to enable soft values of cache
   *
   * @return whether to enable soft values of cache
   */
  public boolean getCacheSoftValues() {
    return cacheSoftValues;
  }

  /**
   * Get whether to enable pool buffer recycle
   *
   * @return whether to enable pool buffer recycle
   */
  public boolean getPoolBufferRecycleEnable() {
    return poolBufferRecycleEnable;
  }

  /**
   * Get the pool buffer recycle align
   *
   * @return the pool buffer recycle align
   */
  public int getPoolBufferRecycleAlign() {
    return poolBufferRecycleAlign;
  }

  /**
   * Get the pool buffer recycle limit
   *
   * @return the pool buffer recycle limit
   */
  public int getPoolBufferRecycleLimit() {
    return poolBufferRecycleLimit;
  }

  /**
   * Get the size of parquet row group in bytes
   *
   * @return the size of parquet row group, bytes
   */
  public long getParquetRowGroupSize() {
    return parquetRowGroupSize;
  }

  /**
   * Get the size of parquet page in bytes
   *
   * @return the size of parquet page, bytes
   */
  public long getParquetPageSize() {
    return parquetPageSize;
  }

  /**
   * Get the parquet compression codec name
   *
   * @return the parquet compression codec name
   */
  public String getParquetCompression() {
    return parquetCompression;
  }

  /**
   * Get the zstd level
   *
   * @return the zstd level
   */
  public int getZstdLevel() {
    return zstdLevel;
  }

  /**
   * Get the zstd workers number
   *
   * @return the zstd workers number
   */
  public int getZstdWorkers() {
    return zstdWorkers;
  }

  /**
   * Get the parquet lz4 buffer size
   *
   * @return the parquet lz4 buffer size
   */
  public int getParquetLz4BufferSize() {
    return parquetLz4BufferSize;
  }

  /**
   * Get a builder of StorageProperties
   *
   * @return a builder of StorageProperties
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", StorageProperties.class.getSimpleName() + "[", "]")
        .add("writeBufferSize=" + writeBufferSize)
        .add("writeBatchSize=" + writeBatchSize)
        .add("compactPermits=" + compactPermits)
        .add("cacheCapacity=" + cacheCapacity)
        .add("cacheTimeout=" + cacheTimeout)
        .add("cacheSoftValues=" + cacheSoftValues)
        .add("poolBufferRecycleEnable=" + poolBufferRecycleEnable)
        .add("poolBufferRecycleAlign=" + poolBufferRecycleAlign)
        .add("poolBufferRecycleLimit=" + poolBufferRecycleLimit)
        .add("parquetRowGroupSize=" + parquetRowGroupSize)
        .add("parquetPageSize=" + parquetPageSize)
        .add("parquetCompression='" + parquetCompression + "'")
        .add("zstdLevel=" + zstdLevel)
        .add("zstdWorkers=" + zstdWorkers)
        .add("parquetLz4BufferSize=" + parquetLz4BufferSize)
        .toString();
  }

  /** A builder of StorageProperties */
  public static class Builder {
    public static final String WRITE_BUFFER_SIZE = "write.buffer.size";
    public static final String WRITE_BATCH_SIZE = "write.batch.size";
    public static final String COMPACT_PERMITS = "compact.permits";
    public static final String CACHE_CAPACITY = "cache.capacity";
    public static final String CACHE_TIMEOUT = "cache.timeout";
    public static final String CACHE_VALUE_SOFT = "cache.value.soft";
    public static final String POOL_BUFFER_RECYCLE_ENABLE = "pool.buffer.recycle.enable";
    public static final String POOL_BUFFER_RECYCLE_ALIGN = "pool.buffer.recycle.align";
    public static final String POOL_BUFFER_RECYCLE_LIMIT = "pool.buffer.recycle.limit";
    public static final String PARQUET_BLOCK_SIZE = "parquet.block.size";
    public static final String PARQUET_PAGE_SIZE = "parquet.page.size";
    public static final String PARQUET_COMPRESSOR = "parquet.compression";
    public static final String ZSTD_LEVEL = "zstd.level";
    public static final String ZSTD_WORKERS = "zstd.workers";
    public static final String PARQUET_LZ4_BUFFER_SIZE = "parquet.lz4.buffer.size";

    private static final int UNINITIALIZED_INT = -1;

    private long writeBufferSize = 100 * 1024 * 1024; // BYTE
    private long writeBatchSize = 1024 * 1024; // BYTE
    private int compactPermits = 16;
    private long cacheCapacity = 1024 * 1024 * 1024; // BYTE
    private Duration cacheTimeout = null;
    private boolean cacheSoftValues = false;
    private boolean poolBufferRecycleEnable = true;
    private int poolBufferRecycleAlign = UNINITIALIZED_INT;
    private int poolBufferRecycleLimit = UNINITIALIZED_INT;
    private long parquetRowGroupSize = 128 * 1024 * 1024; // BYTE
    private long parquetPageSize = 8 * 1024; // BYTE
    private String parquetCompression = "UNCOMPRESSED";
    private int zstdLevel = 3;
    private int zstdWorkers = 0;
    private int parquetLz4BufferSize = 256 * 1024; // BYTE

    private Builder() {}

    /**
     * Set the size of write buffer in bytes
     *
     * @param writeBufferSize the size of write buffer, bytes
     * @return this builder
     */
    public Builder setWriteBufferSize(long writeBufferSize) {
      ParseUtils.checkPositive(writeBufferSize);
      this.writeBufferSize = writeBufferSize;
      return this;
    }

    /**
     * Set the size of write batch in bytes
     *
     * @param writeBatchSize the size of write batch, bytes
     * @return this builder
     */
    public Builder setWriteBatchSize(long writeBatchSize) {
      ParseUtils.checkPositive(writeBatchSize);
      this.writeBatchSize = writeBatchSize;
      return this;
    }

    /**
     * Set the capacity of cache in bytes
     *
     * @param cacheCapacity the capacity of cache, bytes
     * @return this builder
     */
    public Builder setCacheCapacity(long cacheCapacity) {
      ParseUtils.checkNonNegative(cacheCapacity);
      this.cacheCapacity = cacheCapacity;
      return this;
    }

    /**
     * Set the expiry timeout of cache
     *
     * @param cacheTimeout the expiry timeout of cache
     * @return this builder
     */
    public Builder setCacheTimeout(Duration cacheTimeout) {
      ParseUtils.checkPositive(cacheTimeout);
      this.cacheTimeout = cacheTimeout;
      return this;
    }

    /**
     * Set whether to enable soft values of cache
     *
     * @param cacheSoftValues whether to enable soft values of cache
     * @return this builder
     */
    public Builder setCacheSoftValues(boolean cacheSoftValues) {
      this.cacheSoftValues = cacheSoftValues;
      return this;
    }

    /**
     * Set the number of flusher permits
     *
     * @param compactPermits the number of flusher permits
     * @return this builder
     */
    public Builder setCompactPermits(int compactPermits) {
      ParseUtils.checkPositive(compactPermits);
      this.compactPermits = compactPermits;
      return this;
    }

    /**
     * Set the pool buffer recycle enable
     *
     * @param poolBufferRecycleEnable the pool buffer recycle enable
     * @return this builder
     */
    public Builder setPoolBufferRecycleEnable(boolean poolBufferRecycleEnable) {
      this.poolBufferRecycleEnable = poolBufferRecycleEnable;
      return this;
    }

    /**
     * Set the pool buffer recycle align
     *
     * @param poolBufferRecycleAlign the pool buffer recycle align
     * @return this builder
     */
    public Builder setPoolBufferRecycleAlign(int poolBufferRecycleAlign) {
      this.poolBufferRecycleAlign = poolBufferRecycleAlign;
      return this;
    }

    /**
     * Set the pool buffer recycle limit
     *
     * @param poolBufferRecycleLimit the pool buffer recycle limit
     * @return this builder
     */
    public Builder setPoolBufferRecycleLimit(int poolBufferRecycleLimit) {
      this.poolBufferRecycleLimit = poolBufferRecycleLimit;
      return this;
    }

    /**
     * Set the size of parquet row group in bytes
     *
     * @param parquetRowGroupSize the size of parquet row group, bytes
     * @return this builder
     */
    public Builder setParquetRowGroupSize(long parquetRowGroupSize) {
      ParseUtils.checkPositive(parquetRowGroupSize);
      this.parquetRowGroupSize = parquetRowGroupSize;
      return this;
    }

    /**
     * Set the size of parquet page in bytes
     *
     * @param parquetPageSize the size of parquet page, bytes
     * @return this builder
     */
    public Builder setParquetPageSize(long parquetPageSize) {
      ParseUtils.checkPositive(parquetPageSize);
      this.parquetPageSize = parquetPageSize;
      return this;
    }

    /**
     * Set the parquet compression codec name
     *
     * @param name the parquet compression codec name
     *     <p>Supported values: "UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "ZSTD", "LZ4_RAW"
     * @return this builder
     */
    public Builder setParquetCompression(String name) {
      ParseUtils.in("UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "ZSTD", "LZ4_RAW").accept(name);
      this.parquetCompression = name;
      return this;
    }

    /**
     * Set the zstd level
     *
     * @param level the zstd level
     * @return this builder
     */
    public Builder setZstdLevel(int level) {
      this.zstdLevel = level;
      return this;
    }

    /**
     * Set the zstd workers number
     *
     * @param workers the zstd workers number
     * @return this builder
     */
    public Builder setZstdWorkers(int workers) {
      ParseUtils.checkNonNegative(workers);
      this.zstdWorkers = workers;
      return this;
    }

    /**
     * Set the parquet lz4 buffer size
     *
     * @param bufferSize the parquet lz4 buffer size
     * @return this builder
     */
    public Builder setParquetLz4BufferSize(int bufferSize) {
      ParseUtils.checkPositive(bufferSize);
      this.parquetLz4BufferSize = bufferSize;
      return this;
    }

    /**
     * Parse properties to set the properties of StorageProperties
     *
     * @param properties the properties to be parsed
     *     <p>Supported keys:
     *     <ul>
     *       <li>write.buffer.size: the size of write buffer, bytes
     *       <li>write.batch.size: the size of write batch, bytes
     *       <li>compact.permits: the number of flusher permits
     *       <li>cache.capacity: the capacity of cache, bytes
     *       <li>cache.timeout: the expiry timeout of cache, iso8601 duration
     *       <li>cache.value.soft: whether to enable soft values of cache
     *       <li>pool.buffer.recycle.enable: the pool buffer recycle enable
     *       <li>pool.buffer.recycle.align: the pool buffer recycle align
     *       <li>pool.buffer.recycle.limit: the pool buffer recycle limit
     *       <li>parquet.block.size: the size of parquet row group, bytes
     *       <li>parquet.page.size: the size of parquet page, bytes
     *       <li>parquet.compression: the parquet compression codec name
     *       <li>parquet.lz4.buffer.size: the parquet lz4 buffer size, bytes
     *       <li>zstd.level: the zstd level
     *       <li>zstd.workers: the zstd workers number
     *     </ul>
     *
     * @return this builder
     */
    public Builder parse(Map<String, String> properties) {
      ParseUtils.getOptionalLong(properties, WRITE_BUFFER_SIZE).ifPresent(this::setWriteBufferSize);
      ParseUtils.getOptionalLong(properties, WRITE_BATCH_SIZE).ifPresent(this::setWriteBatchSize);
      ParseUtils.getOptionalInteger(properties, COMPACT_PERMITS).ifPresent(this::setCompactPermits);
      ParseUtils.getOptionalLong(properties, CACHE_CAPACITY).ifPresent(this::setCacheCapacity);
      ParseUtils.getOptionalDuration(properties, CACHE_TIMEOUT).ifPresent(this::setCacheTimeout);
      ParseUtils.getOptionalBoolean(properties, CACHE_VALUE_SOFT)
          .ifPresent(this::setCacheSoftValues);
      ParseUtils.getOptionalBoolean(properties, POOL_BUFFER_RECYCLE_ENABLE)
          .ifPresent(this::setPoolBufferRecycleEnable);
      ParseUtils.getOptionalInteger(properties, POOL_BUFFER_RECYCLE_ALIGN)
          .ifPresent(this::setPoolBufferRecycleAlign);
      ParseUtils.getOptionalInteger(properties, POOL_BUFFER_RECYCLE_LIMIT)
          .ifPresent(this::setPoolBufferRecycleLimit);
      ParseUtils.getOptionalLong(properties, PARQUET_BLOCK_SIZE)
          .ifPresent(this::setParquetRowGroupSize);
      ParseUtils.getOptionalLong(properties, PARQUET_PAGE_SIZE).ifPresent(this::setParquetPageSize);
      ParseUtils.getOptionalString(properties, PARQUET_COMPRESSOR)
          .ifPresent(this::setParquetCompression);
      ParseUtils.getOptionalInteger(properties, ZSTD_LEVEL).ifPresent(this::setZstdLevel);
      ParseUtils.getOptionalInteger(properties, ZSTD_WORKERS).ifPresent(this::setZstdWorkers);
      ParseUtils.getOptionalInteger(properties, PARQUET_LZ4_BUFFER_SIZE)
          .ifPresent(this::setParquetLz4BufferSize);
      return this;
    }

    /**
     * Build a StorageProperties
     *
     * @return a StorageProperties
     */
    public StorageProperties build() {
      if (poolBufferRecycleAlign == UNINITIALIZED_INT) {
        poolBufferRecycleAlign = (int) parquetPageSize;
      }
      if (poolBufferRecycleLimit == UNINITIALIZED_INT) {
        poolBufferRecycleLimit = (int) parquetRowGroupSize;
      }
      return new StorageProperties(
          writeBufferSize,
          writeBatchSize,
          compactPermits,
          cacheCapacity,
          cacheTimeout,
          cacheSoftValues,
          poolBufferRecycleEnable,
          poolBufferRecycleAlign,
          poolBufferRecycleLimit,
          parquetRowGroupSize,
          parquetPageSize,
          parquetCompression,
          zstdLevel,
          zstdWorkers,
          parquetLz4BufferSize);
    }
  }
}