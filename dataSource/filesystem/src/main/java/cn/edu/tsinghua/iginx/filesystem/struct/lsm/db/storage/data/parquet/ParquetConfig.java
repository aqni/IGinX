package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.github.luben.zstd.Zstd;
import com.google.common.collect.Range;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;
import org.apache.paimon.shade.org.apache.parquet.format.CompressionCodec;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class ParquetConfig extends AbstractConfig {

  @Optional
  int readBatchSize = 1024;

  @Optional
  int writeBatchSize = 1024;

  @Optional
  ConfigMemorySize writeBatchMemory = ConfigMemorySize.ofBytes(128 * 1024 * 1024);

  @Optional
  CompressionCodec compression = CompressionCodec.LZ4_RAW;

  @Optional
  int zstdLevel = Zstd.maxCompressionLevel();


  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateInRange(problems, Fields.zstdLevel, Range.closed(Zstd.minCompressionLevel(), Zstd.maxCompressionLevel()), zstdLevel);
    return problems;
  }

  public static ParquetConfig of(Config config) {
    return of(config, ParquetConfig.class);
  }
}
