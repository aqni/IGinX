package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.github.luben.zstd.Zstd;
import com.google.common.collect.Range;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class ZstdConfig extends AbstractConfig {

  @Optional
  int level = Zstd.defaultCompressionLevel();

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateInRange(problems, Fields.level, Range.closed(Zstd.minCompressionLevel(), Zstd.maxCompressionLevel()), level);
    return problems;
  }
}
