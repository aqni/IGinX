package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.google.common.collect.Range;
import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;
import org.apache.arrow.vector.BaseValueVector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class MemTableConfig extends AbstractConfig {

  @Optional
  ConfigMemorySize capacity = ConfigMemorySize.ofBytes(128 * 1024 * 1024);

  @Optional
  int chunkValues = BaseValueVector.INITIAL_VALUE_ALLOCATION;

  @Optional
  Duration timeout = Duration.ofSeconds(1);

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateInRange(problems, Fields.chunkValues, Range.greaterThan(0), chunkValues);
    return problems;
  }
}