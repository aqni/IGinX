package cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class CacheConfig extends AbstractConfig {

  @Optional
  Duration timeout = null;

  @Optional
  ConfigMemorySize capacity = ConfigMemorySize.ofBytes(128 * 1024 * 1024);

  @Override
  public List<ValidationProblem> validate() {
    return Collections.emptyList();
  }
}