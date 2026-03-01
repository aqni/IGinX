package cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.cache.CacheConfig;
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
public class SharedConfig extends AbstractConfig {

  @Optional
  CacheConfig cache = new CacheConfig();

  @Optional
  int writers = 2;

  @Optional
  int memtableQueue = 2;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateSubConfig(problems, Fields.cache, cache);
    validateInRange(problems, Fields.writers, Range.greaterThan(0), writers);
    validateInRange(problems, Fields.memtableQueue, Range.greaterThan(0), memtableQueue);
    return problems;
  }
}
