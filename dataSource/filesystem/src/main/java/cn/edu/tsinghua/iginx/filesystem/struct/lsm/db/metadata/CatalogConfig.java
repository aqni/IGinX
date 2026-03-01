package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.field.FieldIndexType;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

import java.util.Collections;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class CatalogConfig extends AbstractConfig {

  @Optional
  FieldIndexType schemaIndexType = FieldIndexType.FLAT;

  @Override
  public List<ValidationProblem> validate() {
    return Collections.emptyList();
  }
}