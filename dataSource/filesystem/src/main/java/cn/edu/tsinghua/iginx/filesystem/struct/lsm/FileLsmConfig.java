package cn.edu.tsinghua.iginx.filesystem.struct.lsm;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.DBConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.shared.SharedConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.FileTreeConfig;
import com.typesafe.config.Config;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class FileLsmConfig extends AbstractConfig {

  @Optional
  SharedConfig shared = new SharedConfig();

  @Optional
  DBConfig db = new DBConfig();

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateSubConfig(problems, Fields.shared, shared);
    validateSubConfig(problems, Fields.db, db);
    return problems;
  }

  public static FileLsmConfig of(Config config) {
    return of(config, FileLsmConfig.class);
  }
}
