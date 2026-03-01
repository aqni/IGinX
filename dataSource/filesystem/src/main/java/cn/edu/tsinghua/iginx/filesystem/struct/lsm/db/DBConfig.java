package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemTableConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.CatalogConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.StorageConfig;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class DBConfig extends AbstractConfig {

  @Optional
  MemTableConfig memtable = new MemTableConfig();

  @Optional
  CatalogConfig catalog = new CatalogConfig();

  @Optional
  StorageConfig storage = new StorageConfig();

  @Optional
  boolean flushOnClose = true;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateSubConfig(problems, Fields.memtable, memtable);
    validateSubConfig(problems, Fields.catalog, catalog);
    validateSubConfig(problems, Fields.storage, storage);
    return problems;
  }
}

