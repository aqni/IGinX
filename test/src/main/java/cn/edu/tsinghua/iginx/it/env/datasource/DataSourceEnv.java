package cn.edu.tsinghua.iginx.it.env.datasource;

import cn.edu.tsinghua.iginx.it.env.TestEnv;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;

public interface DataSourceEnv extends TestEnv {
  StorageEngine getStorageEngineMeta();
}
