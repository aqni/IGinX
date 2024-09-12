package cn.edu.tsinghua.iginx.it.mds.zookeeper;

import cn.edu.tsinghua.iginx.it.env.metadata.MetadataEnv;
import cn.edu.tsinghua.iginx.it.env.metadata.zookeeper.ZookeeperEnv;
import cn.edu.tsinghua.iginx.it.mds.BaseMetaManagerIT;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ZooKeeperMetaManagerIT extends BaseMetaManagerIT {

  private static final MetadataEnv metadataEnv = new ZookeeperEnv();

  @BeforeClass
  public static void beforeClass() throws Exception {
    metadataEnv.init();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    metadataEnv.clean();
  }

  @Override
  protected IMetaManager getIMetaManager() throws Exception {
    return metadataEnv.getDefaultMetaManager();
  }
}
