package cn.edu.tsinghua.iginx.it.mds.etcd;

import cn.edu.tsinghua.iginx.it.env.metadata.MetadataEnv;
import cn.edu.tsinghua.iginx.it.env.metadata.etcd.ETCDEnv;
import cn.edu.tsinghua.iginx.it.mds.BaseMetaManagerIT;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ETCDMetaManagerIT extends BaseMetaManagerIT {

  private static final MetadataEnv metadataEnv = new ETCDEnv();

  @BeforeClass
  public static void beforeClass() throws Exception {
    metadataEnv.init();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    metadataEnv.clean();
  }

  @Override
  protected IMetaManager getIMetaManager() {
    return new ETCDEnv().getDefaultMetaManager();
  }
}
