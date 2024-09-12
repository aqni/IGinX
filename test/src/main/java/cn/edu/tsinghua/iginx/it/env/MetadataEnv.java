package cn.edu.tsinghua.iginx.it.env;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.SyncProtocol;

public abstract class MetadataEnv implements TestEnvironment {

  public abstract IMetaManager getIMetaManager();

  public abstract SyncProtocol newSyncProtocol(String category) ;

  public static MetadataEnv current() {
    return null;
  }
}
