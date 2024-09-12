package cn.edu.tsinghua.iginx.it.env.metadata;

import cn.edu.tsinghua.iginx.it.env.TestEnv;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.SyncProtocol;

public interface MetadataEnv extends TestEnv {

  IMetaManager getDefaultMetaManager() throws Exception;

  SyncProtocol newSyncProtocol(String category) throws Exception;
}
