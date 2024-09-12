package cn.edu.tsinghua.iginx.it.env.metadata.etcd;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.it.env.metadata.MetadataEnv;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.SyncProtocol;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.etcd.ETCDSyncProtocolImpl;
import io.etcd.jetcd.Client;

public class ETCDEnv implements MetadataEnv {

  public static final String DEFAULT_END_POINTS = "http://localhost:2379";

  private ETCDService etcdService;

  @Override
  public void init() throws Exception {
    etcdService = new ETCDService();
  }

  @Override
  public void clean() throws Exception {
    etcdService.close();
  }

  @Override
  public IMetaManager getDefaultMetaManager() {
    Config config = ConfigDescriptor.getInstance().getConfig();
    config.setMetaStorage("etcd");
    config.setEtcdEndpoints(DEFAULT_END_POINTS);
    ConfigDescriptor.getInstance().getConfig().setStorageEngineList("");
    return DefaultMetaManager.getInstance();
  }

  @Override
  public SyncProtocol newSyncProtocol(String category) {
    return new ETCDSyncProtocolImpl(
        category, Client.builder().endpoints(DEFAULT_END_POINTS).build());
  }
}
