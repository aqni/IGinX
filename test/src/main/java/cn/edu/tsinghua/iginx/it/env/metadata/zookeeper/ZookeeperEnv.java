package cn.edu.tsinghua.iginx.it.env.metadata.zookeeper;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.it.env.metadata.MetadataEnv;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.NetworkException;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.SyncProtocol;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.zk.ZooKeeperSyncProtocolImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

public class ZookeeperEnv implements MetadataEnv {

  public static final String DEFAULT_CONNECTION_STRING = "127.0.0.1:2181";

  private ZookeeperService zookeeperService;

  @Override
  public void init() throws Exception {
    zookeeperService = new ZookeeperService();
  }

  @Override
  public void clean() throws Exception {
    zookeeperService.close();
  }

  @Override
  public IMetaManager getDefaultMetaManager() {
    Config config = ConfigDescriptor.getInstance().getConfig();
    config.setMetaStorage("zookeeper");
    config.setZookeeperConnectionString(DEFAULT_CONNECTION_STRING);
    ConfigDescriptor.getInstance().getConfig().setStorageEngineList("");
    return DefaultMetaManager.getInstance();
  }

  @Override
  public SyncProtocol newSyncProtocol(String category) throws NetworkException {
    CuratorFramework client =
        CuratorFrameworkFactory.builder()
            .connectString(DEFAULT_CONNECTION_STRING)
            .connectionTimeoutMs(15000)
            .retryPolicy(new RetryForever(1000))
            .build();
    client.start();
    return new ZooKeeperSyncProtocolImpl(category, client, null);
  }
}
