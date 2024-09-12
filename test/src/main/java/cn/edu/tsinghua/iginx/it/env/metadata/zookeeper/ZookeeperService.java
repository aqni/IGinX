package cn.edu.tsinghua.iginx.it.env.metadata.zookeeper;

import cn.edu.tsinghua.iginx.it.env.ExternalProcessService;
import cn.edu.tsinghua.iginx.it.env.TestEnv;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ZookeeperService extends ExternalProcessService {

  public ZookeeperService() throws IOException {
    super(Paths.get("zookeeper"), Paths.get("ZooKeeper.out"), startupCommand());
  }

  public static List<String> startupCommand() {
    if (TestEnv.isUnix()) {
      return Arrays.asList("zkServer.sh", "start-foreground");
    } else if (TestEnv.isWindows()) {
      return Collections.singletonList("zkServer.cmd");
    } else {
      throw new UnsupportedOperationException(
          "Unsupported operating system: " + TestEnv.getOsName());
    }
  }

  @Override
  protected void stop() throws Exception {
    super.stop();
    killOnPort(2181);
  }
}
