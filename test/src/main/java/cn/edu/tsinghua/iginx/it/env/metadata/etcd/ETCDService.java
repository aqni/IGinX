package cn.edu.tsinghua.iginx.it.env.metadata.etcd;

import cn.edu.tsinghua.iginx.it.env.ExternalProcessService;
import java.io.IOException;
import java.nio.file.Paths;

public class ETCDService extends ExternalProcessService {

  public ETCDService() throws IOException {
    super(Paths.get("etcd"), Paths.get("ETCD.out"), "etcd");
  }
}
