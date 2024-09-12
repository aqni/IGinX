package cn.edu.tsinghua.iginx.it.env;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class EtcdService extends ExternalProcessService {

  private static final Logger LOGGER = LoggerFactory.getLogger(EtcdService.class);

  private Process process;

  public EtcdService() {
    this(Paths.get("etcd"));
  }

  public EtcdService(Path workspace) {
    super(workspace, new ProcessBuilder("etcd"));
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    if(process != null) {
      throw new IllegalStateException("etcd is already running");
    }
    ProcessBuilder pb = new ProcessBuilder("etcd");
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  public String getEndPoints() {
    return "http://localhost:2379";
  }
}
