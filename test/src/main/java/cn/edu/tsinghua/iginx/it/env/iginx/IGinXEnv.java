package cn.edu.tsinghua.iginx.it.env.iginx;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.it.env.TestEnv;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class IGinXEnv implements TestEnv {

  private final Path dataDir;
  private final Path logFile;
  private final Config config;
  private IGinXService iginxService;

  public IGinXEnv(Config config) {
    this(Paths.get("iginx"), Paths.get("IGinX.log"), config);
  }

  public IGinXEnv(Path dataDir, Path logFile, Config config) {
    this.dataDir = Objects.requireNonNull(dataDir);
    this.logFile = Objects.requireNonNull(logFile);
    this.config = Objects.requireNonNull(config);
  }

  @Override
  public void init() throws Exception {
    iginxService = new IGinXService(dataDir, logFile, config);
  }

  @Override
  public void clean() throws Exception {
    iginxService.close();
  }
}
