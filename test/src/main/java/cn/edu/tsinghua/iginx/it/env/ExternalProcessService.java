package cn.edu.tsinghua.iginx.it.env;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExternalProcessService implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalProcessService.class);

  protected final Path workspace;
  protected final Process process;

  protected ExternalProcessService(Path dataDir, Path logFile, String... command)
      throws IOException {
    this(dataDir, logFile, Arrays.asList(command));
  }

  protected ExternalProcessService(Path dataDir, Path logFile, List<String> command)
      throws IOException {
    this(dataDir, logFile, new ProcessBuilder(command));
  }

  protected ExternalProcessService(Path dataDir, Path logFile, ProcessBuilder processBuilder)
      throws IOException {
    this.workspace = TestEnv.getWorkspaceRoot().resolve(dataDir).normalize().toAbsolutePath();
    Path logFilePath = TestEnv.getWorkspaceRoot().resolve(logFile).normalize().toAbsolutePath();
    LOGGER.info("run {} in {} redirect {}", processBuilder.command(), workspace, logFilePath);
    Files.createDirectories(workspace);
    this.process =
        processBuilder
            .directory(workspace.toFile())
            .redirectErrorStream(true)
            .redirectOutput(logFilePath.toFile())
            .start();
    if (!process.isAlive()) {
      throw new IOException("Process is not alive, exit code: " + process.exitValue());
    }
  }

  protected void stop() throws Exception {
    if (!process.destroyForcibly().waitFor(2, TimeUnit.SECONDS)) {
      throw new IOException("Process is still alive after 2 seconds, destroying it forcibly");
    }
  }

  @Override
  public void close() throws Exception {
    stop();
    MoreFiles.deleteRecursively(workspace, RecursiveDeleteOption.ALLOW_INSECURE);
  }

  public static void killOnPort(int port) throws Exception {
    if (TestEnv.isUnix()) {
      Runtime.getRuntime().exec("kill -9 $(lsof -t -i:" + port + ")").waitFor();
    } else if (TestEnv.isWindows()) {
      Runtime.getRuntime()
          .exec(
              "pwsh -Command \"Get-NetTCPConnection -LocalPort "
                  + port
                  + " | ForEach-Object {Stop-Process -Id $_.OwningProcess -Force}\"")
          .waitFor();
    } else {
      throw new UnsupportedOperationException(
          "Unsupported operating system: " + TestEnv.getOsName());
    }
  }
}
