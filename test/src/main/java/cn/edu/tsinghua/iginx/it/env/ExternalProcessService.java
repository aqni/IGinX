package cn.edu.tsinghua.iginx.it.env;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public abstract class ExternalProcessService implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalProcessService.class);

  protected final Path workspace;
  protected final Process process;

  protected ExternalProcessService(Path relative, List<String> command) throws IOException {
    this(relative, new ProcessBuilder(command));
  }

  protected ExternalProcessService(Path relative, ProcessBuilder processBuilder) throws IOException {
    this.workspace = TestEnvironment.getWorkspaceRoot().resolve(relative);
    LOGGER.info("{} run in {}", getClass().getSimpleName(), workspace);
    MoreFiles.createParentDirectories(workspace);
    this.process = processBuilder.directory(workspace.toFile()).start();
  }

  @Override
  public void close() throws Exception {
    if (process != null) {
      process.destroy();
      process.waitFor();
    }
    MoreFiles.deleteRecursively(workspace, RecursiveDeleteOption.ALLOW_INSECURE);
  }
}
