package cn.edu.tsinghua.iginx.it.env;

import org.apache.commons.lang3.SystemUtils;

import java.nio.file.Path;
import java.nio.file.Paths;

public interface TestEnvironment {

  void init() throws Exception;

  void clean() throws Exception;

  static boolean isWindows() {
    return SystemUtils.IS_OS_WINDOWS;
  }

  static boolean isUnix() {
    return SystemUtils.IS_OS_UNIX;
  }

  static String getOsName() {
    return SystemUtils.OS_NAME;
  }

  static Path getWorkspaceRoot() {
    return Paths.get("target", "it");
  }

}
