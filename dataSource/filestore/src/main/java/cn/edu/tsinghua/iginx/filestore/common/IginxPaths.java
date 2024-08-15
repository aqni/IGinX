package cn.edu.tsinghua.iginx.filestore.common;

import com.google.common.collect.Iterables;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

public class IginxPaths {

  public static final String DOT = ".";

  private IginxPaths() {}

  @Nullable
  public static String join(String... paths) {
    return join(Arrays.asList(paths));
  }

  @Nullable
  public static String join(Iterable<? extends CharSequence> paths) {
    Iterable<? extends CharSequence> nonNullPaths = Iterables.filter(paths, Objects::nonNull);
    if (!nonNullPaths.iterator().hasNext()) {
      return null;
    }
    return String.join(DOT, nonNullPaths);
  }

  public static String get(Path path, String dot) {
    List<String> nodes = new ArrayList<>();
    for (Path fsNode : path) {
      nodes.add(fsNode.toString().replace(DOT, dot));
    }
    return join(nodes);
  }

  public static Path toFilePath(@Nullable String path, String dot, FileSystem fs) {
    if (path == null) {
      return fs.getPath("");
    }
    Pattern splitter = Pattern.compile(Pattern.quote(DOT));
    String[] nodes = splitter.split(path);
    String[] fsNodes = new String[nodes.length];
    for (int i = 0; i < nodes.length; i++) {
      fsNodes[i] = nodes[i].replace(dot, DOT);
    }
    return fs.getPath(fsNodes[0], Arrays.copyOfRange(fsNodes, 1, fsNodes.length));
  }

  public static String toStringPrefix(@Nullable String path) {
    if (path == null) {
      return "";
    } else {
      return path + DOT;
    }
  }

  @Nullable
  public static String fromStringPrefix(String path) {
    if (path.isEmpty()) {
      return null;
    } else {
      if (!path.endsWith(DOT)) {
        throw new IllegalArgumentException("not empty string prefix must not end with a dot");
      }
      return path.substring(0, path.length() - DOT.length());
    }
  }
}
