package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.catalog.field;

import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.TypeConflictedException;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.*;
import java.util.function.Consumer;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.checkerframework.checker.nullness.qual.Nullable;

public class TagTreeIndex implements FieldIndex {

  private final TagNodeTree root = new TagNodeTree();

  @Override
  public List<Boolean> contain(List<Field> fields) throws TypeConflictedException {
    List<Boolean> results = new ArrayList<>();
    for (Field field : fields) {
      List<String> nodes = Arrays.asList(field.getName().split("\\."));
      Map<String, String> tags = field.getMetadata();
      Types.MinorType type = Types.getMinorTypeForArrowType(field.getType());
      try {
        boolean result = root.contain(nodes, tags, type);
        results.add(result);
      } catch (TypeConflictedException e) {
        throw new TypeConflictedException(field.getName() + tags, e.getType(), e.getOldType());
      }
    }
    return results;
  }

  @Override
  public List<Field> find(List<String> patterns, @Nullable TagFilter filter) {
    List<List<String>> patternNodes = new ArrayList<>();
    for (String pattern : patterns) {
      List<String> nodes = Arrays.asList(pattern.split("\\."));
      patternNodes.add(nodes);
    }
    List<Field> fields = new ArrayList<>();
    root.find(patternNodes, filter, new ArrayList<>(), fields::add);
    return fields;
  }

  @Override
  public void insert(List<Field> fields) throws TypeConflictedException {
    for (Field field : fields) {
      List<String> nodes = Arrays.asList(field.getName().split("\\."));
      Map<String, String> tags = field.getMetadata();
      Types.MinorType type = Types.getMinorTypeForArrowType(field.getType());
      try {
        root.add(nodes, tags, type);
      } catch (TypeConflictedException e) {
        throw new TypeConflictedException(field.getName() + tags, e.getType(), e.getOldType());
      }
    }
  }

  @Override
  public void delete(List<Field> fields) throws TypeConflictedException {
    for (Field field : fields) {
      List<String> nodes = Arrays.asList(field.getName().split("\\."));
      Map<String, String> tags = field.getMetadata();
      Types.MinorType type = Types.getMinorTypeForArrowType(field.getType());
      try {
        root.remove(nodes, tags, type);
      } catch (TypeConflictedException e) {
        throw new TypeConflictedException(field.getName() + tags, e.getType(), e.getOldType());
      }
    }
  }

  @Override
  public void clear() {
    root.clear();
  }

  private static class PathEndPoint {
    private final Types.MinorType type;
    private @Nullable InvertedTagsSet tagsSet; // null 表示只有无 tagkv 的一列

    public PathEndPoint(Types.MinorType type, Map<String, String> tags) {
      this.type = type;
      if (tags.isEmpty()) {
        this.tagsSet = null;
      } else {
        this.tagsSet = new InvertedTagsSet();
        this.tagsSet.add(tags);
      }
    }

    public void put(Map<String, String> tags, Types.MinorType type) throws TypeConflictedException {
      if (this.type != type) {
        throw new TypeConflictedException(tags.toString(), type.toString(), this.type.toString());
      }
      if (tagsSet == null) {
        if (tags.isEmpty()) {
          return;
        }
        tagsSet = new InvertedTagsSet();
        tagsSet.add(Collections.emptyMap());
      }
      tagsSet.add(tags);
    }

    public boolean contain(Map<String, String> tags, Types.MinorType type)
        throws TypeConflictedException {
      if (this.type != type) {
        throw new TypeConflictedException(tags.toString(), type.toString(), this.type.toString());
      }
      if (tagsSet == null) {
        return tags.isEmpty();
      }
      return tagsSet.contain(tags);
    }

    public void remove(Map<String, String> tags, Types.MinorType type)
        throws TypeConflictedException {
      if (this.type != type) {
        throw new TypeConflictedException(tags.toString(), type.toString(), this.type.toString());
      }
      if (tagsSet == null) {
        if (tags.isEmpty()) {
          tagsSet = new InvertedTagsSet();
        }
        return;
      }
      tagsSet.remove(tags);
    }

    public boolean isEmpty() {
      return tagsSet == null || tagsSet.isEmpty();
    }

    public void find(
        @Nullable TagFilter filter, List<String> prefix, Consumer<Field> fieldConsumer) {
      if (tagsSet == null) {
        if (filter == null || TagKVUtils.match(Collections.emptyMap(), filter)) {
          Field field = ArrowFields.of(String.join(".", prefix), Collections.emptyMap(), type);
          fieldConsumer.accept(field);
        }
      } else {
        Set<Map<String, String>> matchedTags = tagsSet.find(filter);
        for (Map<String, String> tag : matchedTags) {
          Field field = ArrowFields.of(String.join(".", prefix), tag, type);
          fieldConsumer.accept(field);
        }
      }
    }
  }

  private static class TagNodeTree {
    private PathEndPoint pathEndPoint = null; // 非空表示存在以当前前缀为路径的列
    private final Object2ObjectMap<String, TagNodeTree> children = new Object2ObjectOpenHashMap<>();

    private boolean isEmpty() {
      return pathEndPoint == null && children.isEmpty();
    }

    public boolean contain(List<String> nodes, Map<String, String> tags, Types.MinorType type)
        throws TypeConflictedException {
      if (nodes.isEmpty()) {
        if (pathEndPoint == null) {
          return false;
        }
        return pathEndPoint.contain(tags, type);
      }
      String head = nodes.get(0);
      TagNodeTree child = children.get(head);
      if (child == null) {
        return false;
      }
      return child.contain(nodes.subList(1, nodes.size()), tags, type);
    }

    public void find(
        List<List<String>> patternNodes,
        @Nullable TagFilter filter,
        List<String> prefix,
        Consumer<Field> fieldConsumer) {
      boolean reachLeaf = false;
      Map<String, List<List<String>>> groupedPatterns = new Object2ObjectOpenHashMap<>();
      for (List<String> pattern : patternNodes) {
        if (pattern.isEmpty()) {
          reachLeaf = true;
        } else {
          String head = pattern.get(0);
          groupedPatterns.computeIfAbsent(head, k -> new ArrayList<>()).add(pattern);
        }
      }

      if (reachLeaf) {
        if (pathEndPoint != null) {
          pathEndPoint.find(filter, prefix, fieldConsumer);
        }
      }

      List<List<String>> wildcardPatterns = groupedPatterns.remove("*");

      for (Map.Entry<String, List<List<String>>> entry : groupedPatterns.entrySet()) {
        String childName = entry.getKey();
        List<List<String>> patternsForChild = entry.getValue();
        TagNodeTree child = children.get(childName);
        if (child != null) {
          List<List<String>> combinedPatterns = new ArrayList<>();
          for (List<String> pattern : patternsForChild) {
            combinedPatterns.add(pattern.subList(1, pattern.size()));
          }
          if (wildcardPatterns != null) {
            for (List<String> wildcardPattern : wildcardPatterns) {
              combinedPatterns.add(wildcardPattern);
              combinedPatterns.add(wildcardPattern.subList(1, wildcardPattern.size()));
            }
          }
          prefix.add(childName);
          child.find(combinedPatterns, filter, prefix, fieldConsumer);
          prefix.remove(prefix.size() - 1);
        }
      }

      if (wildcardPatterns != null) {
        for (Map.Entry<String, TagNodeTree> entry : children.entrySet()) {
          String childName = entry.getKey();
          TagNodeTree child = entry.getValue();
          if (groupedPatterns.containsKey(childName)) {
            continue;
          }
          List<List<String>> combinedPatterns = new ArrayList<>();
          for (List<String> wildcardPattern : wildcardPatterns) {
            combinedPatterns.add(wildcardPattern);
            combinedPatterns.add(wildcardPattern.subList(1, wildcardPattern.size()));
          }
          prefix.add(childName);
          child.find(combinedPatterns, filter, prefix, fieldConsumer);
          prefix.remove(prefix.size() - 1);
        }
      }
    }

    public void add(List<String> nodes, Map<String, String> tags, Types.MinorType type)
        throws TypeConflictedException {
      if (nodes.isEmpty()) {
        if (pathEndPoint == null) {
          pathEndPoint = new PathEndPoint(type, tags);
        } else {
          pathEndPoint.put(tags, type);
        }
        return;
      }
      String head = nodes.get(0);
      TagNodeTree child = children.computeIfAbsent(head, k -> new TagNodeTree());
      child.add(nodes.subList(1, nodes.size()), tags, type);
    }

    public void remove(List<String> nodes, Map<String, String> tags, Types.MinorType type)
        throws TypeConflictedException {
      if (nodes.isEmpty()) {
        if (pathEndPoint != null) {
          pathEndPoint.remove(tags, type);
          if (pathEndPoint.isEmpty()) {
            pathEndPoint = null;
          }
        }
        return;
      }
      String head = nodes.get(0);
      TagNodeTree child = children.get(head);
      if (child != null) {
        child.remove(nodes.subList(1, nodes.size()), tags, type);
        if (child.isEmpty()) {
          children.remove(head);
        }
      }
    }

    public void clear() {
      pathEndPoint = null;
      children.clear();
    }
  }
}
