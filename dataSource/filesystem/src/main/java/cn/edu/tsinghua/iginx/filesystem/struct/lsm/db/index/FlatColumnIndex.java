package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.index;

import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FlatColumnIndex implements ColumnIndex {

  private final Object2LongOpenHashMap<IndexKey> fieldToIdMap = new Object2LongOpenHashMap<>();
  private final Long2ObjectOpenHashMap<Types.MinorType> id2typeMap = new Long2ObjectOpenHashMap<>();
  private long sequenceNumber = 0L;

  // TODO: 不需要反向 string->id, 可以通过 pattern 临时构建，然后在此基础上通过 tagkv 过滤

  @Override
  public long put(Field field) throws TypeConflictedException {
    IndexKey key = new IndexKey(field.getName(), field.getMetadata());
    return put(key, field.getType());
  }

  public long put(IndexKey key, ArrowType type) throws TypeConflictedException {
    Long existingId = lookup(key, type);
    if (existingId != null) {
      return existingId;
    }

    long newId = nextSequenceNumber();
    fieldToIdMap.put(key, newId);
    Types.MinorType minorType = Types.getMinorTypeForArrowType(type);
    id2typeMap.put(newId, minorType);
    return newId;
  }

  private long nextSequenceNumber() {
    return sequenceNumber++;
  }

  @Override
  public @Nullable Long lookup(Field field) throws TypeConflictedException {
    IndexKey key = new IndexKey(field.getName(), field.getMetadata());
    return lookup(key, field.getType());
  }

  private Long lookup(IndexKey key, ArrowType type) throws TypeConflictedException {
    if (fieldToIdMap.containsKey(key)) {
      long fieldSqn = fieldToIdMap.getLong(key);
      Types.MinorType existingType = id2typeMap.get(fieldSqn);
      Types.MinorType newType = Types.getMinorTypeForArrowType(type);
      if (existingType != newType) {
        throw new TypeConflictedException(key.toString(), newType.toString(), existingType.toString());
      }
      return fieldToIdMap.getLong(key);
    }
    return null;
  }

  @Override
  public @Nullable Long delete(Field field) throws TypeConflictedException {
    IndexKey key = new IndexKey(field.getName(), field.getMetadata());
    Long existingId = lookup(key, field.getType());
    if (existingId != null) {
      fieldToIdMap.removeLong(key);
      id2typeMap.remove(existingId.longValue());
    }

    return existingId;
  }

  @Override
  public Map<Long, Field> find(List<String> patterns, @Nullable TagFilter tagFilter) {
    List<Predicate<String>> matchers = patterns.stream()
        .map(StringUtils::toColumnMatcher)
        .collect(Collectors.toList());

    Map<Long, Field> result = new Object2ObjectOpenHashMap<>();
    fieldToIdMap.forEach((indexKey, id) -> {
      String name = indexKey.getName();
      Map<String, String> tags = indexKey.getTags();
      boolean patternMatched = matchers.stream().anyMatch(matcher -> matcher.test(name));
      if (!patternMatched) {
        return;
      }
      if (tagFilter != null && !TagKVUtils.match(tags, tagFilter)) {
        return;
      }
      Types.MinorType minorType = id2typeMap.get(id);
      Field field = ArrowFields.of(name, tags, minorType);
      result.put(id, field);
    });

    return result;
  }

  public static Map<Long, Field> find(List<String> patterns, @Nullable TagFilter tagFilter) {
    List<Predicate<String>> matchers = patterns.stream()
        .map(StringUtils::toColumnMatcher)
        .collect(Collectors.toList());

    Map<Long, Field> result = new Object2ObjectOpenHashMap<>();
    fieldToIdMap.forEach((indexKey, id) -> {
      String name = indexKey.getName();
      Map<String, String> tags = indexKey.getTags();
      boolean patternMatched = matchers.stream().anyMatch(matcher -> matcher.test(name));
      if (!patternMatched) {
        return;
      }
      if (tagFilter != null && !TagKVUtils.match(tags, tagFilter)) {
        return;
      }
      Types.MinorType minorType = id2typeMap.get(id);
      Field field = ArrowFields.of(name, tags, minorType);
      result.put(id, field);
    });

    return result;
  }

  @Override
  public void clear() {
    fieldToIdMap.clear();
    id2typeMap.clear();
    sequenceNumber = 0L;
  }

  @Override
  public int count() {
    return id2typeMap.size();
  }

  public static class IndexKey {
    private final String[] data;

    public IndexKey(String fullName, Map<String, String> tags) {
      int size = 1 + tags.size() + tags.size();
      this.data = new String[size];
      this.data[0] = fullName;

      List<String> tagKeys = tags.keySet().stream().sorted().collect(Collectors.toList());
      for (int i = 0; i < tagKeys.size(); i++) {
        String key = tagKeys.get(i);
        String value = tags.get(key);
        this.data[1 + i] = key;
        this.data[1 + tags.size() + i] = value;
      }
    }

    public String getName() {
      return data[0];
    }

    public Map<String, String> getTags() {
      int tagCount = (data.length - 1) / 2;
      if (tagCount == 0) {
        return Collections.emptyMap();
      }
      Object2ObjectOpenHashMap<String, String> result = new Object2ObjectOpenHashMap<>(tagCount);
      for (int i = 0; i < tagCount; i++) {
        String key = data[1 + i];
        String value = data[1 + tagCount + i];
        result.put(key, value);
      }
      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      IndexKey indexKey = (IndexKey) o;
      return Arrays.deepEquals(data, indexKey.data);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
      return getName() + getTags();
    }
  }

}
