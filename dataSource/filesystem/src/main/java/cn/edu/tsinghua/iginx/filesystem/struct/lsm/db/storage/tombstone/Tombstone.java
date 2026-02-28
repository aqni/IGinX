package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.tombstone;

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.CachePool;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Tombstone 用于记录被删除的数据范围。
 * 实现了 Serializable 接口，支持标准的 Java 序列化。
 */
public class Tombstone implements Serializable, CachePool.Cacheable {

  private final HashMap<Field, TreeRangeSet<Long>> keyRanges = new HashMap<>();

  public void add(Field field, RangeSet<Long> ranges) {
    if (!keyRanges.containsKey(field)) {
      keyRanges.put(field, TreeRangeSet.create());
    }
    RangeSet<Long> rangeSet = keyRanges.get(field);
    if (rangeSet != null) {
      rangeSet.addAll(ranges);
    }
  }

  public void add(Field field) {
    keyRanges.put(field, null);
  }

  public Map<Field, TreeRangeSet<Long>> getKeyRanges() {
    return keyRanges;
  }

  public static Tombstone merge(Tombstone... tombstones) {
    Tombstone merged = new Tombstone();
    for (Tombstone tombstone : tombstones) {
      for (Map.Entry<Field, TreeRangeSet<Long>> entry : tombstone.getKeyRanges().entrySet()) {
        Field field = entry.getKey();
        RangeSet<Long> ranges = entry.getValue();
        if (ranges == null) {
          merged.add(field);
        } else {
          merged.add(field, ranges);
        }
      }
    }
    return merged;
  }

  @Override
  public String toString() {
    return "Tombstone{" +
        "keyRanges=" + keyRanges +
        '}';
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.defaultWriteObject();
    oos.writeInt(keyRanges.size());
    for (Map.Entry<Field, TreeRangeSet<Long>> entry : keyRanges.entrySet()) {
      Field field = entry.getKey();
      Preconditions.checkArgument(field.isNullable());
      Preconditions.checkArgument(field.getChildren() == null);
      Preconditions.checkArgument(field.getDictionary() == null);
      oos.writeObject(field.getName());
      oos.writeObject(ImmutableMap.copyOf(field.getMetadata()));
      oos.writeObject(Types.getMinorTypeForArrowType(field.getType()));
      oos.writeObject(entry.getValue());
    }
  }

  private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
    ois.defaultReadObject();
    int size = ois.readInt();
    for (int i = 0; i < size; i++) {
      String name = (String) ois.readObject();
      @SuppressWarnings("unchecked")
      Map<String, String> metadata = (ImmutableMap<String, String>) ois.readObject();
      Types.MinorType minorType = (Types.MinorType) ois.readObject();
      Field field = new Field(name, new FieldType(true, minorType.getType(), null, metadata), null);
      @SuppressWarnings("unchecked")
      TreeRangeSet<Long> rangeSet = (TreeRangeSet<Long>) ois.readObject();
      keyRanges.put(field, rangeSet);
    }
  }
}
