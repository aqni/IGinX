package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.index;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.TypeConflictedException;
import org.apache.arrow.vector.types.pojo.Field;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public interface ColumnIndex {

  long put(Field field) throws TypeConflictedException;

  @Nullable
  Long lookup(Field field) throws TypeConflictedException;

  Map<Long, Field> find(List<String> patterns, @Nullable TagFilter filter);

  @org.checkerframework.checker.nullness.qual.Nullable Long delete(Field field) throws TypeConflictedException;

  void clear();

  int count();
}
