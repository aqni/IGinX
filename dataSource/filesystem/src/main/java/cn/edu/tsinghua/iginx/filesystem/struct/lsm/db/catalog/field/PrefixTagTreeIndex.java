package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.catalog.field;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.TypeConflictedException;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Field;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PrefixTagTreeIndex implements FieldIndex {
  @Override
  public List<Boolean> contain(List<Field> fields) throws TypeConflictedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Field> find(List<String> patterns, @Nullable TagFilter filter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void insert(List<Field> fields) throws TypeConflictedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(List<Field> fields) throws TypeConflictedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }
}
