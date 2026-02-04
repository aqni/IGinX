package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.catalog.field;

import java.util.Objects;
import java.util.function.Supplier;

public enum FieldIndexType {
  FLAT(FlatIndex::new),
  TAG_TREE(TagTreeIndex::new),
  PREFIX_TREE_TAG(PrefixTagTreeIndex::new);

  private final Supplier<FieldIndex> factory;

  FieldIndexType(Supplier<FieldIndex> factory) {
    this.factory = Objects.requireNonNull(factory);
  }

  public FieldIndex create() {
    return factory.get();
  }
}
