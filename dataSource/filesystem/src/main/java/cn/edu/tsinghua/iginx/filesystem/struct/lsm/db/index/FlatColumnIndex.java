package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.index;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.manager.data.ProjectUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.thrift.DataType;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Map;

public class FlatColumnIndex implements ColumnIndex {

  private final Object2LongOpenHashMap<String> fieldToIdMap = new Object2LongOpenHashMap<>();
  private final Long2ObjectOpenHashMap<Types.MinorType> id2typeMap = new Long2ObjectOpenHashMap<>();

  @Override
  public long[] put(List<Field> fields) {
    Map<String, DataType> schema = ArrowFields.toIginxSchema(db.schema());
    Map<String, DataType> schemaMatchTags = ProjectUtils.project(schema, tagFilter);

    Map<String, DataType> projectedSchema = ProjectUtils.project(schemaMatchTags, patterns);

    return new long[0];
  }

  @Override
  public void delete(List<String> patterns, TagFilter filter) {

  }

  @Override
  public long[] find(List<String> patterns, TagFilter filter) {
    return new long[0];
  }

  @Override
  public void clear() {

  }

}
