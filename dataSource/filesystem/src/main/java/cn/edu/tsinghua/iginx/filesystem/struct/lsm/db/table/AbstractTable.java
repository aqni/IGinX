package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.table;

import org.apache.arrow.vector.types.pojo.Field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractTable implements Table {

  @Override
  public Meta getMeta() throws IOException {
    List<Meta> subMetas = new ArrayList<>();
    for (SubTable subTable : getSubTables()) {
      subMetas.add(subTable.getMeta());
    }
    return mergeMeta(subMetas);
  }

  private static Meta mergeMeta(Iterable<Meta> subMetas) {
    Map<Field, Statistic> fieldStats = new HashMap<>();
    for (Meta subMeta : subMetas) {
      Map<Field, Statistic> subFieldStats = subMeta.getFieldStats();
      for (Map.Entry<Field, Statistic> entry : subFieldStats.entrySet()) {
        Field field = entry.getKey();
        Statistic statistic = entry.getValue();
        fieldStats.merge(field, statistic, AbstractTable::mergeStatistic);
      }
    }
    return new Meta(fieldStats);
  }

  private static Statistic mergeStatistic(Statistic s1, Statistic s2) {
    return new Statistic(s1.getKeyRange().span(s2.getKeyRange()));
  }

}
