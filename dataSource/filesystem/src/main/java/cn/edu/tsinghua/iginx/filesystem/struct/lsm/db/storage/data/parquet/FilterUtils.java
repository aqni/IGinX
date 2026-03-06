package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.parquet;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class FilterUtils {

  private FilterUtils() {
  }

  @Nullable
  public static Predicate toPaimonPredicate(RangeSet<Long> rangeSet, RowType projectedSchema) {
    List<Predicate> predicates = new ArrayList<>();
    for (Range<Long> range : rangeSet.asRanges()) {
      Predicate predicate = toPaimonPredicate(range, projectedSchema);
      if (predicate != null) {
        predicates.add(predicate);
      }
    }
    return or(predicates);
  }

  @Nullable
  public static Predicate toPaimonPredicate(Range<Long> range, RowType projectedSchema) {
    PredicateBuilder builder = new PredicateBuilder(projectedSchema);
    List<Predicate> predicates = new ArrayList<>();
    if (range.hasLowerBound()) {
      if (range.lowerBoundType() == BoundType.CLOSED) {
        predicates.add(builder.greaterOrEqual(0, range.lowerEndpoint()));
      } else {
        predicates.add(builder.greaterThan(0, range.lowerEndpoint()));
      }
    }
    if (range.hasUpperBound()) {
      if (range.upperBoundType() == BoundType.CLOSED) {
        predicates.add(builder.lessOrEqual(0, range.upperEndpoint()));
      } else {
        predicates.add(builder.lessThan(0, range.upperEndpoint()));
      }
    }
    return and(predicates);
  }

  @Nullable
  public static Predicate and(List<Predicate> predicates) {
    if (predicates.isEmpty()) {
      return null;
    }
    return PredicateBuilder.and(predicates);
  }

  @Nullable
  public static Predicate or(List<Predicate> predicates) {
    if (predicates.isEmpty()) {
      return null;
    }
    return PredicateBuilder.or(predicates);
  }
}
