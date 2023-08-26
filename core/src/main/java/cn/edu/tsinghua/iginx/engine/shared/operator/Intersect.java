package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class Intersect extends AbstractBinaryOperator {

  private final List<String> leftOrder;

  private final List<String> rightOrder;

  private final boolean isDistinct;

  public Intersect(
      Source sourceA,
      Source sourceB,
      List<String> leftOrder,
      List<String> rightOrder,
      boolean isDistinct) {
    super(OperatorType.Intersect, sourceA, sourceB);
    this.leftOrder = leftOrder;
    this.rightOrder = rightOrder;
    this.isDistinct = isDistinct;
  }

  public List<String> getLeftOrder() {
    return leftOrder;
  }

  public List<String> getRightOrder() {
    return rightOrder;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  @Override
  public Operator copy() {
    return new Intersect(
        getSourceA().copy(),
        getSourceB().copy(),
        new ArrayList<>(leftOrder),
        new ArrayList<>(rightOrder),
        isDistinct);
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("LeftOrder: ");
    for (String order : leftOrder) {
      builder.append(order).append(",");
    }
    builder.append(" RightOrder: ");
    for (String order : rightOrder) {
      builder.append(order).append(",");
    }
    builder.append(" isDistinct: ").append(isDistinct);
    return builder.toString();
  }
}