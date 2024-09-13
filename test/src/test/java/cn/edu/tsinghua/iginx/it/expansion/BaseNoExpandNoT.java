package cn.edu.tsinghua.iginx.it.expansion;

import org.junit.jupiter.api.Order;

import static cn.edu.tsinghua.iginx.it.expansion.constant.Constant.expPort;

public abstract class BaseNoExpandNoT extends BaseExpansionIT {

  protected BaseNoExpandNoT(boolean originHasData, boolean expandedHasData) {
    super(originHasData, expandedHasData);
  }
  
}
