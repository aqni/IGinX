package cn.edu.tsinghua.iginx.it.expansion;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.it.func.Controller;
import cn.edu.tsinghua.iginx.session.QueryDataSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class BaseHasExpandHasIT extends BaseExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseHasExpandHasIT.class);

  public BaseHasExpandHasIT() {
    super(true, true);
  }

  protected abstract boolean isSupportKey();

  @Order(900)
  protected void testSameKeyWarning() {
    try {
      getSession().executeSql(
          "insert into mn.wf01.wt01 (key, status) values (0, 123),(1, 123),(2, 123),(3, 123);");
      String statement = "select * from mn.wf01.wt01;";

      QueryDataSet res = getSession().executeQuery(statement);
      if ((res.getWarningMsg() == null
          || res.getWarningMsg().isEmpty()
          || !res.getWarningMsg().contains("The query results contain overlapped keys."))
          && isSupportKey()) {
        LOGGER.error("未抛出重叠key的警告");
        Assertions.fail();
      }

      Controller.clearData(getSession());

      res = getSession().executeQuery(statement);
      if (res.getWarningMsg() != null && isSupportKey()) {
        LOGGER.error("不应抛出重叠key的警告");
        Assertions.fail();
      }
    } catch (SessionException e) {
      LOGGER.error("query data error: ", e);
    }
  }

}
