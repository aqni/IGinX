package cn.edu.tsinghua.iginx.it.expansion;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.it.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.session.ClusterInfo;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static cn.edu.tsinghua.iginx.it.expansion.constant.Constant.*;
import static cn.edu.tsinghua.iginx.it.expansion.constant.Constant.EXP_VALUES_LIST2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class BaseNoExpandHasIT extends BaseExpansionIT {

  private final String EXP_SCHEMA_PREFIX = null;

  protected BaseNoExpandHasIT() {
    super(false,true);
  }

  @Order(700)
  @Test
  public void testAddAndRemoveStorageEngineWithPrefix() {
    String dataPrefix1 = "nt.wf03";
    String dataPrefix2 = "nt.wf04";
    String schemaPrefixSuffix = "";
    String schemaPrefix1 = "p1";
    String schemaPrefix2 = "p2";
    String schemaPrefix3 = "p3";

    List<List<Object>> valuesList = EXP_VALUES_LIST1;

    // 添加不同 schemaPrefix，相同 dataPrefix
    testShowColumnsInExpansion(true);
    addStorageEngine(expPort, true, true, dataPrefix1, schemaPrefix1, extraParams);
    testShowColumnsInExpansion(false);

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 后查询
    String statement = "select status2 from *;";
    List<String> pathList = Arrays.asList("nt.wf03.wt01.status2", "p1.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, REPEAT_EXP_VALUES_LIST1);

    addStorageEngine(expPort, true, true, dataPrefix1, schemaPrefix2, extraParams);
    addStorageEngine(expPort, true, true, dataPrefix1, null, extraParams);
    testShowClusterInfo(5);

    // 如果是重复添加，则报错
    String res = addStorageEngine(expPort, true, true, dataPrefix1, null, extraParams);
    if (res != null && !res.contains("repeatedly add storage engine")) {
      fail();
    }
    testShowClusterInfo(5);

    addStorageEngine(expPort, true, true, dataPrefix1, schemaPrefix3, extraParams);
    // 这里是之后待测试的点，如果添加包含关系的，应当报错。
    //    res = addStorageEngine(expPort, true, true, "nt.wf03.wt01", "p3");
    // 添加相同 schemaPrefix，不同 dataPrefix
    addStorageEngine(expPort, true, true, dataPrefix2, schemaPrefix3, extraParams);
    testShowClusterInfo(7);

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 后查询
    statement = "select wt01.status2 from p1.nt.wf03;";
    pathList = Collections.singletonList("p1.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = p2 后查询
    statement = "select wt01.status2 from p2.nt.wf03;";
    pathList = Collections.singletonList("p2.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 添加节点 dataPrefix = dataPrefix1 && schemaPrefix = null 后查询
    statement = "select wt01.status2 from nt.wf03;";
    pathList = Collections.singletonList("nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 添加节点 dataPrefix = null && schemaPrefix = p3 后查询
    statement = "select wt01.status2 from p3.nt.wf03;";
    pathList = Collections.singletonList("p3.nt.wf03.wt01.status2");
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 通过 session 接口测试移除节点
    testShowColumnsRemoveStorageEngine(true);
    List<RemovedStorageEngineInfo> removedStorageEngineList = new ArrayList<>();
    removedStorageEngineList.add(
        new RemovedStorageEngineInfo("127.0.0.1", expPort, "p2" + schemaPrefixSuffix, dataPrefix1));
    removedStorageEngineList.add(
        new RemovedStorageEngineInfo("127.0.0.1", expPort, "p3" + schemaPrefixSuffix, dataPrefix1));
    try {
      session.removeHistoryDataSource(removedStorageEngineList);
      testShowClusterInfo(5);
    } catch (SessionException e) {
      LOGGER.error("remove history data source through session api error: ", e);
    }
    testShowColumnsRemoveStorageEngine(false);

    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p2 + schemaPrefixSuffix 后再查询
    statement = "select * from p2.nt.wf03;";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);
    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p3 + schemaPrefixSuffix
    // 后再查询，测试重点是移除相同 schemaPrefix，不同 dataPrefix
    statement = "select wt01.temperature from p3.nt.wf04;";
    List<String> pathListAns = new ArrayList<>();
    pathListAns.add("p3.nt.wf04.wt01.temperature");
    SQLTestTools.executeAndCompare(session, statement, pathListAns, EXP_VALUES_LIST2);

    // 通过 sql 语句测试移除节点
    String removeStatement = "remove historydatasource (\"127.0.0.1\", %d, \"%s\", \"%s\");";
    try {
      session.executeSql(
          String.format(removeStatement, expPort, "p1" + schemaPrefixSuffix, dataPrefix1));
      session.executeSql(
          String.format(removeStatement, expPort, "p3" + schemaPrefixSuffix, dataPrefix2));
      session.executeSql(String.format(removeStatement, expPort, "", dataPrefix1));
      testShowClusterInfo(2);
    } catch (SessionException e) {
      LOGGER.error("remove history data source through sql error: ", e);
    }
    // 移除节点 dataPrefix = dataPrefix1 && schemaPrefix = p1 + schemaPrefixSuffix 后再查询
    statement = "select * from p1.nt.wf03;";
    expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(session, statement, expect);

    try {
      session.executeSql(
          String.format(removeStatement, expPort, "p1" + schemaPrefixSuffix, dataPrefix1));
    } catch (SessionException e) {
      if (!e.getMessage().contains("remove history data source failed")) {
        LOGGER.error(
            "remove history data source should throw error when removing the node that does not exist");
        fail();
      }
    }
    testShowClusterInfo(2);
  }

  protected void testShowColumnsInExpansion(boolean before) {
    String statement = "SHOW COLUMNS nt.wf03.*;";
    String expected =
        "Columns:\n"
            + "+--------------------+--------+\n"
            + "|                Path|DataType|\n"
            + "+--------------------+--------+\n"
            + "|nt.wf03.wt01.status2|    LONG|\n"
            + "+--------------------+--------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS;";
    if (before) {
      expected =
          "Columns:\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                                  Path|DataType|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                                 b.b.b|    LONG|\n"
              + "|                                                                        ln.wf02.status| BOOLEAN|\n"
              + "|                                                                       ln.wf02.version|  BINARY|\n"
              + "|                                                                  nt.wf03.wt01.status2|    LONG|\n"
              + "|                                                              nt.wf04.wt01.temperature|  DOUBLE|\n"
              + "|zzzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzzzz|    LONG|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "Total line number = 6\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                                  Path|DataType|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "|                                                                                 b.b.b|    LONG|\n"
              + "|                                                                        ln.wf02.status| BOOLEAN|\n"
              + "|                                                                       ln.wf02.version|  BINARY|\n"
              + "|                                                                  nt.wf03.wt01.status2|    LONG|\n"
              + "|                                                              nt.wf04.wt01.temperature|  DOUBLE|\n"
              + "|                                                               p1.nt.wf03.wt01.status2|    LONG|\n"
              + "|zzzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzzzz|    LONG|\n"
              + "+--------------------------------------------------------------------------------------+--------+\n"
              + "Total line number = 7\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS p1.*;";
    if (before) {
      expected =
          "Columns:\n"
              + "+----+--------+\n"
              + "|Path|DataType|\n"
              + "+----+--------+\n"
              + "+----+--------+\n"
              + "Empty set.\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|p1.nt.wf03.wt01.status2|    LONG|\n"
              + "+-----------------------+--------+\n"
              + "Total line number = 1\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS *.wf03.wt01.*;";
    if (before) {
      expected =
          "Columns:\n"
              + "+--------------------+--------+\n"
              + "|                Path|DataType|\n"
              + "+--------------------+--------+\n"
              + "|nt.wf03.wt01.status2|    LONG|\n"
              + "+--------------------+--------+\n"
              + "Total line number = 1\n";
    } else { // 添加schemaPrefix为p1，dataPrefix为nt.wf03的数据源
      expected =
          "Columns:\n"
              + "+-----------------------+--------+\n"
              + "|                   Path|DataType|\n"
              + "+-----------------------+--------+\n"
              + "|   nt.wf03.wt01.status2|    LONG|\n"
              + "|p1.nt.wf03.wt01.status2|    LONG|\n"
              + "+-----------------------+--------+\n"
              + "Total line number = 2\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  protected void testShowColumnsRemoveStorageEngine(boolean before) {
    String statement = "SHOW COLUMNS p1.*, p2.*, p3.*;";
    String expected;
    if (before) {
      expected =
          "Columns:\n"
              + "+---------------------------+--------+\n"
              + "|                       Path|DataType|\n"
              + "+---------------------------+--------+\n"
              + "|    p1.nt.wf03.wt01.status2|    LONG|\n"
              + "|    p2.nt.wf03.wt01.status2|    LONG|\n"
              + "|    p3.nt.wf03.wt01.status2|    LONG|\n"
              + "|p3.nt.wf04.wt01.temperature|  DOUBLE|\n"
              + "+---------------------------+--------+\n"
              + "Total line number = 4\n";
    } else { // schemaPrefix为p2及p3，dataPrefix为nt.wf03的数据源被移除
      expected =
          "Columns:\n"
              + "+---------------------------+--------+\n"
              + "|                       Path|DataType|\n"
              + "+---------------------------+--------+\n"
              + "|    p1.nt.wf03.wt01.status2|    LONG|\n"
              + "|p3.nt.wf04.wt01.temperature|  DOUBLE|\n"
              + "+---------------------------+--------+\n"
              + "Total line number = 2\n";
    }
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  private void testShowClusterInfo(int expected) {
    try {
      ClusterInfo clusterInfo = session.getClusterInfo();
      assertEquals(expected, clusterInfo.getStorageEngineInfos().size());
    } catch (SessionException e) {
      LOGGER.error("encounter error when showing cluster info: ", e);
    }
  }

}
