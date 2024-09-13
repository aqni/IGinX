package cn.edu.tsinghua.iginx.it.expansion;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.it.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineInfo;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.it.expansion.constant.Constant.*;
import static cn.edu.tsinghua.iginx.it.expansion.constant.Constant.READ_ONLY_VALUES_LIST;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class BaseHasExpandReadOnlyIT extends BaseExpansionIT {

  private final String READ_ONLY_SCHEMA_PREFIX = null;
  protected static final String ALTER_ENGINE_STRING = "alter storageengine %d with params \"%s\";";
  protected List<String> wrongExtraParams = new ArrayList<>();
  protected Map<String, String> updatedParams = new HashMap<>();
  protected static final String updateParamsScriptDir = ".github/scripts/dataSources/update/";

  protected BaseHasExpandReadOnlyIT() {
    super(true, true);
  }

  @Order(3)
  @Test
  public void testExpandReadOnly() throws SessionException {
    super.testUpdateEngineParams();
    // 测试参数错误的只读节点扩容
    testInvalidDummyParams(readOnlyPort, true, true, null, READ_ONLY_SCHEMA_PREFIX);
    // 扩容只读节点
    addStorageEngineInProgress(readOnlyPort, true, true, null, READ_ONLY_SCHEMA_PREFIX);
    super.testQueryHistoryDataReadOnly();
  }

  @Order(3)
  @Test
  public void testAddStorageEngines(){
    // 测试参数错误的可写节点扩容
    testInvalidDummyParams(expPort, true, false, null, EXP_SCHEMA_PREFIX);
    addStorageEngineInProgress(expPort, true, false, null, EXP_SCHEMA_PREFIX);
  }


  @Order(7)
  @Test
  public void testReadOnly(){
    testQuerySpecialHistoryData();

    // 扩容后show columns测试
    testShowColumns();

    // clear data first, because history generator cannot append. It can only write
    clearData();
    generator.clearHistoryData();

    // 向三个dummy数据库中追加dummy数据，数据的key和列名都在添加数据库时的范围之外
    generator.writeExtendDummyData();
    // 能查到key在初始范围外的数据，查不到列名在初始范围外的数据
    queryExtendedKeyDummy();
    queryExtendedColDummy();
  }

  /**
   * 这个方法需要实现：通过脚本修改port对应数据源的可变参数，如密码等
   */
  protected abstract void updateParams(int port);

  /**
   * 这个方法需要实现：通过脚本恢复updateParams中修改的可变参数
   */
  protected abstract void restoreParams(int port);

  @Test
  protected abstract void testQuerySpecialHistoryData();

  /**
   * 测试引擎修改参数（目前仅支持dummy & read-only）
   */
  protected void testUpdateEngineParams() throws SessionException {
    // 修改前后通过相同schema_prefix查询判断引擎成功更新
    LOGGER.info("Testing updating engine params...");
    if (updatedParams.isEmpty()) {
      LOGGER.info("Engine {} skipped this test.", type);
      return;
    }

    String prefix = "prefix";
    // 添加只读节点
    addStorageEngine(readOnlyPort, true, true, null, prefix, extraParams);
    // 查询
    String statement = "select wt01.status, wt01.temperature from " + prefix + ".tm.wf05;";
    List<String> pathList =
        READ_ONLY_PATH_LIST.stream().map(s -> prefix + "." + s).collect(Collectors.toList());
    List<List<Object>> valuesList = READ_ONLY_VALUES_LIST;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 修改数据库参数
    updateParams(readOnlyPort);

    // 修改
    List<StorageEngineInfo> engineInfoList = session.getClusterInfo().getStorageEngineInfos();
    long id = -1;
    for (StorageEngineInfo info : engineInfoList) {
      if (info.getIp().equals("127.0.0.1")
          && info.getPort() == readOnlyPort
          && info.getDataPrefix().equals("null")
          && info.getSchemaPrefix().equals(prefix)
          && info.getType().equals(type)) {
        id = info.getId();
      }
    }
    assertTrue(id != -1);

    String newParams =
        updatedParams.entrySet().stream()
            .map(entry -> entry.getKey() + ":" + entry.getValue())
            .collect(Collectors.joining(", "));
    session.executeSql(String.format(ALTER_ENGINE_STRING, id, newParams));

    // 重新查询
    statement = "select wt01.status, wt01.temperature from " + prefix + ".tm.wf05;";
    pathList = READ_ONLY_PATH_LIST.stream().map(s -> prefix + "." + s).collect(Collectors.toList());
    valuesList = READ_ONLY_VALUES_LIST;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

    // 删除，不影响后续测试
    session.removeHistoryDataSource(
        Collections.singletonList(
            new RemovedStorageEngineInfo("127.0.0.1", readOnlyPort, prefix, "")));

    // 改回数据库参数
    restoreParams(readOnlyPort);
  }

  protected void testInvalidDummyParams(
      int port, boolean hasData, boolean isReadOnly, String dataPrefix, String schemaPrefix) {
    // wrong params
    String res;
    for (String params : wrongExtraParams) {
      res = addStorageEngine(port, hasData, isReadOnly, dataPrefix, schemaPrefix, params);
      if (res != null) {
        LOGGER.info(
            "Successfully rejected dummy engine with wrong params: {}; {}. msg: {}",
            port,
            params,
            res);
      } else {
        LOGGER.error("Dummy engine with wrong params {}; {} shouldn't be added.", port, params);
        fail();
      }
    }

    // wrong port
    res = addStorageEngine(port + 999, hasData, isReadOnly, dataPrefix, schemaPrefix, extraParams);
    if (res != null) {
      LOGGER.info(
          "Successfully rejected dummy engine with wrong port: {}; params: {}. msg: {}",
          port + 999,
          extraParams,
          res);
    } else {
      LOGGER.error(
          "Dummy engine with wrong port {} & params:{} shouldn't be added.",
          port + 999,
          extraParams);
      fail();
    }
  }

  protected void queryExtendedKeyDummy() {
    // ori
    // extended key queryable
    // NOTE: in some database(e.g. mongoDB), the key for dummy data is given randomly and cannot be
    // controlled. Thus, when extended value can be queried without specifying key filter,
    // we still assume that dummy key range is extended.
    String statement = "select wf01.wt01.status, wf01.wt01.temperature from mn;";
    SQLTestTools.executeAndContainValue(session, statement, ORI_PATH_LIST, ORI_EXTEND_VALUES_LIST);

    // exp
    statement = "select wf03.wt01.status2 from nt;";
    SQLTestTools.executeAndContainValue(
        session, statement, EXP_PATH_LIST1, EXP_EXTEND_VALUES_LIST1);
    statement = "select wf04.wt01.temperature from nt;";
    SQLTestTools.executeAndContainValue(
        session, statement, EXP_PATH_LIST2, EXP_EXTEND_VALUES_LIST2);

    // ro
    statement = "select wf05.wt01.status, wf05.wt01.temperature from tm;";
    SQLTestTools.executeAndContainValue(
        session, statement, READ_ONLY_PATH_LIST, READ_ONLY_EXTEND_VALUES_LIST);
  }

  protected void queryExtendedColDummy() {
    // ori
    // extended columns unreachable
    String statement = "select * from a.a.a;";
    SQLTestTools.executeAndCompare(session, statement, new ArrayList<>(), new ArrayList<>());

    // exp
    statement = "select * from a.a.b;";
    SQLTestTools.executeAndCompare(session, statement, new ArrayList<>(), new ArrayList<>());

    // ro
    statement = "select * from a.a.c;";
    SQLTestTools.executeAndCompare(session, statement, new ArrayList<>(), new ArrayList<>());
  }

  protected void testQueryHistoryDataReadOnly() {
    String statement = "select wt01.status, wt01.temperature from tm.wf05;";
    List<String> pathList = READ_ONLY_PATH_LIST;
    List<List<Object>> valuesList = READ_ONLY_VALUES_LIST;
    SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
  }

  // test dummy and non-dummy columns, in read only test
  public void testShowColumns() {
    String statement = "SHOW COLUMNS mn.*;";
    String expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|     mn.wf01.wt01.status|    LONG|\n"
            + "|mn.wf01.wt01.temperature|  DOUBLE|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS nt.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|    nt.wf03.wt01.status2|    LONG|\n"
            + "|nt.wf04.wt01.temperature|  DOUBLE|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);

    statement = "SHOW COLUMNS tm.*;";
    expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|     tm.wf05.wt01.status|    LONG|\n"
            + "|tm.wf05.wt01.temperature|  DOUBLE|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(session, statement, expected);
  }

  // test dummy query for data out of initial key range (should be visible)
  protected void testDummyKeyRange() throws SessionException {
    String statement;
    statement = "select * from mn where key < 1;";
    String expected =
        "Columns:\n"
            + "+------------------------+--------+\n"
            + "|                    Path|DataType|\n"
            + "+------------------------+--------+\n"
            + "|     mn.wf01.wt01.status|  BINARY|\n"
            + "|mn.wf01.wt01.temperature|  BINARY|\n"
            + "+------------------------+--------+\n"
            + "Total line number = 2\n";
    SQLTestTools.executeAndCompare(getSession(), statement, expected);
  }

}
