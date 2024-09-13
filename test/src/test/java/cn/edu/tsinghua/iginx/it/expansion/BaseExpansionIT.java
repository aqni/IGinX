/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.it.expansion;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.it.func.Controller;
import cn.edu.tsinghua.iginx.it.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static cn.edu.tsinghua.iginx.it.expansion.constant.Constant.*;

public abstract class BaseExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseExpansionIT.class);

  protected final boolean originHasData;
  protected final boolean expandedHasData;

  protected BaseExpansionIT(boolean originHasData, boolean expandedHasData) {
    this.originHasData = originHasData;
    this.expandedHasData = expandedHasData;
  }

  protected abstract Session getSession() throws SessionException;

  protected abstract StorageEngine getExpandedStorageEngine();

  //////////////////////////////////////////////////////////////////////////////
  ////////////////// test query origin history data ////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  @Order(100)
  @Test
  public void testOriginHasHistoryData() throws SessionException {
    Assumptions.assumeTrue(originHasData);

    String statement = "select wf01.wt01.status, wf01.wt01.temperature from mn;";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(getSession(), statement, expect);
  }

  @Order(100)
  @Test
  public void testOriginNoHistoryData() throws SessionException {
    Assumptions.assumeFalse(originHasData);

    String statement = "select wf01.wt01.status, wf01.wt01.temperature from mn;";
    List<String> pathList = ORI_PATH_LIST;
    List<List<Object>> valuesList = ORI_VALUES_LIST;
    SQLTestTools.executeAndCompare(getSession(), statement, pathList, valuesList);
  }

  //////////////////////////////////////////////////////////////////////////////
  ////////////////// test write new data ///////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  @Order(200)
  @Test
  public void testWriteAndQueryNewData() {
    try {
      getSession().executeSql("insert into ln.wf02 (key, status, version) values (100, true, \"v1\");");
      getSession().executeSql("insert into ln.wf02 (key, status, version) values (400, false, \"v4\");");
      getSession().executeSql("insert into ln.wf02 (key, version) values (800, \"v8\");");
      queryNewData();
    } catch (SessionException e) {
      LOGGER.error("insert new data error: ", e);
    }
  }

  protected void queryNewData() throws SessionException {
    String statement = "select * from ln;";
    String expect =
        "ResultSets:\n"
            + "+---+--------------+---------------+\n"
            + "|key|ln.wf02.status|ln.wf02.version|\n"
            + "+---+--------------+---------------+\n"
            + "|100|          true|             v1|\n"
            + "|400|         false|             v4|\n"
            + "|800|          null|             v8|\n"
            + "+---+--------------+---------------+\n"
            + "Total line number = 3\n";
    SQLTestTools.executeAndCompare(getSession(), statement, expect);

    statement = "select count(*) from ln.wf02;";
    expect =
        "ResultSets:\n"
            + "+---------------------+----------------------+\n"
            + "|count(ln.wf02.status)|count(ln.wf02.version)|\n"
            + "+---------------------+----------------------+\n"
            + "|                    2|                     3|\n"
            + "+---------------------+----------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(getSession(), statement, expect);
  }

  //////////////////////////////////////////////////////////////////////////////
  ////////////////// test capacity expansion ///////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  @Order(300)
  @Test
  public void testExpandCapacity() throws SessionException {
    getSession().addStorageEngines(Collections.singletonList(getExpandedStorageEngine()));
  }

  //////////////////////////////////////////////////////////////////////////////
  ////////////////// test query expanded history data //////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  @Order(400)
  @Test
  protected void testExpandedHasHistoryData() throws SessionException {
    Assumptions.assumeTrue(expandedHasData);

    String statement = "select wt01.status2 from nt.wf03;";
    List<String> pathList = EXP_PATH_LIST1;
    List<List<Object>> valuesList = EXP_VALUES_LIST1;
    SQLTestTools.executeAndCompare(getSession(), statement, pathList, valuesList);

    statement = "select wt01.temperature from nt.wf04;";
    pathList = EXP_PATH_LIST2;
    valuesList = EXP_VALUES_LIST2;
    SQLTestTools.executeAndCompare(getSession(), statement, pathList, valuesList);
  }

  @Order(400)
  @Test
  public void testExpandedNoHistoryData() throws SessionException {
    Assumptions.assumeFalse(expandedHasData);

    String statement = "select wf03.wt01.status, wf04.wt01.temperature from nt;";
    String expect =
        "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
    SQLTestTools.executeAndCompare(getSession(), statement, expect);
  }

  //////////////////////////////////////////////////////////////////////////////
  ////////////////// test query new data after capacity expansion //////////////
  //////////////////////////////////////////////////////////////////////////////

  @Order(500)
  @Test
  public void testQueryNewDataAfterCE() throws SessionException {
    queryNewData();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////////////////// test write new data after capacity expansion //////////////
  //////////////////////////////////////////////////////////////////////////////

  @Order(600)
  @Test
  public void testWriteAndQueryNewDataAfterCE() throws SessionException {
    getSession().executeSql("insert into ln.wf02 (key, version) values (1600, \"v48\");");
    queryAllNewData();
  }

  private void queryAllNewData() throws SessionException {
    String statement = "select * from ln;";
    String expect =
        "ResultSets:\n"
            + "+----+--------------+---------------+\n"
            + "| key|ln.wf02.status|ln.wf02.version|\n"
            + "+----+--------------+---------------+\n"
            + "| 100|          true|             v1|\n"
            + "| 400|         false|             v4|\n"
            + "| 800|          null|             v8|\n"
            + "|1600|          null|            v48|\n"
            + "+----+--------------+---------------+\n"
            + "Total line number = 4\n";
    SQLTestTools.executeAndCompare(getSession(), statement, expect);

    statement = "select count(*) from ln.wf02;";
    expect =
        "ResultSets:\n"
            + "+---------------------+----------------------+\n"
            + "|count(ln.wf02.status)|count(ln.wf02.version)|\n"
            + "+---------------------+----------------------+\n"
            + "|                    2|                     4|\n"
            + "+---------------------+----------------------+\n"
            + "Total line number = 1\n";
    SQLTestTools.executeAndCompare(getSession(), statement, expect);
  }

  //////////////////////////////////////////////////////////////////////////////
  ////////////////// test normal IT ////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////

  // no order annotation means this test will be executed last
  @Test
  public void testNormalIT() throws IOException {
    Controller.testNormalIT();
  }

}
