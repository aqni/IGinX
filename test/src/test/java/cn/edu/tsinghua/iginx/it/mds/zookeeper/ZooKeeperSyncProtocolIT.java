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

package cn.edu.tsinghua.iginx.it.mds.zookeeper;

import cn.edu.tsinghua.iginx.it.env.metadata.MetadataEnv;
import cn.edu.tsinghua.iginx.it.env.metadata.zookeeper.ZookeeperEnv;
import cn.edu.tsinghua.iginx.it.mds.BaseSyncProtocolIT;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.SyncProtocol;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ZooKeeperSyncProtocolIT extends BaseSyncProtocolIT {

  private static final MetadataEnv metadataEnv = new ZookeeperEnv();

  @BeforeClass
  public static void beforeClass() throws Exception {
    metadataEnv.init();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    metadataEnv.clean();
  }

  @Override
  protected SyncProtocol newSyncProtocol(String category) throws Exception {
    return metadataEnv.newSyncProtocol(category);
  }
}
