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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.thrift.IService;

public abstract class AbstractFunctionClient {

  protected final IginXClientImpl iginXClient;

  protected final IService.Iface client;

  protected final long sessionId;

  public AbstractFunctionClient(IginXClientImpl iginXClient) {
    this.iginXClient = iginXClient;
    this.client = iginXClient.getClient();
    this.sessionId = iginXClient.getSessionId();
  }
}
