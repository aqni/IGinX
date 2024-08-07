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

package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;

public class CollectionWriter extends ExportWriter {

  private BatchData collectedData;

  @Override
  public void write(BatchData batchData) {
    if (collectedData == null) {
      collectedData = new BatchData(batchData.getHeader());
    }
    for (Row row : batchData.getRowList()) {
      collectedData.appendRow(row);
    }
  }

  public BatchData getCollectedData() {
    return collectedData;
  }

  @Override
  public void reset() {
    collectedData = null;
  }
}
