/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.RestUtils;
import cn.edu.tsinghua.iginx.rest.bean.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryAggregatorPercentile extends QueryAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryAggregatorPercentile.class);

  public QueryAggregatorPercentile() {
    super(QueryAggregatorType.PERCENTILE);
  }

  @Override
  public QueryResultDataset doAggregate(
      RestSession session,
      List<String> paths,
      List<Map<String, List<String>>> tagList,
      long startKey,
      long endKey) {
    QueryResultDataset queryResultDataset = new QueryResultDataset();
    try {
      SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startKey, endKey, tagList);
      queryResultDataset.setPaths(getPathsFromSessionQueryDataSet(sessionQueryDataSet));
      DataType type = RestUtils.checkType(sessionQueryDataSet);
      int n = sessionQueryDataSet.getKeys().length;
      int m = sessionQueryDataSet.getPaths().size();
      int datapoints = 0;

      List<Number> tmp = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
          Number value = (Number) sessionQueryDataSet.getValues().get(i).get(j);
          if (value != null) {
            datapoints++;
            tmp.add(value);
          }
        }
        if (i == n - 1
            || RestUtils.getInterval(sessionQueryDataSet.getKeys()[i], startKey, getDur())
                != RestUtils.getInterval(
                    sessionQueryDataSet.getKeys()[i + 1], startKey, getDur())) {
          switch (type) {
            case LONG:
              tmp.sort(Comparator.comparingLong(Number::longValue));
              break;
            case DOUBLE:
              tmp.sort(Comparator.comparingDouble(Number::doubleValue));
              break;
          }
          queryResultDataset.add(
              RestUtils.getIntervalStart(sessionQueryDataSet.getKeys()[i], startKey, getDur()),
              tmp.get((int) Math.floor(getPercentile() * (tmp.size() - 1))));
          tmp = new ArrayList<>();
        }
      }

      queryResultDataset.setSampleSize(datapoints);
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
    }
    return queryResultDataset;
  }
}
