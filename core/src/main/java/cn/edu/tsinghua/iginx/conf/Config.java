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
package cn.edu.tsinghua.iginx.conf;

import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class Config {

  private String ip = "0.0.0.0";

  private int port = 6888;

  private String username = "root";

  private String password = "root";

  private String metaStorage = "zookeeper";

  private String zookeeperConnectionString = "127.0.0.1:2181";

  private String storageEngineList =
      "127.0.0.1#6667#iotdb12#username=root#password=root#sessionPoolSize=20#dataDir=/path/to/your/data/";

  private String defaultUDFDir = "udf_funcs";

  private int maxAsyncRetryTimes = 2;

  private int syncExecuteThreadPool = 60;

  private int asyncExecuteThreadPool = 20;

  private int replicaNum = 0;

  private TimePrecision timePrecision = TimePrecision.NS;

  private String databaseClassNames =
      "iotdb12=cn.edu.tsinghua.iginx.iotdb.IoTDBStorage,influxdb=cn.edu.tsinghua.iginx.influxdb.InfluxDBStorage,relational=cn.edu.tsinghua.iginx.relational.RelationalStorage,mongodb=cn.edu.tsinghua.iginx.mongodb.MongoDBStorage,redis=cn.edu.tsinghua.iginx.redis.RedisStorage,filestore=cn.edu.tsinghua.iginx.filestore.FileStorage";

  private String policyClassName = "cn.edu.tsinghua.iginx.policy.naive.NaivePolicy";

  private boolean enableMonitor = false;

  private int loadBalanceCheckInterval = 3;

  private boolean enableFragmentCompaction = false;

  private boolean enableInstantCompaction = false; // 启动即时分片合并，仅用于测试

  private long fragmentCompactionWriteThreshold = 1000;

  private long fragmentCompactionReadThreshold = 1000;

  private double fragmentCompactionReadRatioThreshold = 0.1;

  private long reshardFragmentTimeMargin = 60;

  private String migrationPolicyClassName = "cn.edu.tsinghua.iginx.migration.GreedyMigrationPolicy";

  private long migrationBatchSize = 100;

  private int maxReshardFragmentsNum = 3;

  private double maxTimeseriesLoadBalanceThreshold = 2;

  private String statisticsCollectorClassName = "";

  private int statisticsLogInterval = 5000;

  private boolean enableEnvParameter = false;

  private String restIp = "127.0.0.1";

  private int restPort = 6666;

  private int maxTimeseriesLength = 10;

  private long disorderMargin = 10;

  private int asyncRestThreadPool = 100;

  private boolean enableRestService = true;

  private String etcdEndpoints = "http://localhost:2379";

  private boolean enableMQTT = false;

  private String mqttHost = "0.0.0.0";

  private int mqttPort = 1883;

  private int mqttHandlerPoolSize = 1;

  private String mqttPayloadFormatter = "cn.edu.tsinghua.iginx.mqtt.JsonPayloadFormatter";

  private int mqttMaxMessageSize = 1048576;

  private String clients = "";

  private int instancesNumPerClient = 0;

  private String queryOptimizer = "";

  private String constraintChecker = "naive";

  private String physicalOptimizer = "naive";

  private int memoryTaskThreadPoolSize = 200;

  private int physicalTaskThreadPoolSizePerStorage = 100;

  private int maxCachedPhysicalTaskPerStorage = 500;

  private double cachedTimeseriesProb = 0.01;

  private int retryCount = 10;

  private int retryWait = 5000;

  private int reAllocatePeriod = 30000;

  private int fragmentPerEngine = 10;

  private boolean enableStorageGroupValueLimit = true;

  private double storageGroupValueLimit = 200.0;

  private boolean enablePushDown = true;

  private boolean useStreamExecutor = true;

  private boolean enableMemoryControl = true;

  private String systemResourceMetrics = "default";

  private double heapMemoryThreshold = 0.9;

  private double systemMemoryThreshold = 0.9;

  private double systemCpuThreshold = 0.9;

  private boolean enableMetaCacheControl = false;

  private long fragmentCacheThreshold = 1024 * 128;

  private int batchSize = 50;

  private String pythonCMD = "python3";

  private int transformTaskThreadPoolSize = 10;

  private int transformMaxRetryTimes = 3;

  private boolean needInitBasicUDFFunctions = true;

  private List<String> udfList = new ArrayList<>();

  private String historicalPrefixList = "";

  private int expectedStorageUnitNum = 0;

  private int minThriftWorkerThreadNum = 20;

  private int maxThriftWrokerThreadNum = 2147483647;

  private String ruleBasedOptimizer =
      "NotFilterRemoveRule=on,FragmentPruningByFilterRule=on,ColumnPruningRule=on,FragmentPruningByPatternRule=on";

  //////////////

  public static final String tagNameAnnotation = TagKVUtils.tagNameAnnotation;

  public static final String tagPrefix = TagKVUtils.tagPrefix;

  public static final String tagSuffix = TagKVUtils.tagSuffix;

  /////////////

  private int parallelFilterThreshold = 10000;

  private int parallelGroupByRowsThreshold = 10000;

  private int parallelApplyFuncGroupsThreshold = 1000;

  private int parallelGroupByPoolSize = 5;

  private int parallelGroupByPoolNum = 5;

  private int streamParallelGroupByWorkerNum = 5;

  /////////////

  private boolean enableEmailNotification = false;

  private String mailSmtpHost = "";

  private int mailSmtpPort = 465;

  private String mailSmtpUser = "";

  private String mailSmtpPassword = "";

  private String mailSender = "";

  private String mailRecipient = "";

  /////////////

  private int batchSizeImportCsv = 10000;

  private boolean UTTestEnv = false; // 是否是单元测试环境
}
