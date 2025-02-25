/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.CLEAR_DUMMY_DATA_CAUTION;
import static cn.edu.tsinghua.iginx.constant.GlobalConstant.KEY_NAME;
import static cn.edu.tsinghua.iginx.utils.StringUtils.replaceSpecialCharsWithUnderscore;
import static cn.edu.tsinghua.iginx.utils.StringUtils.tryParse2Key;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.constraint.ConstraintChecker;
import cn.edu.tsinghua.iginx.engine.logical.constraint.ConstraintCheckerManager;
import cn.edu.tsinghua.iginx.engine.logical.generator.*;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.VectorSchemaRoots;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskInfoVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.engine.shared.file.FileType;
import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportCsv;
import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportFile;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportByteStream;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportCsv;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportFile;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.OperatorInfoVisitor;
import cn.edu.tsinghua.iginx.engine.shared.processor.*;
import cn.edu.tsinghua.iginx.exception.StatusCode;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.resource.ResourceManager;
import cn.edu.tsinghua.iginx.resource.ResourceSet;
import cn.edu.tsinghua.iginx.resource.exception.ResourceException;
import cn.edu.tsinghua.iginx.sql.exception.SQLParserException;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement;
import cn.edu.tsinghua.iginx.statistics.IStatisticsCollector;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeInferenceUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import cn.hutool.core.io.CharsetDetector;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatementExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatementExecutor.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final StatementBuilder builder = StatementBuilder.getInstance();

  private static final PhysicalEngine engine = PhysicalEngineImpl.getInstance();

  private static final ConstraintChecker checker =
      ConstraintCheckerManager.getInstance().getChecker(config.getConstraintChecker());
  private static final ConstraintManager constraintManager = engine.getConstraintManager();

  private static final ResourceManager resourceManager = ResourceManager.getInstance();

  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static final Map<StatementType, List<LogicalGenerator>> generatorMap = new HashMap<>();

  private static final List<LogicalGenerator> queryGeneratorList = new ArrayList<>();
  private static final List<LogicalGenerator> deleteGeneratorList = new ArrayList<>();
  private static final List<LogicalGenerator> insertGeneratorList = new ArrayList<>();
  private static final List<LogicalGenerator> showTSGeneratorList = new ArrayList<>();

  private final List<PreParseProcessor> preParseProcessors = new ArrayList<>();
  private final List<PostParseProcessor> postParseProcessors = new ArrayList<>();
  private final List<PreLogicalProcessor> preLogicalProcessors = new ArrayList<>();
  private final List<PostLogicalProcessor> postLogicalProcessors = new ArrayList<>();
  private final List<PrePhysicalProcessor> prePhysicalProcessors = new ArrayList<>();
  private final List<PostPhysicalProcessor> postPhysicalProcessors = new ArrayList<>();
  private final List<PreExecuteProcessor> preExecuteProcessors = new ArrayList<>();
  private final List<PostExecuteProcessor> postExecuteProcessors = new ArrayList<>();

  private static class StatementExecutorHolder {

    private static final StatementExecutor instance = new StatementExecutor();
  }

  static {
    generatorMap.put(StatementType.SELECT, queryGeneratorList);
    generatorMap.put(StatementType.DELETE, deleteGeneratorList);
    generatorMap.put(StatementType.INSERT, insertGeneratorList);
    generatorMap.put(StatementType.SHOW_COLUMNS, showTSGeneratorList);
  }

  private StatementExecutor() {
    registerGenerator(QueryGenerator.getInstance());
    registerGenerator(DeleteGenerator.getInstance());
    registerGenerator(InsertGenerator.getInstance());
    registerGenerator(ShowColumnsGenerator.getInstance());

    try {
      String statisticsCollectorClassName =
          ConfigDescriptor.getInstance().getConfig().getStatisticsCollectorClassName();
      if (statisticsCollectorClassName != null && !statisticsCollectorClassName.equals("")) {
        Class<? extends IStatisticsCollector> statisticsCollectorClass =
            StatementExecutor.class
                .getClassLoader()
                .loadClass(statisticsCollectorClassName)
                .asSubclass(IStatisticsCollector.class);
        IStatisticsCollector statisticsCollector =
            statisticsCollectorClass.getConstructor().newInstance();
        registerPreParseProcessor(statisticsCollector.getPreParseProcessor());
        registerPostParseProcessor(statisticsCollector.getPostParseProcessor());
        registerPreLogicalProcessor(statisticsCollector.getPreLogicalProcessor());
        registerPostLogicalProcessor(statisticsCollector.getPostLogicalProcessor());
        registerPrePhysicalProcessor(statisticsCollector.getPrePhysicalProcessor());
        registerPostPhysicalProcessor(statisticsCollector.getPostPhysicalProcessor());
        registerPreExecuteProcessor(statisticsCollector.getPreExecuteProcessor());
        registerPostExecuteProcessor(statisticsCollector.getPostExecuteProcessor());
        statisticsCollector.startBroadcasting();
      }
    } catch (ClassNotFoundException
        | InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      LOGGER.error("initial statistics collector error: ", e);
    }
  }

  public static StatementExecutor getInstance() {
    return StatementExecutorHolder.instance;
  }

  public void registerGenerator(LogicalGenerator generator) {
    if (generator != null) {
      switch (generator.getType()) {
        case Query:
          queryGeneratorList.add(generator);
          break;
        case Delete:
          deleteGeneratorList.add(generator);
          break;
        case Insert:
          insertGeneratorList.add(generator);
          break;
        case ShowColumns:
          showTSGeneratorList.add(generator);
          break;
        default:
          throw new IllegalArgumentException("unknown generator type");
      }
    }
  }

  public void registerPreParseProcessor(PreParseProcessor processor) {
    if (processor != null) {
      preParseProcessors.add(processor);
    }
  }

  public void registerPostParseProcessor(PostParseProcessor processor) {
    if (processor != null) {
      postParseProcessors.add(processor);
    }
  }

  public void registerPreLogicalProcessor(PreLogicalProcessor processor) {
    if (processor != null) {
      preLogicalProcessors.add(processor);
    }
  }

  public void registerPostLogicalProcessor(PostLogicalProcessor processor) {
    if (processor != null) {
      postLogicalProcessors.add(processor);
    }
  }

  public void registerPrePhysicalProcessor(PrePhysicalProcessor processor) {
    if (processor != null) {
      prePhysicalProcessors.add(processor);
    }
  }

  public void registerPostPhysicalProcessor(PostPhysicalProcessor processor) {
    if (processor != null) {
      postPhysicalProcessors.add(processor);
    }
  }

  public void registerPreExecuteProcessor(PreExecuteProcessor processor) {
    if (processor != null) {
      preExecuteProcessors.add(processor);
    }
  }

  public void registerPostExecuteProcessor(PostExecuteProcessor processor) {
    if (processor != null) {
      postExecuteProcessors.add(processor);
    }
  }

  public void execute(RequestContext ctx) {
    execute(ctx, true);
  }

  public void execute(RequestContext ctx, boolean releaseResources) {
    ResourceSet holder = null;
    try {
      holder = resourceManager.setup(ctx);
      before(ctx, preExecuteProcessors);
      if (ctx.isFromSQL()) {
        executeSQL(ctx);
      } else {
        executeStatement(ctx);
      }
      after(ctx, postExecuteProcessors);
    } catch (ResourceException e) {
      ctx.setResult(new Result(e.getStatus()));
      if (ctx.isFromSQL()) {
        ctx.getResult().setSqlType(ctx.getSqlType());
      }
    } finally {
      if (holder != null && releaseResources) {
        try {
          holder.close();
        } catch (PhysicalException e) {
          ctx.setResult(
              new Result(RpcUtils.status(StatusCode.STATEMENT_EXECUTION_ERROR, e.toString())));
          LOGGER.debug("statement execution failed: ", e);
          if (ctx.isFromSQL()) {
            ctx.getResult().setSqlType(ctx.getSqlType());
          }
        }
      }
    }
  }

  public void executeSQL(RequestContext ctx) {
    try {
      before(ctx, preParseProcessors);
      builder.buildFromSQL(ctx);
      after(ctx, postParseProcessors);
      executeStatement(ctx);
    } catch (SQLParserException | ParseCancellationException e) {
      StatusCode statusCode = StatusCode.STATEMENT_PARSE_ERROR;
      ctx.setResult(new Result(RpcUtils.status(statusCode, e.getMessage())));
    } catch (Exception e) {
      LOGGER.error("unexpected error: ", e);
      StatusCode statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
      String errMsg =
          "Execute Error: encounter error(s) when executing sql statement, "
              + "see server log for more details.";
      ctx.setResult(new Result(RpcUtils.status(statusCode, errMsg)));
    } finally {
      if (ctx.getResult() != null) {
        ctx.getResult().setSqlType(ctx.getSqlType());
      }
    }
  }

  public void executeStatement(RequestContext ctx) {
    try {
      if (ctx.isFromSQL()) {
        LOGGER.debug("Execute statement: {}", ctx.getSql());
      }
      Statement statement = ctx.getStatement();
      if (statement instanceof DataStatement) {
        StatementType type = statement.getType();
        switch (type) {
          case SELECT:
          case DELETE:
          case INSERT:
          case SHOW_COLUMNS:
            process(ctx);
            return;
          case INSERT_FROM_SELECT:
            processInsertFromSelect(ctx);
            return;
          case COUNT_POINTS:
            processCountPoints(ctx);
            return;
          case DELETE_COLUMNS:
            processDeleteColumns(ctx);
            return;
          case CLEAR_DATA:
            processClearData(ctx);
            return;
          case INSERT_FROM_CSV:
            processInsertFromFile(ctx);
            return;
          case EXPORT_CSV_FROM_SELECT:
          case EXPORT_STREAM_FROM_SELECT:
            processExportFileFromSelect(ctx);
            return;
          default:
            throw new StatementExecutionException(
                String.format("Execute Error: unknown statement type [%s].", type));
        }
      } else {
        ((SystemStatement) statement).execute(ctx);
      }
    } catch (StatementExecutionException | PhysicalException | IOException e) {
      if (e.getCause() != null) {
        LOGGER.error("Execute Error: ", e);
      } else {
        LOGGER.debug("statement execution failed: ", e);
      }
      StatusCode statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
      ctx.setResult(new Result(RpcUtils.status(statusCode, e.getMessage())));
    } catch (Exception e) {
      LOGGER.error(
          "unexpected exception during dispatcher memory task, please contact developer to check: ",
          e);
      StatusCode statusCode = StatusCode.SYSTEM_ERROR;
      ctx.setResult(new Result(RpcUtils.status(statusCode, e.getMessage())));
    }
  }

  private void process(RequestContext ctx) throws StatementExecutionException, PhysicalException {
    StatementType type = ctx.getStatement().getType();
    List<LogicalGenerator> generatorList = generatorMap.get(type);
    for (LogicalGenerator generator : generatorList) {
      before(ctx, preLogicalProcessors);
      Operator root = generator.generate(ctx);
      after(ctx, postLogicalProcessors);
      if (root == null && !metaManager.hasWritableStorageEngines()) {
        ctx.setResult(new Result(RpcUtils.SUCCESS));
        setResult(ctx, BatchStreams.empty());
        return;
      }
      if (constraintManager.check(root) && checker.check(root)) {
        if (type == StatementType.SELECT) {
          SelectStatement selectStatement = (SelectStatement) ctx.getStatement();
          if (selectStatement.isNeedLogicalExplain()) {
            processExplainLogicalStatement(ctx, root);
            return;
          }
        }

        before(ctx, prePhysicalProcessors);
        BatchStream stream = engine.execute(ctx, root);
        after(ctx, postPhysicalProcessors);

        if (type == StatementType.SELECT) {
          SelectStatement selectStatement = (SelectStatement) ctx.getStatement();
          if (selectStatement.isNeedPhysicalExplain()) {
            try (BatchStream ignore = stream) {
              while (stream.hasNext()) {
                stream.getNext().close();
              }
            }
            processExplainPhysicalStatement(ctx);
            return;
          }
        }

        setResult(ctx, stream);
        return;
      }
    }
    throw new StatementExecutionException("Execute Error: can not construct a legal logical tree.");
  }

  private void processExplainLogicalStatement(RequestContext ctx, Operator root)
      throws PhysicalException, StatementExecutionException {
    List<Field> fields =
        new ArrayList<>(
            Arrays.asList(
                new Field("Logical Tree", DataType.BINARY),
                new Field("Operator Type", DataType.BINARY),
                new Field("Operator Info", DataType.BINARY)));
    Header header = new Header(fields);

    OperatorInfoVisitor visitor = new OperatorInfoVisitor();
    root.accept(visitor);
    formatTree(ctx, header, visitor.getCache(), visitor.getMaxLen());
  }

  private void processExplainPhysicalStatement(RequestContext ctx)
      throws PhysicalException, StatementExecutionException {
    PhysicalTask root = ctx.getPhysicalTree();
    List<Field> fields =
        new ArrayList<>(
            Arrays.asList(
                new Field("Physical Tree", DataType.BINARY),
                new Field("Execute Time", DataType.BINARY),
                new Field("Task Type", DataType.BINARY),
                new Field("Task Info", DataType.BINARY),
                new Field("Affect Rows", DataType.LONG)));
    Header header = new Header(fields);

    TaskInfoVisitor visitor = new TaskInfoVisitor();
    root.accept(visitor);
    formatTree(ctx, header, visitor.getCache(), visitor.getMaxLen());
  }

  private void formatTree(RequestContext ctx, Header header, List<Object[]> cache, int maxLen)
      throws PhysicalException, StatementExecutionException {
    List<Row> rows = new ArrayList<>();
    for (Object[] rowValues : cache) {
      StringBuilder str = new StringBuilder(((String) rowValues[0]));
      while (str.length() < maxLen) {
        str.append(" ");
      }
      rowValues[0] = str.toString().getBytes();
      rows.add(new Row(header, rowValues));
    }
    RowStream stream = new Table(header, rows);
    BatchStream batchStream = BatchStreams.wrap(ctx.getAllocator(), stream, ctx.getBatchRowCount());
    setResult(ctx, batchStream);
  }

  private void processExportFileFromSelect(RequestContext ctx)
      throws StatementExecutionException, PhysicalException, IOException {
    ExportFileFromSelectStatement statement = (ExportFileFromSelectStatement) ctx.getStatement();

    // step 1: select stage
    SelectStatement selectStatement = statement.getSelectStatement();
    RequestContext selectContext = new RequestContext(ctx.getSessionId(), selectStatement, true);
    try {
      ResourceSet holder =
          resourceManager.setup(selectContext); // will be closed after exportation finished
      process(selectContext);
      BatchStream batchStream = selectContext.getResult().getBatchStream();

      // step 2: export file
      setResultFromBatchStream(ctx, batchStream);
      ExportFile exportFile = statement.getExportFile();
      switch (exportFile.getType()) {
        case CSV:
          ExportCsv exportCsv = (ExportCsv) exportFile;
          ctx.getResult().setExportCsv(exportCsv);
          break;
        case BYTE_STREAM:
          ExportByteStream exportByteStream = (ExportByteStream) exportFile;
          ctx.getResult().setExportByteStreamDir(exportByteStream.getDir());
          break;
        default:
          throw new RuntimeException("Unknown export file type: " + exportFile.getType());
      }
    } catch (ResourceException e) {
      LOGGER.error("Cannot setup resource for query", e);
      throw new StatementExecutionException(e);
    }
  }

  private void processInsertFromFile(RequestContext ctx)
      throws StatementExecutionException, PhysicalException, IOException {
    InsertFromCsvStatement statement = (InsertFromCsvStatement) ctx.getStatement();
    ImportFile importFile = statement.getImportFile();
    InsertStatement insertStatement = statement.getSubInsertStatement();

    if (Objects.requireNonNull(importFile.getType()) == FileType.CSV) {
      ImportCsv importCsv = (ImportCsv) importFile;
      if (ctx.getLoadCSVFileName() == null || ctx.getLoadCSVFileName().isEmpty()) {
        ctx.setResult(new Result(RpcUtils.SUCCESS));
        ctx.getResult().setLoadCSVPath(importCsv.getFilepath());
      } else {
        loadValuesSpecFromCsv(
            ctx,
            (ImportCsv) importFile,
            insertStatement,
            statement.getKeyBase(),
            statement.getKeyCol());
      }
    } else {
      throw new RuntimeException("Unknown import file type: " + importFile.getType());
    }
  }

  private void loadValuesSpecFromCsv(
      RequestContext ctx,
      ImportCsv importCsv,
      InsertStatement insertStatement,
      long keyBase,
      String keyCol)
      throws IOException {
    final int BATCH_SIZE = config.getBatchSizeImportCsv();
    String filepath =
        String.join(File.separator, System.getProperty("java.io.tmpdir"), ctx.getLoadCSVFileName());
    File tmpCSV = new File(filepath);
    long count = 0;
    LOGGER.info("Begin to load data from csv file: {}", tmpCSV.getCanonicalPath());
    try {
      CSVParser parser =
          importCsv
              .getCSVBuilder()
              .build()
              .parse(
                  new InputStreamReader(
                      Files.newInputStream(tmpCSV.toPath()), CharsetDetector.detect(tmpCSV)));

      CSVRecord tmp;
      Iterator<CSVRecord> iterator = parser.stream().iterator();
      // 跳过解析第一行
      if (importCsv.isSkippingImportHeader() && iterator.hasNext()) {
        tmp = iterator.next();
      }

      int pathSize = insertStatement.getPaths().size();
      // only when the first column in the file is KEY
      AtomicBoolean keyInFile = new AtomicBoolean(false);
      int keyIdx = 0;
      // 处理未给声明路径的情况
      if (pathSize == 0) {
        keyIdx = -1; // 未声明路径的情况，默认就是key列不存在
        // 从文件中读出列名来，并设置给insertStatement
        tmp = iterator.next();
        for (int i = 0; i < tmp.size(); i++) {
          String colName = replaceSpecialCharsWithUnderscore(tmp.get(i));
          if (colName.equalsIgnoreCase(KEY_NAME)) {
            keyInFile.set(true);
            if (keyCol == null) keyIdx = i;
          } else { // colName should only be taken as path when it is not called key
            insertStatement.setPath(colName, insertStatement.getGlobalTags());
          }
          if (keyCol != null && colName.equalsIgnoreCase(keyCol)) {
            keyIdx = i;
          }
        }
        if (keyCol != null) {
          if (keyInFile.get() && !keyCol.equalsIgnoreCase(KEY_NAME))
            throw new StatementExecutionException("Key columns conflict. Execution aborted.");
          if (keyIdx == -1)
            throw new StatementExecutionException(
                "The specified key column is not in file. Execution aborted.");
        }
        // update pathSize accordingly
        pathSize = insertStatement.getPaths().size();
      } else keyInFile.set(true);

      // sort by paths
      List<String> iPaths = insertStatement.getPaths();
      Integer[] idx = new Integer[iPaths.size()];
      for (int i = 0; i < iPaths.size(); i++) {
        idx[i] = i;
      }
      Arrays.sort(idx, Comparator.comparing(iPaths::get));
      Collections.sort(iPaths);

      int delta = keyInFile.get() && keyIdx == 0 ? 1 : 0;
      // type must be fixed once set, just like paths
      List<DataType> types = null;

      while (iterator.hasNext()) {
        long KeyStart = keyBase + count;
        List<CSVRecord> records = new ArrayList<>(BATCH_SIZE);
        // 每次从文件中取出BATCH_SIZE行数据
        for (int n = 0; n < BATCH_SIZE && iterator.hasNext(); n++) {
          tmp = iterator.next();
          // more values are OK; the extra ones are skipped
          if (tmp.size() < pathSize + delta) {
            throw new RuntimeException(
                "The paths' size doesn't match csv data at line: " + tmp.getRecordNumber());
          }
          records.add(tmp);
        }

        int recordsSize = records.size();
        Long[] keys = new Long[recordsSize];
        Object[][] values = new Object[recordsSize][pathSize];
        List<Bitmap> bitmaps = new ArrayList<>();

        // 填充 types
        // 类型推断一定可以在一个batch中完成
        if (types == null) {
          types = new ArrayList<>();
          Set<Integer> dataTypeIndex = new HashSet<>();
          for (int i = 0; i < pathSize; i++) {
            types.add(null);
          }
          for (int i = 0; i < pathSize; i++) {
            dataTypeIndex.add(i);
          }

          for (CSVRecord record : records) {
            if (dataTypeIndex.isEmpty()) {
              break;
            }
            for (int j = 0; j < pathSize; j++) {
              if (!dataTypeIndex.contains(j)) {
                continue;
              }
              DataType inferredDataType =
                  DataTypeInferenceUtils.getInferredDataType(record.get(j + delta));
              if (inferredDataType != null) { // 找到每一列第一个不为 null 的值进行类型推断
                types.set(j, inferredDataType);
                dataTypeIndex.remove(j);
              }
            }
          }
          if (!dataTypeIndex.isEmpty()) {
            for (Integer index : dataTypeIndex) {
              types.set(index, DataType.BINARY);
            }
          }
          // sort types by paths
          List<DataType> sortedDataTypeList = new ArrayList<>();
          for (int i = 0; i < idx.length; i++) {
            sortedDataTypeList.add(types.get(idx[i]));
          }
          types = sortedDataTypeList;
        }

        // 填充 keys, values 和 bitmaps
        for (int i = 0; i < recordsSize; i++) {
          CSVRecord record = records.get(i);
          if (keyInFile.get()) keys[i] = Long.parseLong(record.get(keyIdx)) + keyBase; // 指定了同名key列
          else if (keyIdx != -1) keys[i] = tryParse2Key(record.get(keyIdx)) + keyBase; // 指定了非同名key列
          else keys[i] = (long) i + KeyStart; // 需要自增key列
          Bitmap bitmap = new Bitmap(pathSize);

          // 按照排好序的列来处理
          for (int index = 0; index < pathSize; ) {
            int j = idx[index];
            if (!record.get(j + delta).equalsIgnoreCase("null")) {
              bitmap.mark(index);
              switch (types.get(index)) { // types已经排好序了
                case BOOLEAN:
                  values[i][index] = Boolean.parseBoolean(record.get(j + delta));
                  index++;
                  break;
                case INTEGER:
                  values[i][index] = Integer.parseInt(record.get(j + delta));
                  index++;
                  break;
                case LONG:
                  values[i][index] = Long.parseLong(record.get(j + delta));
                  index++;
                  break;
                case FLOAT:
                  values[i][index] = Float.parseFloat(record.get(j + delta));
                  index++;
                  break;
                case DOUBLE:
                  values[i][index] = Double.parseDouble(record.get(j + delta));
                  index++;
                  break;
                case BINARY:
                  values[i][index] = record.get(j + delta).getBytes();
                  index++;
                  break;
                default:
              }
            }
          }
          bitmaps.add(bitmap);
        }

        insertStatement.setKeys(new ArrayList<>(Arrays.asList(keys)));
        insertStatement.setValues(values);
        insertStatement.setTypes(types);
        insertStatement.setBitmaps(bitmaps);

        // do the actual insert
        LOGGER.info("Inserting {} rows, {} rows completed", recordsSize, count);
        RequestContext subInsertContext = new RequestContext(ctx.getSessionId(), insertStatement);
        process(subInsertContext);

        if (!subInsertContext.getResult().getStatus().equals(RpcUtils.SUCCESS)) {
          ctx.setResult(new Result(RpcUtils.FAILURE));
          return;
        }
        count += recordsSize;
      }
      ctx.setResult(new Result(RpcUtils.SUCCESS));
      ctx.getResult().setLoadCSVColumns(insertStatement.getPaths());
      ctx.getResult().setLoadCSVRecordNum(count);
    } catch (IOException e) {
      throw new RuntimeException(
          "Encounter an error when reading csv file "
              + tmpCSV.getCanonicalPath()
              + ", because "
              + e.getMessage());
    } catch (StatementExecutionException | PhysicalException e) {
      throw new RuntimeException(e);
    }

    Files.delete(tmpCSV.toPath());
  }

  private void processInsertFromSelect(RequestContext ctx)
      throws StatementExecutionException, PhysicalException {
    InsertFromSelectStatement statement = (InsertFromSelectStatement) ctx.getStatement();

    // step 1: select stage
    SelectStatement selectStatement = statement.getSubSelectStatement();
    RequestContext subSelectContext = new RequestContext(ctx.getSessionId(), selectStatement, true);
    try (ResourceSet holder = resourceManager.setup(subSelectContext)) {
      process(subSelectContext);

      BatchStream batchStream = subSelectContext.getResult().getBatchStream();

      // step 2: insert stage
      InsertStatement insertStatement = statement.getSubInsertStatement();
      parseOldTagsFromSchema(batchStream.getSchema(), insertStatement);
      parseInsertValuesSpecFromBatchStream(
          subSelectContext.getAllocator(), statement.getKeyOffset(), batchStream, insertStatement);
      RequestContext subInsertContext =
          new RequestContext(ctx.getSessionId(), insertStatement, ctx.isUseStream());
      try (ResourceSet holder2 = resourceManager.setup(subInsertContext)) {
        process(subInsertContext);
        ctx.setResult(subInsertContext.getResult());
      }
    } catch (ResourceException e) {
      LOGGER.error("Cannot setup resource for query", e);
      throw new StatementExecutionException(e);
    }
  }

  private void processCountPoints(RequestContext ctx)
      throws StatementExecutionException, PhysicalException {
    SelectStatement statement =
        new UnarySelectStatement(Collections.singletonList("*"), AggregateType.COUNT);
    ctx.setStatement(statement);
    process(ctx);

    long pointsNum = 0;
    ByteUtils.DataSet dataSet = ByteUtils.getDataFromArrowData(ctx.getResult().getArrowData());
    if (dataSet.getValues() != null) {
      Object[] row = dataSet.getValues().get(0).toArray();
      pointsNum = Arrays.stream(row).mapToLong(e -> (Long) e).sum();
    }

    ctx.getResult().setPointsNum(pointsNum);
  }

  private void processDeleteColumns(RequestContext ctx)
      throws StatementExecutionException, PhysicalException {
    DeleteColumnsStatement deleteColumnsStatement = (DeleteColumnsStatement) ctx.getStatement();
    DeleteStatement deleteStatement =
        new DeleteStatement(
            deleteColumnsStatement.getPaths(), deleteColumnsStatement.getTagFilter());
    ctx.setStatement(deleteStatement);
    process(ctx);
  }

  private void processClearData(RequestContext ctx)
      throws StatementExecutionException, PhysicalException {
    DeleteStatement deleteStatement = new DeleteStatement(Collections.singletonList("*"));
    ctx.setStatement(deleteStatement);
    process(ctx);
  }

  private void setEmptyQueryResp(RequestContext ctx) throws PhysicalException {
    Result result = new Result(RpcUtils.SUCCESS);
    BigIntVector vector =
        (BigIntVector)
            org.apache.arrow.vector.types.pojo.Field.notNullable(
                    KEY_NAME, Types.MinorType.BIGINT.getType())
                .createVector(ctx.getAllocator());
    vector.allocateNew(1);
    vector.setValueCount(1);
    vector.setSafe(0, 0);
    VectorSchemaRoot root = VectorSchemaRoots.create(Collections.singletonList(vector), 1);
    result.setArrowData(getBytesFromVector(root, ctx.getAllocator()));
    ctx.setResult(result);
  }

  private void setResult(RequestContext ctx, BatchStream stream)
      throws PhysicalException, StatementExecutionException {
    Statement statement = ctx.getStatement();
    switch (statement.getType()) {
      case INSERT:
        ctx.setResult(new Result(RpcUtils.SUCCESS));
        break;
      case DELETE:
        DeleteStatement deleteStatement = (DeleteStatement) statement;
        if (deleteStatement.isInvolveDummyData()) {
          throw new StatementExecutionException(CLEAR_DUMMY_DATA_CAUTION);
        } else {
          ctx.setResult(new Result(RpcUtils.SUCCESS));
        }
        break;
      case SELECT:
        setResultFromBatchStream(ctx, stream);
        break;
      case SHOW_COLUMNS:
        setShowColumnsResult(ctx, stream);
        break;
      default:
        throw new StatementExecutionException(
            String.format("Execute Error: unknown statement type [%s].", statement.getType()));
    }
  }

  private void setResultFromBatchStream(RequestContext ctx, BatchStream stream)
      throws PhysicalException {
    Result result;
    if (ctx.isUseStream()) {
      Status status = RpcUtils.SUCCESS;
      if (ctx.getWarningMsg() != null && !ctx.getWarningMsg().isEmpty()) {
        status = new Status(StatusCode.PARTIAL_SUCCESS.getStatusCode());
        status.setMessage(ctx.getWarningMsg());
      }
      result = new Result(status);
      result.setBatchStream(stream);
      ctx.setResult(result);
      return;
    }
    if (stream == null) {
      setEmptyQueryResp(ctx);
      return;
    }

    List<ByteBuffer> dataList = getBytesFromBatchStream(stream, ctx.getAllocator());
    Status status = RpcUtils.SUCCESS;
    if (ctx.getWarningMsg() != null && !ctx.getWarningMsg().isEmpty()) {
      status = new Status(StatusCode.PARTIAL_SUCCESS.getStatusCode());
      status.setMessage(ctx.getWarningMsg());
    }
    result = new Result(status);
    result.setArrowData(dataList);
    ctx.setResult(result);
  }

  private List<ByteBuffer> getBytesFromBatchStream(BatchStream stream, BufferAllocator allocator)
      throws PhysicalException {
    List<ByteBuffer> dataList = new ArrayList<>();
    try (BatchStream batchStream = stream) {
      while (batchStream.hasNext()) {
        try (Batch batch = batchStream.getNext()) {
          dataList.addAll(getBytesFromVector(batch.flattened(allocator), allocator));
        }
      }
    }
    return dataList;
  }

  private List<ByteBuffer> getBytesFromVector(
      VectorSchemaRoot vectorSchemaRoot, BufferAllocator allocator) throws PhysicalException {
    List<ByteBuffer> dataList = new ArrayList<>();
    try (VectorSchemaRoot root = vectorSchemaRoot;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
      writer.start();
      writer.writeBatch();
      writer.end();
      ByteBuffer data = ByteBuffer.wrap(out.toByteArray());
      dataList.add(data);
    } catch (IOException e) {
      throw new PhysicalException(e);
    }
    return dataList;
  }

  private void setShowColumnsResult(RequestContext ctx, BatchStream stream)
      throws PhysicalException {
    if (ctx.isUseStream()) {
      Result result = new Result(RpcUtils.SUCCESS);
      result.setBatchStream(stream);
      ctx.setResult(result);
      return;
    }
    List<String> paths = new ArrayList<>();
    // todo:need physical layer to support.
    List<Map<String, String>> tagsList = new ArrayList<>();
    List<DataType> types = new ArrayList<>();

    try (BatchStream batchStream = stream) {
      while (batchStream.hasNext()) {
        try (Batch batch = batchStream.getNext()) {
          int rowCnt = batch.getRowCount();
          List<FieldVector> vectors = batch.getVectors();
          if (vectors.size() != 2) {
            LOGGER.warn("show columns result col size = {}", vectors.size());
          } else {
            try (VarBinaryVector pathVector = (VarBinaryVector) vectors.get(0);
                VarBinaryVector typeVector = (VarBinaryVector) vectors.get(1)) {
              for (int i = 0; i < rowCnt; i++) {
                paths.add(new String(pathVector.get(i)));
                DataType type = DataTypeUtils.getDataTypeFromString(new String(typeVector.get(i)));
                if (type == null) {
                  LOGGER.warn("unknown data type [{}]", typeVector.get(i));
                }
                types.add(type);
              }
            }
          }
        }
      }
    }

    Result result = new Result(RpcUtils.SUCCESS);
    result.setPaths(paths);
    result.setDataTypes(types);
    ctx.setResult(result);
  }

  private void parseOldTagsFromSchema(BatchSchema schema, InsertStatement insertStatement)
      throws PhysicalException, StatementExecutionException {
    if (insertStatement.getPaths().size() != schema.getFieldCount()
        && !(schema.hasKey() && insertStatement.getPaths().size() + 1 == schema.getFieldCount())) {
      throw new StatementExecutionException(
          "Execute Error: Insert path size and value size must be equal.");
    }
    int fieldCount = schema.getFieldCount();
    List<Map<String, String>> tagsList = insertStatement.getTagsList();
    int start = schema.hasKey() ? 1 : 0;
    for (int i = start; i < fieldCount; i++) {
      org.apache.arrow.vector.types.pojo.Field field = schema.getField(i);
      Map<String, String> tags = tagsList.get(i - start);
      Map<String, String> oldTags = field.getMetadata();
      if (oldTags != null && !oldTags.isEmpty()) {
        if (tags == null) {
          tagsList.set(i - start, oldTags);
        } else {
          tags.putAll(oldTags);
        }
      }
    }
  }

  private void parseInsertValuesSpecFromBatchStream(
      BufferAllocator allocator,
      long offset,
      BatchStream batchStream,
      InsertStatement insertStatement)
      throws PhysicalException, StatementExecutionException {
    BatchSchema schema = batchStream.getSchema();
    if (insertStatement.getPaths().size() != schema.getFieldCount()
        && !(schema.hasKey() && insertStatement.getPaths().size() + 1 == schema.getFieldCount())) {
      throw new StatementExecutionException(
          "Execute Error: Insert path size and value size must be equal.");
    }
    boolean hasKey = schema.hasKey();
    List<DataType> types = new ArrayList<>();
    for (int i = hasKey ? 1 : 0; i < schema.getFieldCount(); i++) {
      types.add(schema.getDataType(i));
    }

    List<Long> keys = new ArrayList<>();
    List<Object[]> rows = new ArrayList<>();
    List<Bitmap> bitmaps = new ArrayList<>();

    while (batchStream.hasNext()) {
      try (Batch batch = batchStream.getNext();
          VectorSchemaRoot current = VectorSchemaRoots.transfer(allocator, batch.getData())) {
        int rowCnt = current.getRowCount();
        int colCnt = current.getFieldVectors().size();
        int realColCount = colCnt - (hasKey ? 1 : 0);
        List<FieldVector> vectors = current.getFieldVectors();
        for (int i = 0; i < rowCnt; i++) {
          Object[] row = new Object[realColCount];
          int start = 0;
          if (hasKey) {
            keys.add((Long) vectors.get(0).getObject(i) + offset);
            start++;
          } else {
            keys.add(i + offset);
          }
          Bitmap bitmap = new Bitmap(realColCount);
          for (int j = start; j < colCnt; j++) {
            row[j - start] = (vectors.get(j).getObject(i));
            if (row[j - start] != null) {
              bitmap.mark(j - start);
            }
          }
          rows.add(row);
          bitmaps.add(bitmap);
        }
      }
    }
    Object[][] values = rows.toArray(new Object[0][0]);

    insertStatement.setKeys(keys);
    insertStatement.setValues(values);
    insertStatement.setTypes(types);
    insertStatement.setBitmaps(bitmaps);
  }

  private void before(RequestContext ctx, List<? extends Processor> list) {
    record(ctx, list);
  }

  private void after(RequestContext ctx, List<? extends Processor> list) {
    record(ctx, list);
  }

  private void record(RequestContext ctx, List<? extends Processor> list) {
    for (Processor processor : list) {
      Status status = processor.process(ctx);
      if (status != null) {
        ctx.setStatus(status);
        return;
      }
    }
  }
}
