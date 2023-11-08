package cn.edu.tsinghua.iginx.parquet.io;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.parquet.entity.Table;
import cn.edu.tsinghua.iginx.parquet.tools.ByteUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetIOTest {

  private static final Logger logger = LoggerFactory.getLogger(ParquetIOTest.class);

  protected static final Path FILE_PATH = Paths.get("./src/test/temp.parquet");

  protected static final Storer STORER = new Storer(FILE_PATH);

  protected static final Loader LOADER = new Loader(FILE_PATH);

  @Before
  public void setUp() throws Exception {
    if (new File(FILE_PATH.toString()).createNewFile()) {
      logger.info("Create file: " + FILE_PATH.toString());
    }
  }

  @After
  public void tearDown() throws Exception {
    if (Files.deleteIfExists(FILE_PATH)) {
      logger.info("Delete file: " + FILE_PATH.toString());
    }
  }

  public static Table createInMemoryTable(
      String[] names, DataType[] types, int num, long seed, double filled) {
    assert names.length == types.length;

    if (seed <= 0) {
      seed = -seed + 1;
    }
    Table table = new Table();
    for (int i = 0; i < names.length; i++) {
      table.declareColumn(names[i], types[i]);
    }
    Random rand = new Random(seed);
    long key = seed;
    for (int i = 0; i < num; i++) {
      for (int j = 0; j < table.getHeader().size(); j++) {
        if (rand.nextDouble() > filled) {
          continue;
        }
        Object value = null;
        switch (table.getHeader().get(j).getType()) {
          case BOOLEAN:
            value = rand.nextBoolean();
            break;
          case INTEGER:
            value = rand.nextInt();
            break;
          case LONG:
            value = rand.nextLong();
            break;
          case FLOAT:
            value = rand.nextFloat();
            break;
          case DOUBLE:
            value = rand.nextDouble();
            break;
          case BINARY:
            byte[] bytes = new byte[rand.nextInt(256)];
            rand.nextBytes(bytes);
            value = bytes;
            break;
          default:
            throw new RuntimeException("unsupported data type of " + table.getHeader().get(j));
        }
        table.put(j, key, value);
      }
      key += seed;
    }
    return table;
  }

  @Test
  public void testWriteReadSimpleInMemoryTable() throws Exception {
    Table memTable =
        createInMemoryTable(
            new String[] {"s1", "s2", "s3"},
            new DataType[] {
              DataType.LONG, DataType.DOUBLE, DataType.BINARY,
            },
            10,
            7,
            1);
    STORER.flush(memTable);
    Table fileTable = new Table();
    LOADER.load(fileTable);
    assertEquals(memTable, fileTable);
  }

  @Test
  public void testWriteReadCompleteInMemoryTable() throws Exception {
    Table memTable =
        createInMemoryTable(
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            new DataType[] {
              DataType.BOOLEAN,
              DataType.INTEGER,
              DataType.LONG,
              DataType.FLOAT,
              DataType.DOUBLE,
              DataType.BINARY
            },
            10,
            7,
            1);
    STORER.flush(memTable);
    Table fileTable = new Table();
    LOADER.load(fileTable);
    assertEquals(memTable, fileTable);
  }

  @Test
  public void testWriteReadSparseInMemoryTable() throws Exception {
    Table memTable =
        createInMemoryTable(
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            new DataType[] {
              DataType.BOOLEAN,
              DataType.INTEGER,
              DataType.LONG,
              DataType.FLOAT,
              DataType.DOUBLE,
              DataType.BINARY
            },
            10,
            7,
            0.2);
    STORER.flush(memTable);
    Table fileTable = new Table();
    LOADER.load(fileTable);
    assertEquals(memTable, fileTable);
  }

  @Test
  public void testWriteReadLargeInMemoryTable() throws Exception {
    Table memTable =
        createInMemoryTable(
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            new DataType[] {
              DataType.BOOLEAN,
              DataType.INTEGER,
              DataType.LONG,
              DataType.FLOAT,
              DataType.DOUBLE,
              DataType.BINARY
            },
            100000,
            7,
            0.5);
    STORER.flush(memTable);
    Table fileTable = new Table();
    LOADER.load(fileTable);
    assertEquals(memTable, fileTable);
  }

  @Test
  public void testWriteReadSimpleNestedRecord() throws Exception {
    final int ROW_NUM = 10;

    MessageType schema =
        new MessageType(
            "root",
            new PrimitiveType(
                Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "a1"),
            new GroupType(
                Type.Repetition.REQUIRED,
                "a2",
                new PrimitiveType(
                    Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "b1"),
                new PrimitiveType(
                    Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "b2"),
                new PrimitiveType(
                    Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "b3")),
            new GroupType(
                Type.Repetition.OPTIONAL,
                "a3",
                new GroupType(
                    Type.Repetition.OPTIONAL,
                    "b4",
                    new PrimitiveType(
                        Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "c1"))));

    IginxParquetWriter.Builder builder = IginxParquetWriter.builder(FILE_PATH, schema);
    try (IginxParquetWriter writer = builder.build()) {
      for (int i = 0; i < ROW_NUM; i++) {
        IginxRecord optRecord = new IginxRecord();
        if (i % 2 == 0) {
          optRecord.add(0, i);
        }
        writer.write(
            new IginxRecord()
                .add(0, (long) i)
                .add(
                    1,
                    new IginxRecord()
                        .add(1, (double) i)
                        .add(0, (long) i)
                        .add(2, ("test" + i).getBytes()))
                .add(2, new IginxRecord().add(0, optRecord)));
      }
    }

    Table memTable =
        createInMemoryTable(
            new String[] {"a1", "a2.b1", "a2.b2", "a2.b3", "a3.b4.c1"},
            new DataType[] {
              DataType.LONG, DataType.LONG, DataType.DOUBLE, DataType.BINARY, DataType.INTEGER
            },
            0,
            0,
            0);

    for (int i = 0; i < ROW_NUM; i++) {
      memTable.put(0, i, (long) i);
      memTable.put(1, i, (long) i);
      memTable.put(2, i, (double) i);
      memTable.put(3, i, ("test" + i).getBytes());
      if (i % 2 == 0) {
        memTable.put(4, i, i);
      }
    }

    Table fileTable = new Table();
    LOADER.load(fileTable);
    assertEquals(memTable, fileTable);
  }

  @Test
  public void testWriteReadSimpleRepeatedRecord() throws Exception {
    final int ROW_NUM = 10;
    final int REPEATED_NUM = 3;

    MessageType schema =
        new MessageType(
            "root",
            new PrimitiveType(
                Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "a1"),
            new GroupType(
                Type.Repetition.REQUIRED,
                "a2",
                new PrimitiveType(
                    Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT64, "b1"),
                new PrimitiveType(
                    Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.DOUBLE, "b2"),
                new PrimitiveType(
                    Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.BINARY, "b3")));

    IginxParquetWriter.Builder builder = IginxParquetWriter.builder(FILE_PATH, schema);
    try (IginxParquetWriter writer = builder.build()) {
      for (int i = 0; i < ROW_NUM; i++) {
        Object[] b1 = new Object[REPEATED_NUM];
        Object[] b2 = new Object[REPEATED_NUM];
        Object[] b3 = new Object[REPEATED_NUM];
        for (int j = 0; j < REPEATED_NUM; j++) {
          b1[j] = (long) i + j;
          b2[j] = (double) i + j;
          b3[j] = ("test" + (i + j)).getBytes();
        }
        writer.write(
            new IginxRecord()
                .add(0, (long) i)
                .add(1, new IginxRecord().add(1, b2).add(0, b1).add(2, b3)));
      }
    }

    Table memTable =
        createInMemoryTable(
            new String[] {"a1", "a2.b1", "a2.b2", "a2.b3"},
            new DataType[] {DataType.LONG, DataType.BINARY, DataType.BINARY, DataType.BINARY},
            0,
            0,
            0);

    for (int i = 0; i < ROW_NUM; i++) {
      memTable.put(0, i, (long) i);
      List<byte[]> b1 = new ArrayList<>();
      List<byte[]> b2 = new ArrayList<>();
      List<byte[]> b3 = new ArrayList<>();
      for (int j = 0; j < REPEATED_NUM; j++) {
        b1.add(ByteUtils.asBytes(((long) i + j)));
        b2.add(ByteUtils.asBytes(((double) i + j)));
        b3.add(("test" + (i + j)).getBytes());
      }
      memTable.put(1, i, ByteUtils.concat(b1));
      memTable.put(2, i, ByteUtils.concat(b2));
      memTable.put(3, i, ByteUtils.concat(b3));
    }

    Table fileTable = new Table();
    LOADER.load(fileTable);
    assertEquals(memTable, fileTable);
  }
}
