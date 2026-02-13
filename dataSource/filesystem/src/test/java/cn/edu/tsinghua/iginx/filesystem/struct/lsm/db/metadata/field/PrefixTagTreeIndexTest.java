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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.field;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.util.exception.TypeConflictedException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PrefixTagTreeIndexTest {

  private final PrefixTagTreeIndex prefixTagTreeIndex = new PrefixTagTreeIndex();

  @BeforeEach
  @AfterEach
  void setUp() {
    prefixTagTreeIndex.clear();
  }

  void insertNotAlign() throws TypeConflictedException {
    Map<String, String> tags1 = new HashMap<>();
    tags1.put("k1", "nv11");
    tags1.put("k2", "nv21");
    tags1.put("k3", "nv31");
    prefixTagTreeIndex.insert(
        Arrays.asList(
            ArrowFields.of("tagkv.measurement.metric2", tags1, Types.MinorType.BIGINT),
            ArrowFields.of("tagkv.measurement.metric3", tags1, Types.MinorType.FLOAT8)));

    Map<String, String> tags2 = new HashMap<>();
    tags2.put("k1", "nv12");
    tags2.put("k2", "nv22");
    tags2.put("k3", "nv32");
    prefixTagTreeIndex.insert(
        Arrays.asList(
            ArrowFields.of("tagkv.measurement.metric1", tags2, Types.MinorType.BIT),
            ArrowFields.of("tagkv.measurement.metric3", tags2, Types.MinorType.FLOAT8)));

    Map<String, String> tags3 = new HashMap<>();
    tags3.put("k1", "nv13");
    tags3.put("k2", "nv23");
    tags3.put("k3", "nv33");
    prefixTagTreeIndex.insert(
        Arrays.asList(
            ArrowFields.of("tagkv.measurement.metric2", tags2, Types.MinorType.BIGINT),
            ArrowFields.of("tagkv.measurement.metric3", tags3, Types.MinorType.FLOAT8)));
  }

  void insertAlign() throws TypeConflictedException {
    Map<String, String> tags1 = new HashMap<>();
    tags1.put("k1", "v11");
    tags1.put("k2", "v21");
    tags1.put("k3", "v31");
    prefixTagTreeIndex.insert(
        Arrays.asList(
            ArrowFields.of("tagkv.measurement.metric1", tags1, Types.MinorType.BIT),
            ArrowFields.of("tagkv.measurement.metric2", tags1, Types.MinorType.BIGINT),
            ArrowFields.of("tagkv.measurement.metric3", tags1, Types.MinorType.FLOAT8)));

    Map<String, String> tags2 = new HashMap<>();
    tags2.put("k1", "v12");
    tags2.put("k2", "v22");
    prefixTagTreeIndex.insert(
        Arrays.asList(
            ArrowFields.of("tagkv.measurement.metric1", tags2, Types.MinorType.BIT),
            ArrowFields.of("tagkv.measurement.metric2", tags2, Types.MinorType.BIGINT),
            ArrowFields.of("tagkv.measurement.metric3", tags2, Types.MinorType.FLOAT8)));

    Map<String, String> tags3 = new HashMap<>();
    tags3.put("k1", "v13");
    tags3.put("k2", "v23");
    tags3.put("k3", "v33");
    prefixTagTreeIndex.insert(
        Arrays.asList(
            ArrowFields.of("tagkv.measurement.metric1", tags3, Types.MinorType.BIT),
            ArrowFields.of("tagkv.measurement.metric2", tags3, Types.MinorType.BIGINT),
            ArrowFields.of("tagkv.measurement.metric3", tags3, Types.MinorType.FLOAT8)));
  }

  @Test
  void testAlignAndNotAlign() throws TypeConflictedException {
    insertAlign();
    List<Field> result =
        prefixTagTreeIndex.find(
            Arrays.asList("tagkv.measurement.*"), new BaseTagFilter("k2", "v22"));
    insertNotAlign();
    List<Field> result2 =
        prefixTagTreeIndex.find(
            Arrays.asList("tagkv.measurement.*"), new BaseTagFilter("k2", "v22"));
    Assertions.assertEquals(result, result2);
  }

  @Test
  void testAlignWithExisting() throws TypeConflictedException {
    Map<String, String> tags1 = new HashMap<>();
    prefixTagTreeIndex.insert(
        Arrays.asList(
            ArrowFields.of("tagkv.notag.metric1", tags1, Types.MinorType.BIT),
            ArrowFields.of("tagkv.notag.metric2", tags1, Types.MinorType.BIGINT)));

    insertAlign();
    List<Field> result =
        prefixTagTreeIndex.find(
            Arrays.asList("tagkv.measurement.*"), new BaseTagFilter("k2", "v22"));

    prefixTagTreeIndex.remove(
        Arrays.asList(
            ArrowFields.of("tagkv.notag.metric1", tags1, Types.MinorType.BIT),
            ArrowFields.of("tagkv.notag.metric2", tags1, Types.MinorType.BIGINT)));
    List<Field> result1 = prefixTagTreeIndex.find(Arrays.asList("tagkv.notag.*"), null);
    Assertions.assertTrue(result1.isEmpty());

    Map<String, String> tags3 = new HashMap<>();
    tags3.put("k1", "v13");
    tags3.put("k2", "v23");
    tags3.put("k3", "v33");
    prefixTagTreeIndex.remove(
        Arrays.asList(
            ArrowFields.of("tagkv.measurement.metric1", tags3, Types.MinorType.BIT),
            ArrowFields.of("tagkv.measurement.metric2", tags3, Types.MinorType.BIGINT),
            ArrowFields.of("tagkv.measurement.metric3", tags3, Types.MinorType.FLOAT8)));
    List<Field> result2 =
        prefixTagTreeIndex.find(
            Arrays.asList("tagkv.measurement.*"), new BaseTagFilter("k1", "v13"));
    Assertions.assertTrue(result2.isEmpty());

    Map<String, String> tags4 = new HashMap<>();
    tags4.put("k1", "v11");
    tags4.put("k2", "v21");
    tags4.put("k3", "v31");
    prefixTagTreeIndex.remove(
        Arrays.asList(ArrowFields.of("tagkv.measurement.metric1", tags4, Types.MinorType.BIT)));
    List<Field> result3 =
        prefixTagTreeIndex.find(
            Arrays.asList("tagkv.measurement.metric1"), new BaseTagFilter("k1", "v11"));
    Assertions.assertTrue(result3.isEmpty());
  }

  @Test
  void testNotAlignAndAlign() throws TypeConflictedException {
    insertNotAlign();
    List<Field> result =
        prefixTagTreeIndex.find(
            Arrays.asList("tagkv.measurement.*"), new BaseTagFilter("k2", "nv22"));
    insertAlign();
    List<Field> result2 =
        prefixTagTreeIndex.find(
            Arrays.asList("tagkv.measurement.*"), new BaseTagFilter("k2", "nv22"));
    Assertions.assertEquals(result, result2);
  }
}
