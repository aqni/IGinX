package cn.edu.tsinghua.iginx.metadata.utils;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ColumnsIntervalUtilsTest {

  @ParameterizedTest
  @MethodSource("provideColumnsIntervals")
  public void testEncodeDecode(ColumnsInterval interval) {
    String encoded = ColumnsIntervalUtils.toString(interval);
    ColumnsInterval decoded = ColumnsIntervalUtils.fromString(encoded);
    assertEquals(interval.getStartColumn(), decoded.getStartColumn());
    assertEquals(interval.getEndColumn(), decoded.getEndColumn());
  }

  private static Stream<ColumnsInterval> provideColumnsIntervals() {
    // ASCII 特殊字符范围：32 到 126，排除字母和数字
    String specialCharacters = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

    return Stream.concat(
        Stream.of(
            new ColumnsInterval("start column", "end column"), // 正常情况
            new ColumnsInterval(null, null), // 两端为 null
            new ColumnsInterval("start column", null), // 仅结束列为 null
            new ColumnsInterval(null, "end column"), // 仅开始列为 null
            new ColumnsInterval("", ""), // 空字符串
            new ColumnsInterval(" ", " "), // 空格
            new ColumnsInterval("特殊字符测试", "结束列测试"), // Unicode 字符
            new ColumnsInterval("😀", "😎"), // Emoji
            new ColumnsInterval("start\ncolumn", "end\ncolumn"), // 包含换行符
            new ColumnsInterval("start\tcolumn", "end\tcolumn") // 包含制表符
            ),
        specialCharacters
            .chars()
            .mapToObj(c -> new ColumnsInterval("start" + (char) c, "end" + (char) c)));
  }
}
