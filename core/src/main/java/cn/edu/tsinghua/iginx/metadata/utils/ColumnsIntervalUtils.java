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
package cn.edu.tsinghua.iginx.metadata.utils;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class ColumnsIntervalUtils {

  public static ColumnsInterval fromString(String str) {
    String[] parts = str.split(":", 2); // 使用 ":" 作为分隔符，避免与列名中的 "-" 冲突
    assert parts.length == 2;
    return new ColumnsInterval(
        "null".equals(parts[0]) ? null : decode(parts[0]),
        "null".equals(parts[1]) ? null : decode(parts[1]));
  }

  public static String toString(ColumnsInterval interval) {
    String start = interval.getStartColumn() == null ? "null" : encode(interval.getStartColumn());
    String end = interval.getEndColumn() == null ? "null" : encode(interval.getEndColumn());
    return start + ":" + end; // 使用 ":" 作为分隔符
  }

  private static String encode(String value) {
    try {
      return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String decode(String value) {
    try {
      return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }
}
