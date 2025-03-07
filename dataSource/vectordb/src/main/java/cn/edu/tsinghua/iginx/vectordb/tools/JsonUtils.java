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
package cn.edu.tsinghua.iginx.vectordb.tools;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonUtils.class);

  public static Map<String, Object> jsonToMap(String jsonString) {
    try {
      Gson gson = new Gson();
      Type type = new TypeToken<Map<String, Object>>() {}.getType();
      Map<String, Object> map = gson.fromJson(jsonString, type);
      return map;
    } catch (JsonSyntaxException e) {
      return null;
    }
  }

  public static <T> T jsonToType(String jsonString, TypeToken<T> type) {
    try {
      Gson gson = new Gson();
      return gson.fromJson(jsonString, type.getType());
    } catch (JsonSyntaxException e) {
      return null;
    }
  }

  public static String toJson(Object object) {
    return new Gson().toJson(object);
  }

  public static JsonObject mapToJson(Map<String, Object> map) {
    Gson gson = new Gson();
    String jsonString = gson.toJson(map);
    return JsonParser.parseString(jsonString).getAsJsonObject();
  }

  public static List<Float> stringToArray(String str) {
    if (!(str.length() > 2) || str.charAt(0) != '[' || str.charAt(str.length() - 1) != ']') {
      LOGGER.error("vector data is improperly formatted : " + str);
      return null;
    }
    try {
      List<Float> result = new ArrayList<>();
      for (String s : str.substring(1, str.length() - 1).split(",")) {
        result.add(Float.parseFloat(s.trim()));
      }
      return result;
    } catch (NumberFormatException e) {
      LOGGER.error("vector data is improperly formatted : " + str, e);
      return null;
    }
  }
}
