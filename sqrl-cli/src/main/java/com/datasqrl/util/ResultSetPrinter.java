/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.util;

import com.datasqrl.graphql.server.CustomScalars;
import com.google.common.base.Predicates;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.function.Predicate;
import lombok.SneakyThrows;

public class ResultSetPrinter {

  public static int print(ResultSet resultSet, PrintStream out) {
    return print(resultSet, out, Predicates.alwaysTrue(), Predicates.alwaysTrue());
  }

  @SneakyThrows
  public static int print(
      ResultSet resultSet,
      PrintStream out,
      Predicate<String> filterColumnsByName,
      Predicate<Integer> filterColumnsByType) {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    int size = 0;
    while (resultSet.next()) {
      if (size > 0) {
        out.println();
      }
      size++;
      int cols = 0;
      for (int i = 1; i <= columnCount; i++) {
        if (!filterColumnsByName.test(metaData.getColumnLabel(i))) {
          continue;
        }
        if (!filterColumnsByType.test(metaData.getColumnType(i))) {
          continue;
        }
        if (cols++ > 0) {
          out.print(", ");
        }
        Object o = resultSet.getObject(i);
        if (o instanceof Double) {
          // We convert to string to strip some irrelevant double digits during the conversion
          o = CustomScalars.DOUBLE.getCoercing().serialize(o);
        }
        out.print(o);
      }
    }
    return size;
  }

  public static String toString(ResultSet resultSet) {
    return toString(resultSet, Predicates.alwaysTrue(), Predicates.alwaysTrue());
  }

  public static String toString(
      ResultSet resultSet,
      Predicate<String> filterColumnsByName,
      Predicate<Integer> filterColumnsByType) {
    var os = new ByteArrayOutputStream();
    var ps = new PrintStream(os);
    print(resultSet, ps, filterColumnsByName, filterColumnsByType);
    return os.toString(StandardCharsets.UTF_8);
  }

  public static String[] toLines(
      ResultSet resultSet,
      Predicate<String> filterColumnsByName,
      Predicate<Integer> filterColumnsByType) {
    return toString(resultSet, filterColumnsByName, filterColumnsByType).split("\\R");
  }
}
