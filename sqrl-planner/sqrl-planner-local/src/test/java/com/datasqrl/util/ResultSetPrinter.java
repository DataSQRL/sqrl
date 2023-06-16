/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.google.common.base.Predicates;
import java.math.BigDecimal;
import lombok.SneakyThrows;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.function.Predicate;

public class ResultSetPrinter {

  public static int print(ResultSet resultSet, PrintStream out) {
    return print(resultSet, out, Predicates.alwaysTrue(), Predicates.alwaysTrue());
  }

  @SneakyThrows
  public static int print(ResultSet resultSet, PrintStream out,
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
          //We convert to string to strip some irrelevant double digits during the conversion
          BigDecimal bigDecimal = new BigDecimal(Double.toString((double)o));
          o = bigDecimal.stripTrailingZeros().toPlainString();
        }
        out.print(o);
      }
    }
    return size;
  }

  public static String toString(ResultSet resultSet, Predicate<String> filterColumnsByName,
      Predicate<Integer> filterColumnsByType) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);
    print(resultSet, ps, filterColumnsByName, filterColumnsByType);
    return os.toString(StandardCharsets.UTF_8);
  }

  public static String[] toLines(ResultSet resultSet, Predicate<String> filterColumnsByName,
      Predicate<Integer> filterColumnsByType) {
    return toString(resultSet, filterColumnsByName, filterColumnsByType)
        .split("\\R");
  }
}
