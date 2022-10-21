package ai.datasqrl.util;

import com.google.common.base.Predicates;
import lombok.SneakyThrows;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.function.Predicate;

public class ResultSetPrinter {

  public static int print(ResultSet resultSet, PrintStream out) {
    return print(resultSet, out, Predicates.alwaysTrue());
  }

  @SneakyThrows
  public static int print(ResultSet resultSet, PrintStream out, Predicate<String> filterColumns) {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    int size = 0;
    while (resultSet.next()) {
      if (size > 0) out.println();
      size++;
      int cols = 0;
      for (int i = 1; i <= columnCount; i++) {
        if (!filterColumns.test(metaData.getColumnName(i))) continue;
        if (cols++ > 0) out.print(", ");
        out.print(resultSet.getObject(i));
      }
    }
    return size;
  }

  public static String toString(ResultSet resultSet, Predicate<String> filterColumns) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);
    print(resultSet,ps,filterColumns);
    return os.toString(StandardCharsets.UTF_8);
  }

  public static String[] toLines(ResultSet resultSet, Predicate<String> filterColumns) {
    return toString(resultSet,filterColumns).split("\\R");
  }
}
