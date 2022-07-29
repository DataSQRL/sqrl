package ai.datasqrl.util;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetPrinter {

  public static int print(ResultSet resultSet, PrintStream out)
      throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    int size = 0;
    while (resultSet.next()) {
      size++;
      for (int i = 1;; i++) {
        out.print(resultSet.getObject(i));
        if (i < columnCount) {
          out.print(", ");
        } else {
          out.println();
          break;
        }
      }
    }
    return size;
  }
}
