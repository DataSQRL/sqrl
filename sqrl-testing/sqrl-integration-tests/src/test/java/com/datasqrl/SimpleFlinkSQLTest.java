package com.datasqrl;

import com.google.common.io.Resources;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class SimpleFlinkSQLTest {

  @Test
  @SneakyThrows
  public void testFlinkSQLExecution() {
    // Initialize a TableEnvironment
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);
    tableEnv.getConfig().getConfiguration().setInteger("parallelism.default", 1);
    String myFlinkSQL = Files.readString(Path.of("src","test","resources","flink.sql"));

    try {
      // Execute the SQL query
      String[] sqlStatements = myFlinkSQL.split(";");

      TableResult result = null;
      for (String statement : sqlStatements) {
        if (!statement.trim().isEmpty()) {
          try {
            // Execute each SQL statement
            result = tableEnv.executeSql(statement.trim());
            // Perform assertions on the result if necessary
          } catch (Exception e) {
            System.out.println(statement);
            e.printStackTrace();
            fail("Execution failed with exception: " + e.getMessage());
          }
        }
      }
      result = tableEnv.executeSql("EXECUTE STATEMENT SET BEGIN\n"
          + "INSERT INTO PrintSinkFilter SELECT name, metric, cdc, ts FROM Dedup1;\n"
          + "INSERT INTO PrintSinkDedup SELECT name, metric, 0 AS cdc, TO_TIMESTAMP('1970-01-01 00:00:00') AS ts FROM Dedup2;\n"
          + "INSERT INTO InputData SELECT name, metric, cdc, ts FROM fake1;\n"
          + "END;\n");
      result.await();

    } catch (Exception e) {
      fail("Execution failed with exception: " + e.getMessage());
    }
  }
}