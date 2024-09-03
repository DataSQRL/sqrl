package com.datasqrl.graphql.postgres_log;

import com.datasqrl.graphql.io.SinkConsumer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

@AllArgsConstructor
public class PostgresSinkConsumer implements SinkConsumer {

  private Connection connection;

  @SneakyThrows
  @Override
  public void listen(Consumer<Object> listener, Consumer<Throwable> errorHandler, Consumer<Void> endOfStream) {
    try (Statement stmt = connection.createStatement()) {
      // issue the LISTEN command to get events when a record is inserted into pgsink
      stmt.execute("LISTEN hightempalert_1_notify;");

      // Get the connection's PGConnection for receiving notifications
      PGConnection pgconn = connection.unwrap(PGConnection.class);

      while (true) {
        // Check for notifications synchronously
        PGNotification[] notifications = pgconn.getNotifications();

        if (notifications != null) {
          for (PGNotification notification : notifications) {
            // Parse the record ID from the notification parameter
            String recordId = notification.getParameter();

            // Fetch the record and store it
            Statement selectStmt = connection.createStatement();
            ResultSet rs = selectStmt.executeQuery("SELECT * FROM pgsink WHERE id = " + recordId);
            ResultSetMetaData rsmd = rs.getMetaData();
            Map<String, Object> resultMap = new HashMap<>();

            while (rs.next()) {
              for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                String columnName = rsmd.getColumnName(i);
                Object columnValue = rs.getObject(i);
                resultMap.put(columnName, columnValue);
              }
            }

            listener.accept(resultMap);
          }
        }

        // Wait a while before checking again
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }
}
