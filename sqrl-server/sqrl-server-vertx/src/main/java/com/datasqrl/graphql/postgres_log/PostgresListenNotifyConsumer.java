package com.datasqrl.graphql.postgres_log;

import com.datasqrl.graphql.VertxJdbcClient;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

@Slf4j
@AllArgsConstructor
public class PostgresListenNotifyConsumer {

  private VertxJdbcClient sqlClient;
  private Connection connection;
  private String listenQuery;
  private String onNotifyQuery;
  private List<String> parameters;

  @SneakyThrows
  public void subscribe(Consumer<Object> listener) {
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    executorService.submit(() -> {
      try (Statement stmt = connection.createStatement()) {
        // issue the LISTEN command to get events when a record is inserted into pgsink
        stmt.execute(listenQuery);

        // Get the connection's PGConnection for receiving notifications
        PGConnection pgconn = connection.unwrap(PGConnection.class);

        while (true) {
          // Check for notifications synchronously
          PGNotification[] notifications = pgconn.getNotifications();

          if (notifications != null) {
            for (PGNotification notification : notifications) {
              String notificationPayload = notification.getParameter();

              JsonObject jsonPayload = new JsonObject(notificationPayload);

              List<Object> paramObj = new ArrayList<>();

              //hack (Soma)
              for (String parameter : parameters) {
                if (parameter.equals("timeSec")) {
                  paramObj.add(OffsetDateTime.parse(jsonPayload.getString(parameter)));

                } else if (parameter.equals("sensorid")) {
                  paramObj.add(jsonPayload.getInteger(parameter));
                } else {
                  throw new IllegalArgumentException("Unknown parameter: " + parameter);
                }
              }

              PreparedQuery<RowSet<Row>> preparedQuery = sqlClient.getClients().get("postgres")
                  .preparedQuery(onNotifyQuery);
              preparedQuery.execute(Tuple.from(paramObj))
                  .onComplete(asyncResult -> {
                    if (asyncResult.succeeded()) {
                      RowSet<Row> rows = asyncResult.result();
                      for (Row row : rows) {
                        Map<String, Object> resultMap = new HashMap<>();
                        for (Row rowValue : rows) {
                          for (int i = 0; i < row.size(); i++) {
                            resultMap.put(row.getColumnName(i), rowValue.getValue(i));
                          }
                        }
                        listener.accept(resultMap);
                      }
                    } else {
                      log.error("An error happened while executing the query: {}", onNotifyQuery, asyncResult.cause());
                    }
                  })
                  .onFailure(e -> log.error("An error happened while executing the query: " + onNotifyQuery, e));
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
      } catch (SQLException e) {
        log.error(e.getMessage(), e);
        // handle the exception
      }
    });
  }

  public void handle(Consumer<Map<String, Object>> onNewRecord) {

  }

}
