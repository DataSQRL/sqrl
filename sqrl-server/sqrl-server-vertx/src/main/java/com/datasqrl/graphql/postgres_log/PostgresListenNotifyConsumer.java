package com.datasqrl.graphql.postgres_log;

import com.datasqrl.graphql.VertxJdbcClient;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgConnection;
import io.vertx.pgclient.PgNotification;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Listens for PostgreSQL notifications on a PostgreSQL channel, converts the
 * notification payload into a JsonObject and extracts the required parameters. These parameters are
 * then used to execute a predefined SQL query (onNotifyQuery)
 */
@Slf4j
@AllArgsConstructor
public class PostgresListenNotifyConsumer {

  private VertxJdbcClient sqlClient;
  private String listenQuery;
  private String onNotifyQuery;
  private List<String> parameters;
  private Vertx vertx;
  private PgConnectOptions pgConnectOptions;

  @SneakyThrows
  public void subscribe(Consumer<Object> listener) {
    // Establish a direct PgConnection
    PgConnection.connect(vertx, pgConnectOptions, res -> {
      if (res.succeeded()) {
        PgConnection pgConnection = res.result();

        // Set the notification handler
        pgConnection.notificationHandler(notification -> {
          log.trace("Received notification on channel: {} Payload: {}",
              notification.getChannel(), notification.getPayload());

          handleNotification(notification, listener);
        });

        // Execute the LISTEN command to listen for notifications on a specific channel
        pgConnection.query(listenQuery).execute(queryResult -> {
          if (queryResult.succeeded()) {
            log.info("LISTEN command executed successfully: {}", listenQuery);
          } else {
            log.error(String.format("Unable to execute LISTEN command: %s", queryResult.cause().getMessage()), queryResult.cause());
          }
        });
      } else {
        log.error(String.format("Unable to establish connection. %s", res.cause().getMessage()), res.cause());
      }
    });
  }

  public void handleNotification(PgNotification notification, Consumer<Object> listener) {
    JsonObject jsonPayload = new JsonObject(notification.getPayload());

    List<Object> paramObj = new ArrayList<>();

    // TODO: properly do datatype conversion
    for (String parameter : parameters) {
      Object value = jsonPayload.getValue(parameter);
      paramObj.add(value);
    }

    // TODO (Soma) - It feels odd that we are using vertx a bit differently compared to how we
    //  handle the notifications. This is the accepted way of running queries in the codebase
    //  however in case on notifications we are forced to use PGConnection since that's the
    //  only way currently to listen to notifications.
    PreparedQuery<RowSet<Row>> preparedQuery = sqlClient.getClients().get("postgres").preparedQuery(onNotifyQuery);

    preparedQuery.execute(Tuple.from(paramObj))
        .onComplete(asyncResult -> {
          if (asyncResult.succeeded()) {
            RowSet<Row> rows = asyncResult.result();
            for (Row row : rows) {
              listener.accept(row.toJson());
            }
          } else {
            log.error("An error happened while executing the query: {}", onNotifyQuery,
                asyncResult.cause());
          }
        })
        .onFailure(e -> log.error("An error happened while executing the query: " + onNotifyQuery, e));
  }

}
