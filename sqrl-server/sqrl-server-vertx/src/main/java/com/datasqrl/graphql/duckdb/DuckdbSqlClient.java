package com.datasqrl.graphql.duckdb;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.PrepareOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import lombok.SneakyThrows;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDriver;

public class DuckdbSqlClient implements SqlClient {
  DuckDBConnection conn;

  @SneakyThrows
  public DuckdbSqlClient() {
    String url = "jdbc:duckdb:"; // In-memory DuckDB instance or you can specify a file path for persistence

    try {
      Class.forName("org.duckdb.DuckDBDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    Properties props = new Properties();
    props.setProperty(DuckDBDriver.DUCKDB_READONLY_PROPERTY, String.valueOf(true));
    props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, String.valueOf(true));
    conn = (DuckDBConnection)DriverManager.getConnection(url, props);

    try (Statement stmt = conn.createStatement()) {

      stmt.execute("INSTALL iceberg;");
      stmt.execute("LOAD iceberg;");

    }

  }

  @Override
  public Query<RowSet<Row>> query(String s) {
    throw new RuntimeException();
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
    return new DuckDBPreparedQuery(conn, s);
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s, PrepareOptions prepareOptions) {
    throw new RuntimeException();
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {

  }

  @Override
  public Future<Void> close() {
    return null;
  }
}
