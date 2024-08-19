package com.datasqrl.graphql.duckdb;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;
import lombok.SneakyThrows;
import org.duckdb.DuckDBConnection;

public class DuckDBPreparedQuery implements PreparedQuery {

  private final DuckDBConnection conn;
  private final String query;

  public DuckDBPreparedQuery(DuckDBConnection conn, String query) {
    this.conn = conn;
    this.query = query;
  }

  @Override
  public void execute(Tuple tuple, Handler handler) {

    System.out.println();
  }

  @SneakyThrows
  @Override
  public Future execute(Tuple tuple) {

    try (PreparedStatement stmt = conn.duplicate().prepareStatement(query)) {

//      while (rs.next()) {
//        System.out.println(rs.getString(1));
//        System.out.println(rs.getInt(3));
//      }
    }
    System.out.println();
    return null;
  }

  @Override
  public PreparedQuery<RowSet> mapping(Function function) {
    return null;
  }

  @Override
  public PreparedQuery<SqlResult> collecting(Collector collector) {
    return null;
  }

  @Override
  public Future executeBatch(List list) {
    return null;
  }

  @Override
  public void executeBatch(List list, Handler handler) {

  }

  @Override
  public void execute(Handler handler) {

  }

  @Override
  public Future execute() {
    return null;
  }
}
