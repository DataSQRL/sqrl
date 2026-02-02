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
package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.PreparedSqrlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Spring-based JDBC client that supports both R2DBC (async) and JDBC (sync with virtual threads).
 * Replaces the Vert.x-based VertxJdbcClient.
 */
@Slf4j
public record SpringJdbcClient(Map<DatabaseType, Object> clients, boolean asyncMode)
    implements JdbcClient {

  @Override
  public ResolvedQuery prepareQuery(SqlQuery query, Context context) {
    var client = clients.get(query.getDatabase());
    if (client == null) {
      throw new RuntimeException("Could not find database engine: " + query.getDatabase());
    }

    return new ResolvedSqlQuery(query, new PreparedQueryWrapper(query.getSql()));
  }

  @Override
  public ResolvedQuery unpreparedQuery(SqlQuery sqlQuery, Context context) {
    return new ResolvedSqlQuery(sqlQuery, null);
  }

  public CompletableFuture<List<Map<String, Object>>> execute(
      DatabaseType database, String sql, List<Object> params) {

    var client = clients.get(database);
    if (client == null) {
      return CompletableFuture.failedFuture(
          new RuntimeException("Could not find database engine: " + database));
    }

    if (client instanceof ConnectionFactory connectionFactory) {
      return executeR2dbc(connectionFactory, sql, params);
    } else if (client instanceof DataSource dataSource) {
      return executeJdbc(dataSource, sql, params);
    } else {
      return CompletableFuture.failedFuture(
          new RuntimeException("Unknown client type: " + client.getClass()));
    }
  }

  private CompletableFuture<List<Map<String, Object>>> executeR2dbc(
      ConnectionFactory connectionFactory, String sql, List<Object> params) {

    return Mono.usingWhen(
            connectionFactory.create(),
            connection -> executeStatement(connection, sql, params),
            Connection::close)
        .toFuture();
  }

  private Mono<List<Map<String, Object>>> executeStatement(
      Connection connection, String sql, List<Object> params) {

    Statement statement = connection.createStatement(sql);

    for (int i = 0; i < params.size(); i++) {
      Object param = params.get(i);
      if (param == null) {
        statement.bindNull(i, Object.class);
      } else {
        statement.bind(i, param);
      }
    }

    return Flux.from(statement.execute()).flatMap(result -> result.map(this::mapRow)).collectList();
  }

  private Map<String, Object> mapRow(Row row, RowMetadata metadata) {
    var result = new HashMap<String, Object>();
    for (var columnMetadata : metadata.getColumnMetadatas()) {
      var columnName = columnMetadata.getName();
      result.put(columnName, row.get(columnName));
    }
    return result;
  }

  private CompletableFuture<List<Map<String, Object>>> executeJdbc(
      DataSource dataSource, String sql, List<Object> params) {

    if (asyncMode) {
      return CompletableFuture.supplyAsync(() -> executeJdbcSync(dataSource, sql, params));
    } else {
      return CompletableFuture.completedFuture(executeJdbcSync(dataSource, sql, params));
    }
  }

  private List<Map<String, Object>> executeJdbcSync(
      DataSource dataSource, String sql, List<Object> params) {

    try (var connection = dataSource.getConnection();
        var statement = connection.prepareStatement(sql)) {

      for (int i = 0; i < params.size(); i++) {
        statement.setObject(i + 1, params.get(i));
      }

      try (var resultSet = statement.executeQuery()) {
        var metadata = resultSet.getMetaData();
        var columnCount = metadata.getColumnCount();
        var results = new java.util.ArrayList<Map<String, Object>>();

        while (resultSet.next()) {
          var row = new HashMap<String, Object>();
          for (int i = 1; i <= columnCount; i++) {
            row.put(metadata.getColumnLabel(i), resultSet.getObject(i));
          }
          results.add(row);
        }

        return results;
      }
    } catch (Exception e) {
      throw new RuntimeException("Error executing JDBC query", e);
    }
  }

  public record PreparedQueryWrapper(String sql) implements PreparedSqrlQuery<String> {
    @Override
    public String preparedQuery() {
      return sql;
    }
  }
}
