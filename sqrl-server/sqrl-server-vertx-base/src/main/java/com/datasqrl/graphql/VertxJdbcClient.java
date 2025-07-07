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
package com.datasqrl.graphql;

import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.graphql.jdbc.JdbcClient;
import com.datasqrl.graphql.query.UserSqlQueryPreprocessor;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import com.datasqrl.graphql.server.query.ResolvedQuery;
import com.datasqrl.graphql.server.query.ResolvedQuery.PreparedQueryContainer;
import com.datasqrl.graphql.server.query.ResolvedQuery.Preprocessor;
import com.datasqrl.graphql.server.query.ResolvedSqlQuery;
import com.datasqrl.graphql.server.query.SqlQueryModifier;
import com.datasqrl.graphql.server.query.SqlQueryModifier.UserSqlQuery;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import java.util.Map;
import java.util.Optional;
import lombok.Value;

/**
 * Purpose: Manages SQL clients and executes queries. Collaboration: Used by {@link VertxContext} to
 * prepare and execute SQL queries.
 */
@Value
public class VertxJdbcClient implements JdbcClient {
  Map<DatabaseType, SqlClient> clients;

  @Override
  public ResolvedQuery prepareQuery(SqlQuery query, Context context) {
    var sqlClient = clients.get(query.getDatabase());
    if (sqlClient == null) {
      throw new RuntimeException("Could not find database engine: " + query.getDatabase());
    }

    var preparedQuery = sqlClient.preparedQuery(query.getSql());

    return new ResolvedSqlQuery(query, new PreparedSqrlQueryImpl(preparedQuery), Optional.empty());
  }

  @Override
  public ResolvedQuery unpreparedQuery(
      SqlQuery sqlQuery, Optional<SqlQueryModifier> preprocessor, Context context) {
    Optional<Preprocessor> resolvedPreprocessor =
        preprocessor.map(
            pre -> {
              if (pre instanceof UserSqlQuery userQuery) {
                return new UserSqlQueryPreprocessor(userQuery);
              } else {
                throw new UnsupportedOperationException("Unsupported query modifier: " + pre);
              }
            });
    return new ResolvedSqlQuery(sqlQuery, null, resolvedPreprocessor);
  }

  public Future<RowSet<Row>> execute(DatabaseType database, PreparedQuery query, Tuple tup) {
    var sqlClient = clients.get(database);

    if (database == DatabaseType.DUCKDB) {
      return sqlClient
          .query("INSTALL iceberg;")
          .execute()
          .compose(v -> sqlClient.query("LOAD iceberg;").execute())
          .compose(t -> query.execute(tup));
    }
    return query.execute(tup);
  }

  public Future<RowSet<Row>> execute(DatabaseType database, String query, Tuple tup) {
    var sqlClient = clients.get(database);
    return execute(database, sqlClient.preparedQuery(query), tup);
  }

  @Value
  public static class PreparedSqrlQueryImpl
      implements PreparedQueryContainer<PreparedQuery<RowSet<Row>>> {
    PreparedQuery<RowSet<Row>> preparedQuery;
  }
}
