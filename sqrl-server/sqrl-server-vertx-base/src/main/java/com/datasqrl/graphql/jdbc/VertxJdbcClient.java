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

import com.datasqrl.graphql.VertxContext;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.PreparedSqrlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Purpose: Manages SQL clients and executes queries. Collaboration: Used by {@link VertxContext} to
 * prepare and execute SQL queries.
 */
@Slf4j
public record VertxJdbcClient(Map<DatabaseType, SqlClient> clients) implements JdbcClient {
  @Override
  public ResolvedQuery prepareQuery(SqlQuery query, Context context) {
    var sqlClient = clients.get(query.getDatabase());
    if (sqlClient == null) {
      throw new RuntimeException("Could not find database engine: " + query.getDatabase());
    }

    var preparedQuery = sqlClient.preparedQuery(query.getSql());

    return new ResolvedSqlQuery(query, new PreparedSqrlQueryImpl(preparedQuery));
  }

  @Override
  public ResolvedQuery unpreparedQuery(SqlQuery sqlQuery, Context context) {
    return new ResolvedSqlQuery(sqlQuery, null);
  }

  public Future<RowSet<Row>> execute(PreparedQuery<RowSet<Row>> query, Tuple tup) {
    return query.execute(tup);
  }

  public Future<RowSet<Row>> execute(DatabaseType database, String query, Tuple tup) {
    var sqlClient = clients.get(database);

    return execute(sqlClient.preparedQuery(query), tup);
  }

  public record PreparedSqrlQueryImpl(PreparedQuery<RowSet<Row>> preparedQuery)
      implements PreparedSqrlQuery<PreparedQuery<RowSet<Row>>> {}
}
