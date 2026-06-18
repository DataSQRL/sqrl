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
package com.datasqrl.server.jdbc;

import static com.datasqrl.server.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.server.jdbc.SchemaConstants.OFFSET;

import com.datasqrl.server.VertxServerContext;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.Argument;
import com.datasqrl.server.graphql.RootGraphQLModel.ResolvedSqlQuery;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;

/**
 * It is the ExecutionContext per servlet type. It is responsible for executing the resolved SQL
 * queries (paginated or not) in Vert.x and mapping the database resultSet to json for using in
 * GraphQL responses. It also implements the parameters and arguments visitors for the {@link
 * RootGraphQLModel} visitors
 */
public class VertxQueryExecutionContext extends AbstractQueryExecutionContext<VertxServerContext> {

  final CompletableFuture<Object> cf;

  public VertxQueryExecutionContext(
      VertxServerContext serverCtx,
      DataFetchingEnvironment environment,
      Set<Argument> arguments,
      CompletableFuture<Object> cf,
      ParamArgumentTypeMapper paramArgumentTypeMapper) {
    super(serverCtx, environment, arguments, paramArgumentTypeMapper);
    this.cf = cf;
  }

  @Override
  public CompletableFuture<Object> runQuery(ResolvedSqlQuery resolvedQuery, boolean isList) {
    getParamArgumentsFuture(resolvedQuery.getQuery().getParameters())
        .whenComplete(
            (paramObj, throwable) -> {
              if (throwable != null) {
                cf.completeExceptionally(throwable);
              } else {
                runQueryInternal(resolvedQuery, isList, paramObj);
              }
            });

    return cf;
  }

  private CompletableFuture<Object> runQueryInternal(
      ResolvedSqlQuery resolvedQuery, boolean isList, List<Object> paramObj) {

    var preparedQueryContainer = (PreparedVertxSqrlQuery) resolvedQuery.getPreparedQueryContainer();
    var query = resolvedQuery.getQuery();
    var unpreparedSqlQuery = query.getSql();
    switch (query.getPagination()) {
      case NONE:
        break;
      case LIMIT_AND_OFFSET:
        var limit = Optional.<Integer>ofNullable(environment.getArgument(LIMIT));
        var offset = Optional.<Integer>ofNullable(environment.getArgument(OFFSET));

        // special case where database doesn't support binding for limit/offset => need
        // to execute dynamically
        if (!query.getDatabase().supportsLimitOffsetBinding) {
          assert preparedQueryContainer == null;
          unpreparedSqlQuery =
              AbstractQueryExecutionContext.addLimitOffsetToQuery(
                  unpreparedSqlQuery,
                  limit.map(Object::toString).orElse("ALL"),
                  String.valueOf(offset.orElse(0)));
        } else {
          paramObj.add(limit.orElse(Integer.MAX_VALUE));
          paramObj.add(offset.orElse(0));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported pagination: " + query.getPagination());
    }

    // execute the preparedQuery with the arguments extracted above
    Future<RowSet<Row>> future;
    var params = Tuple.from(paramObj);
    var database = resolvedQuery.getQuery().getDatabase();

    if (preparedQueryContainer == null) {
      future = serverContext.getSqlClient().execute(database, unpreparedSqlQuery, params);
    } else {
      var preparedQuery = preparedQueryContainer.preparedQuery();
      future = serverContext.getSqlClient().execute(preparedQuery, params);
    }

    // map the resultSet to json for GraphQL response
    future
        .map(r -> resultMapper(r, isList))
        .onSuccess(cf::complete)
        .onFailure(
            f -> {
              f.printStackTrace();
              cf.completeExceptionally(f);
            });
    return cf;
  }

  private Object resultMapper(RowSet<Row> r, boolean isList) {
    var o = StreamSupport.stream(r.spliterator(), false).map(Row::toJson).toList();

    return unboxList(o, isList);
  }
}
