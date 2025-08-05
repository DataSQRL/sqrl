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

import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;

import com.datasqrl.graphql.VertxJdbcClient.PreparedSqrlQueryImpl;
import com.datasqrl.graphql.jdbc.AbstractQueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Value;

/**
 * It is the ExecutionContext per servlet type. It is responsible for executing the resolved SQL
 * queries (paginated or not) in Vert.x and mapping the database resultSet to json for using in
 * GraphQL responses. It also implements the parameters and arguments visitors for the {@link
 * com.datasqrl.graphql.server.RootGraphqlModel} visitors
 */
@Value
public class VertxQueryExecutionContext extends AbstractQueryExecutionContext {
  VertxContext context;
  DataFetchingEnvironment environment;
  Set<Argument> arguments;
  CompletableFuture<Object> cf;

  @Override
  public CompletableFuture runQuery(ResolvedSqlQuery resolvedQuery, boolean isList) {
    var preparedQueryContainer = (PreparedSqrlQueryImpl) resolvedQuery.getPreparedQueryContainer();
    final var paramObj = getParamArguments(resolvedQuery.getQuery().getParameters());
    var query = resolvedQuery.getQuery();
    var unpreparedSqlQuery = query.getSql();
    switch (query.getPagination()) {
      case NONE:
        break;
      case LIMIT_AND_OFFSET:
        Optional<Integer> limit = Optional.ofNullable(getEnvironment().getArgument(LIMIT));
        Optional<Integer> offset = Optional.ofNullable(getEnvironment().getArgument(OFFSET));

        // special case where database doesn't support binding for limit/offset => need to execute
        // dynamically
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
    var parameters = Tuple.from(paramObj);
    if (preparedQueryContainer == null) {
      future =
          this.context
              .getSqlClient()
              .execute(resolvedQuery.getQuery().getDatabase(), unpreparedSqlQuery, parameters);
    } else {
      var preparedQuery = preparedQueryContainer.preparedQuery();
      future =
          this.context
              .getSqlClient()
              .execute(resolvedQuery.getQuery().getDatabase(), preparedQuery, parameters);
    }

    // map the resultSet to json for GraphQL response
    future
        .map(r -> resultMapper(r, isList))
        .onSuccess(result -> cf.complete(result))
        .onFailure(
            f -> {
              f.printStackTrace();
              cf.completeExceptionally(f);
            });
    return cf;
  }

  private Object resultMapper(RowSet<Row> r, boolean isList) {
    List<JsonObject> o =
        StreamSupport.stream(r.spliterator(), false).map(Row::toJson).collect(Collectors.toList());
    return unboxList(o, isList);
  }
}
