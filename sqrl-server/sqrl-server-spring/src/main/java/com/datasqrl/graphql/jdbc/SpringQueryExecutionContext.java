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

import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;

import com.datasqrl.graphql.SpringContext;
import com.datasqrl.graphql.jdbc.SpringJdbcClient.PreparedQueryWrapper;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * Spring-based query execution context that executes resolved SQL queries. Replaces the
 * Vert.x-based VertxQueryExecutionContext.
 */
@Slf4j
public class SpringQueryExecutionContext extends AbstractQueryExecutionContext<SpringContext> {

  private final CompletableFuture<Object> cf;

  public SpringQueryExecutionContext(
      SpringContext context,
      DataFetchingEnvironment environment,
      Set<Argument> arguments,
      CompletableFuture<Object> cf) {
    super(context, environment, arguments);
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

  @Override
  protected Object mapParamArgumentType(Object param) {
    if (param instanceof List<?> l) {
      return l.toArray();
    }
    return param;
  }

  private void runQueryInternal(
      ResolvedSqlQuery resolvedQuery, boolean isList, List<Object> paramObj) {

    var query = resolvedQuery.getQuery();
    var preparedQueryContainer = (PreparedQueryWrapper) resolvedQuery.getPreparedQueryContainer();
    var unpreparedSqlQuery = query.getSql();

    switch (query.getPagination()) {
      case NONE:
        break;
      case LIMIT_AND_OFFSET:
        Optional<Integer> limit = Optional.ofNullable(getEnvironment().getArgument(LIMIT));
        Optional<Integer> offset = Optional.ofNullable(getEnvironment().getArgument(OFFSET));

        if (!query.getDatabase().supportsLimitOffsetBinding) {
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

    var sqlClient = getContext().getSqlClient();
    var database = resolvedQuery.getQuery().getDatabase();
    var finalSql =
        preparedQueryContainer != null ? preparedQueryContainer.sql() : unpreparedSqlQuery;

    sqlClient
        .execute(database, finalSql, new ArrayList<>(paramObj))
        .thenApply(results -> resultMapper(results, isList))
        .whenComplete(
            (result, throwable) -> {
              if (throwable != null) {
                log.error("Query execution failed", throwable);
                cf.completeExceptionally(throwable);
              } else {
                cf.complete(result);
              }
            });
  }

  private Object resultMapper(List<Map<String, Object>> rows, boolean isList) {
    return unboxList(rows, isList);
  }
}
