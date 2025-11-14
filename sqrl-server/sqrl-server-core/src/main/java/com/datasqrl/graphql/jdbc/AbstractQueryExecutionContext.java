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

import com.datasqrl.graphql.exec.StandardExecutionContext;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractQueryExecutionContext<C extends Context>
    extends StandardExecutionContext<C> implements QueryExecutionContext {

  protected AbstractQueryExecutionContext(
      C context, DataFetchingEnvironment environment, Set<Argument> arguments) {
    super(context, environment, arguments);
  }

  public static String addLimitOffsetToQuery(String sqlQuery, String limit, String offset) {
    return "SELECT * FROM (%s) x LIMIT %s OFFSET %s".formatted(sqlQuery, limit, offset);
  }

  protected Object mapParamArgumentType(Object param) {
    // By default, do nothing
    return param;
  }

  protected CompletableFuture<List<Object>> getParamArgumentsFuture(
      List<RootGraphqlModel.QueryParameterHandler> parameters) {

    var paramFutures =
        parameters.stream()
            .map(param -> param.accept(this, this))
            .map(this::wrapToFuture)
            .map(future -> future.thenApply(this::mapParamArgumentType))
            .toArray(CompletableFuture[]::new);

    return CompletableFuture.allOf(paramFutures)
        .thenApply(
            v -> {
              var result = new ArrayList<>();
              for (CompletableFuture<?> paramFuture : paramFutures) {
                result.add(paramFuture.join());
              }

              return result;
            });
  }

  private CompletableFuture<Object> wrapToFuture(Object paramRes) {
    if (paramRes instanceof CompletableFuture<?>) {
      return (CompletableFuture<Object>) paramRes;
    }

    return CompletableFuture.completedFuture(paramRes);
  }
}
