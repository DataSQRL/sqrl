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

import com.datasqrl.server.ServerContext;
import com.datasqrl.server.exec.QueryExecutionContext;
import com.datasqrl.server.exec.StandardExecutionContext;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.Argument;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractQueryExecutionContext<C extends ServerContext>
    extends StandardExecutionContext<C> implements QueryExecutionContext {

  private final ParamArgumentTypeMapper paramArgumentTypeMapper;

  protected AbstractQueryExecutionContext(
      C context,
      DataFetchingEnvironment environment,
      Set<Argument> arguments,
      ParamArgumentTypeMapper paramArgumentTypeMapper) {
    super(context, environment, arguments);
    this.paramArgumentTypeMapper = paramArgumentTypeMapper;
  }

  public static String addLimitOffsetToQuery(String sqlQuery, String limit, String offset) {
    return "SELECT * FROM (%s) x LIMIT %s OFFSET %s".formatted(sqlQuery, limit, offset);
  }

  protected CompletableFuture<List<Object>> getParamArgumentsFuture(
      List<RootGraphQLModel.QueryParameterHandler> parameters) {

    var paramFutures = new CompletableFuture[parameters.size()];
    for (int i = 0; i < parameters.size(); i++) {
      var param = parameters.get(i);
      var paramValue = param.accept(this, this);

      Optional<String> paramSqlType;
      if (param instanceof RootGraphQLModel.ArgumentParameter argParam) {
        paramSqlType = Optional.of(argParam.getSqlType());
      } else {
        paramSqlType = Optional.empty();
      }

      var paramFuture = wrapToFuture(paramValue);
      paramFutures[i] =
          paramFuture.thenApply(obj -> paramArgumentTypeMapper.map(obj, paramSqlType));
    }

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
