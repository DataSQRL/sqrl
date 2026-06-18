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
package com.datasqrl.server;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datasqrl.server.exec.QueryExecutionContext;
import com.datasqrl.server.graphql.GraphQLEngineBuilder;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.Argument;
import com.datasqrl.server.graphql.RootGraphQLModel.ResolvedQuery;
import com.datasqrl.server.jdbc.JdbcClient;
import com.datasqrl.server.jdbc.ParamArgumentTypeMapper;
import com.datasqrl.server.jdbc.VertxJdbcClient;
import com.datasqrl.server.jdbc.VertxQueryExecutionContext;
import com.datasqrl.server.util.CaseInsensitiveJsonDataFetcher;
import graphql.schema.DataFetcher;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/** Implements {@link ServerContext} for Vert.x, providing SQL clients and data fetchers. */
@Slf4j
@Value
public class VertxServerContext implements ServerContext {

  VertxJdbcClient sqlClient;
  Map<MetadataType, MetadataReader> metadataReaders;
  FunctionExecutor functionExecutor;
  ParamArgumentTypeMapper paramArgumentTypeMapper;

  @Override
  public JdbcClient getClient() {
    return sqlClient;
  }

  @Override
  public DataFetcher<Object> createPropertyFetcher(String name) {
    return new CaseInsensitiveJsonDataFetcher(name);
  }

  @Override
  public DataFetcher<?> createArgumentLookupFetcher(
      GraphQLEngineBuilder server, Set<Argument> arguments, ResolvedQuery resolvedQuery) {

    return env -> {
      Set<Argument> argumentSet =
          RootGraphQLModel.VariableArgument.convertArguments(env.getArguments());

      var cf = new CompletableFuture<>();

      // Execute
      QueryExecutionContext context =
          new VertxQueryExecutionContext(this, env, argumentSet, cf, paramArgumentTypeMapper);
      resolvedQuery.accept(server, context);

      return cf;
    };
  }

  @Override
  public MetadataReader getMetadataReader(@NonNull MetadataType metadataType) {
    return checkNotNull(metadataReaders.get(metadataType), "Invalid metadataType %s", metadataType);
  }

  @Override
  public FunctionExecutor getFunctionExecutor() {
    return functionExecutor;
  }
}
