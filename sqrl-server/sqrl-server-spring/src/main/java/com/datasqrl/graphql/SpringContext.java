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

import static com.google.common.base.Preconditions.checkNotNull;

import com.datasqrl.graphql.jdbc.JdbcClient;
import com.datasqrl.graphql.jdbc.SpringJdbcClient;
import com.datasqrl.graphql.jdbc.SpringQueryExecutionContext;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.FunctionExecutor;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.MetadataReader;
import com.datasqrl.graphql.server.MetadataType;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.util.CaseInsensitiveJsonDataFetcher;
import graphql.schema.DataFetcher;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/** Spring-based implementation of {@link Context}. Replaces the Vert.x-based VertxContext. */
@Slf4j
@Value
public class SpringContext implements Context {

  SpringJdbcClient sqlClient;
  Map<MetadataType, MetadataReader> metadataReaders;
  FunctionExecutor functionExecutor;

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
          RootGraphqlModel.VariableArgument.convertArguments(env.getArguments());

      var cf = new CompletableFuture<>();

      var context = new SpringQueryExecutionContext(this, env, argumentSet, cf);
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
