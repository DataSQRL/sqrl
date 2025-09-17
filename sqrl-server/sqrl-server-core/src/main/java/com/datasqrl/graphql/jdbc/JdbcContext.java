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

import static com.google.common.base.Preconditions.checkNotNull;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.MetadataReader;
import com.datasqrl.graphql.server.MetadataType;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import graphql.schema.DataFetcher;
import graphql.schema.PropertyDataFetcher;
import java.util.Map;
import java.util.Set;
import lombok.NonNull;
import lombok.Value;

@Value
public class JdbcContext implements Context {

  GenericJdbcClient client;
  Map<MetadataType, MetadataReader> metadataReaders;

  @Override
  public DataFetcher<Object> createPropertyFetcher(String name) {
    return PropertyDataFetcher.fetching(name);
  }

  @Override
  public DataFetcher<?> createArgumentLookupFetcher(
      GraphQLEngineBuilder server, Set<Argument> arguments, ResolvedQuery resolvedQuery) {
    // Runtime execution, keep this as light as possible
    return (env) -> {
      Set<Argument> argumentSet =
          RootGraphqlModel.VariableArgument.convertArguments(env.getArguments());

      QueryExecutionContext context = new JdbcExecutionContext(this, env, argumentSet);
      return resolvedQuery.accept(server, context);
    };
  }

  @Override
  public MetadataReader getMetadataReader(@NonNull MetadataType metadataType) {
    return checkNotNull(metadataReaders.get(metadataType), "Invalid metadataType %s", metadataType);
  }
}
