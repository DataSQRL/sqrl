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
import com.datasqrl.graphql.jdbc.VertxJdbcClient;
import com.datasqrl.graphql.jdbc.VertxQueryExecutionContext;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.MetadataReader;
import com.datasqrl.graphql.server.MetadataType;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.PropertyDataFetcher;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Purpose: Implements Context for Vert.x, providing SQL clients and data fetchers. Collaboration:
 * Uses {@link VertxJdbcClient} for database operations and {@link NameCanonicalizer} for name
 * handling.
 */
@Slf4j
@Value
public class VertxContext implements Context {

  VertxJdbcClient sqlClient;
  Map<MetadataType, MetadataReader> metadataReaders;

  @Override
  public JdbcClient getClient() {
    return sqlClient;
  }

  @Override
  public DataFetcher<Object> createPropertyFetcher(String name) {
    return VertxCreateCaseInsensitivePropertyDataFetcher.createCaseInsensitive(name);
  }

  public interface VertxCreateCaseInsensitivePropertyDataFetcher {

    static PropertyDataFetcher<Object> createCaseInsensitive(String propertyName) {
      return new PropertyDataFetcher<>(propertyName) {
        @Override
        public Object get(DataFetchingEnvironment environment) {
          var source = environment.getSource();
          if (source instanceof JsonObject jsonObject) {
            var value = jsonObject.getValue(getPropertyName());
            if (value != null) {
              return value;
            }
            // Case-insensitive lookup for drivers that may not preserve sensitivity
            return jsonObject.getMap().entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(getPropertyName()))
                .filter(e -> e.getValue() != null)
                .map(Map.Entry::getValue)
                .findAny()
                .orElse(null);
          }
          return super.get(environment);
        }
      };
    }
  }

  @Override
  public DataFetcher<?> createArgumentLookupFetcher(
      GraphQLEngineBuilder server, Set<Argument> arguments, ResolvedQuery resolvedQuery) {

    return env -> {
      Set<Argument> argumentSet =
          RootGraphqlModel.VariableArgument.convertArguments(env.getArguments());

      var cf = new CompletableFuture<>();

      // Execute
      QueryExecutionContext context = new VertxQueryExecutionContext(this, env, argumentSet, cf);
      resolvedQuery.accept(server, context);

      return cf;
    };
  }

  @Override
  public MetadataReader getMetadataReader(@NonNull MetadataType metadataType) {
    return checkNotNull(metadataReaders.get(metadataType), "Invalid metadataType %s", metadataType);
  }
}
