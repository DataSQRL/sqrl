package com.datasqrl.graphql;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.VariableArgument;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.PropertyDataFetcher;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.graphql.schema.VertxDataFetcher;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Purpose: Implements Context for Vert.x, providing SQL clients and data fetchers. Collaboration:
 * Uses {@link VertxJdbcClient} for database operations and {@link NameCanonicalizer} for name
 * handling.
 */
@Value
public class VertxContext implements Context {

  private static final Logger log = LoggerFactory.getLogger(VertxContext.class);
  VertxJdbcClient sqlClient;

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
      return new PropertyDataFetcher<Object>(propertyName) {
        @Override
        public Object get(DataFetchingEnvironment environment) {
          Object source = environment.getSource();
          if (source instanceof JsonObject) {
            JsonObject jsonObject = (JsonObject) source;
            Object value = jsonObject.getValue(getPropertyName());
            if (value != null) {
              return value;
            }
            // Case-insensitive lookup for drivers that may not preserve sensitivity
            return jsonObject.getMap().entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(getPropertyName()))
                .filter(e -> e.getValue() != null)
                .map(e -> e.getValue())
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
      GraphQLEngineBuilder server, Map<Set<Argument>, ResolvedQuery> lookupMap) {
    return VertxDataFetcher.create(
        (env, future) -> {
          // Map args
          Set<Argument> argumentSet =
              env.getArguments().entrySet().stream()
                  .map(argument -> new VariableArgument(argument.getKey(), argument.getValue()))
                  .collect(Collectors.toSet());

          // Find query
          ResolvedQuery resolvedQuery = lookupMap.get(argumentSet);
          if (resolvedQuery == null) {
            future.fail("Could not find query: " + env.getArguments());
            return;
          }
          // Execute
          QueryExecutionContext context =
              new VertxQueryExecutionContext(this, env, argumentSet, future);
          resolvedQuery.accept(server, context);
        });
  }
}
