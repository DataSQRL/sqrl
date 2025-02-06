package com.datasqrl.graphql;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasqrl.canonicalizer.NameCanonicalizer;
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
import lombok.Value;

/**
 * Purpose: Implements Context for Vert.x, providing SQL clients and data fetchers.
 * Collaboration: Uses {@link VertxJdbcClient} for database operations and {@link NameCanonicalizer} for name handling.
 */
@Value
public class VertxContext implements Context {

  private static final Logger log = LoggerFactory.getLogger(VertxContext.class);
  VertxJdbcClient sqlClient;
  NameCanonicalizer canonicalizer;

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
                .filter(e->e.getKey().equalsIgnoreCase(getPropertyName()))
                .filter(e -> e.getValue() != null)
                .map(Entry::getValue)
                .findAny()
                .orElse(null);
          }
          return super.get(environment);
        }
      };
    }
  }

  @Override
  public DataFetcher<?> createArgumentLookupFetcher(GraphQLEngineBuilder server, Map<Set<Argument>, ResolvedQuery> lookupMap) {
    return VertxDataFetcher.create((env, future) -> {
      //Map args
      Set<Argument> argumentSet = env.getArguments().entrySet().stream()
          .map(argument -> new VariableArgument(argument.getKey(), argument.getValue()))
          .collect(Collectors.toSet());

      //Find query
      var resolvedQuery = lookupMap.get(argumentSet);
      if (resolvedQuery == null) {
        future.fail("Could not find query: " + env.getArguments());
        return;
      }
      //Execute
      QueryExecutionContext context = new VertxQueryExecutionContext(this,
          env, argumentSet, future);
      resolvedQuery.accept(server, context);
    });
  }

}
