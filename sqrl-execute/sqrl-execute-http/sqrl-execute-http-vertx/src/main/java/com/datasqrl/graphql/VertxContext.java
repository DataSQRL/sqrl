package com.datasqrl.graphql;

import com.datasqrl.graphql.kafka.KafkaSinkResult;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.kafka.KafkaSinkRecord;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.FixedArgument;
import com.datasqrl.graphql.server.Model.GraphQLArgumentWrapper;
import com.datasqrl.graphql.server.Model.KafkaMutationCoords;
import com.datasqrl.graphql.server.Model.ResolvedQuery;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.BuildGraphQLEngine;
import com.datasqrl.graphql.server.SinkEmitter;
import com.datasqrl.graphql.server.SinkResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import graphql.schema.DataFetcher;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.graphql.schema.VertxDataFetcher;
import io.vertx.ext.web.handler.graphql.schema.VertxPropertyDataFetcher;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Value;

@Value
public class VertxContext implements Context {

  VertxJdbcClient sqlClient;
  Map<String, SinkEmitter> sinks;

  @Override
  public JdbcClient getClient() {
    return sqlClient;
  }

  @Override
  public DataFetcher<Object> createPropertyFetcher(String name) {
    return VertxPropertyDataFetcher.create(name);
  }

  @Override
  public DataFetcher<?> createArgumentLookupFetcher(BuildGraphQLEngine server, Map<Set<Argument>, ResolvedQuery> lookupMap) {
    return VertxDataFetcher.create((env, fut) -> {
      //Map args
      Set<FixedArgument> argumentSet = GraphQLArgumentWrapper.wrap(env.getArguments())
          .accept(server, lookupMap);

      //Find query
      ResolvedQuery resolvedQuery = lookupMap.get(argumentSet);
      if (resolvedQuery == null) {
        throw new RuntimeException("Could not find query");
      }

      //Execute
      QueryExecutionContext context = new VertxQueryExecutionContext(this,
          env, argumentSet, fut);
      resolvedQuery.accept(server, context);
    });
  }

  @Override
  public DataFetcher<?> createSinkFetcher(KafkaMutationCoords coords) {
    SinkEmitter emitter = sinks.get(coords.getFieldName());

    Preconditions.checkNotNull(emitter, "Could not find sink for field: %s", coords.getFieldName());
    return VertxDataFetcher.create((env, fut) -> {
      //Rules:
      //Only one argument is allowed, it doesn't matter the name
      //Rule 2: input argument cannot be null.
      Map<String, Object> args = env.getArguments();

      Map<String, Object> entry = (Map<String, Object>)args.entrySet().stream()
          .findFirst().map(Entry::getValue).get();

      try {
        String value = new JsonObject(entry).encode();
        emitter.send(new KafkaSinkRecord(value), fut, entry);
      } catch (Exception e) {
        fut.fail(e);
      }
    });
  }
}
