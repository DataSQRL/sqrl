package com.datasqrl.graphql;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.server.BuildGraphQLEngine;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.ResolvedQuery;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.graphql.server.Model.VariableArgument;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.google.common.base.Preconditions;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.ext.web.handler.graphql.schema.VertxDataFetcher;
import io.vertx.ext.web.handler.graphql.schema.VertxPropertyDataFetcher;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Value;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@Value
public class VertxContext implements Context {

  VertxJdbcClient sqlClient;
  Map<String, SinkProducer> sinks;
  Map<String, SinkConsumer> subscriptions;
  NameCanonicalizer canonicalizer;

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
      Set<Argument> argumentSet = env.getArguments().entrySet().stream()
          .map(argument -> new VariableArgument(argument.getKey(), argument.getValue()))
          .collect(Collectors.toSet());

      //Find query
      ResolvedQuery resolvedQuery = lookupMap.get(argumentSet);
      if (resolvedQuery == null) {
        fut.fail("Could not find query: " + env.getArguments());
        return;
      }
      //Execute
      QueryExecutionContext context = new VertxQueryExecutionContext(this,
          env, argumentSet, fut);
      resolvedQuery.accept(server, context);
    });
  }

  @Override
  public DataFetcher<?> createSinkFetcher(MutationCoords coords) {
    SinkProducer emitter = sinks.get(coords.getFieldName());

    Preconditions.checkNotNull(emitter, "Could not find sink for field: %s", coords.getFieldName());
    return VertxDataFetcher.create((env, fut) -> {
      //Rules:
      //- Only one argument is allowed, it doesn't matter the name
      //- input argument cannot be null.
      Map<String, Object> args = env.getArguments();

      Map<String, Object> entry = (Map<String, Object>)args.entrySet().stream()
          .findFirst().map(Entry::getValue).get();

      emitter.send(entry)
          .onSuccess(sinkResult->{
            ZonedDateTime dateTime = ZonedDateTime.ofInstant(sinkResult.getSourceTime(), ZoneOffset.UTC);
            entry.put(ReservedName.SOURCE_TIME.getCanonical(), dateTime.toLocalDateTime());
            fut.complete(entry);
          })
          .onFailure(fut::fail);
    });
  }

  @Override
  public DataFetcher<?> createSubscriptionFetcher(SubscriptionCoords coords, Map<String, String> filters) {
    SinkConsumer consumer = subscriptions.get(coords.getFieldName());
    Preconditions.checkNotNull(consumer, "Could not find subscription consumer: {}", coords.getFieldName());

    Flux<Map<String, Object>> deferredFlux = Flux.<Map<String, Object>>create(sink -> {
      consumer.listen(t -> sink.next(t), sink::error, (x) -> sink.complete());
    }).share();

    return new DataFetcher<>() {
      @Override
      public Publisher<Map<String,Object>> get(DataFetchingEnvironment env) throws Exception {
        return deferredFlux.filter(entry -> !filterSubscription(entry, env.getArguments()));
      }

      private boolean filterSubscription(Map<String, Object> data, Map<String, Object> args) {
        if (args == null) {
          return false;
        }
        for (Map.Entry<String, String> filter : filters.entrySet()) {
          Object argValue = args.get(filter.getKey());
          if (argValue == null) continue;

          Object retrievedData = data.get(filter.getValue());
          if (!argValue.equals(retrievedData)) {
            return true;
          }
        }

        return false;
      }
    };
  }
}
