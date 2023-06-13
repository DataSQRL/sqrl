package com.datasqrl.graphql;

import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.server.BuildGraphQLEngine;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.FixedArgument;
import com.datasqrl.graphql.server.Model.GraphQLArgumentWrapper;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.ResolvedQuery;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
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
import lombok.Value;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@Value
public class VertxContext implements Context {

  VertxJdbcClient sqlClient;
  Map<String, SinkProducer> sinks;
  Map<String, SinkConsumer> subscriptions;

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
  public DataFetcher<?> createSubscriptionFetcher(SubscriptionCoords coords) {
    SinkConsumer consumer = subscriptions.get(coords.getFieldName());
    Preconditions.checkNotNull(consumer, "Could not find subscription consumer: {}", coords.getFieldName());

    return new DataFetcher<>() {
      @Override
      public Publisher<Map<String,Object>> get(DataFetchingEnvironment env) throws Exception {

        Publisher<Map<String,Object>> publisher = Flux.create(sink -> {
          consumer.listen(entry -> {
                Map<String, Object> args = env.getArguments();
                if (!filterSubscription(entry, args)) {
                  sink.next(entry);
                }
              }, sink::error, done -> sink.complete());
        });
        return publisher;
      }

      private boolean filterSubscription(Map<String, Object> map, Map<String, Object> args) {
        if (args == null) {
          return false;
        }
        for (Map.Entry<String, Object> arg : args.entrySet()) {
          Object o = map.get(arg.getKey());
          if (!arg.getValue().equals(o)) {
            return true;
          }
        }
        return false;
      }
    };
  }
}
