package com.datasqrl.graphql.postgres_log;

import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresSubscriptionCoords;
import com.google.common.base.Preconditions;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class PostgresDataFetcherFactory {
  public static DataFetcher<?> create(Map<String, SinkConsumer> subscriptions, PostgresSubscriptionCoords coords) {
    SinkConsumer consumer = subscriptions.get(coords.getFieldName());
    Preconditions.checkNotNull(consumer, "Could not find subscription consumer: {}", coords.getFieldName());

    Flux<Object> deferredFlux = Flux.create(sink ->
        consumer.listen(sink::next, sink::error, (x) -> sink.complete())).share();

    return new DataFetcher<>() {
      @Override
      public Publisher<Object> get(DataFetchingEnvironment env) throws Exception {
        return deferredFlux.filter(entry -> !filterSubscription(entry, env.getArguments()));
      }

      private boolean filterSubscription(Object data, Map<String, Object> args) {
        if (args == null) {
          return false;
        }
        for (Map.Entry<String, String> filter : coords.getFilters().entrySet()) {
          Object argValue = args.get(filter.getKey());
          if (argValue == null) continue;

          Map<String, Object> objectMap;
          if (data instanceof Map) {
            objectMap = (Map) data;
          } else if (data instanceof JsonObject) {
            objectMap = ((JsonObject)data).getMap();
          } else {
            objectMap = Map.of();
          }

          Object retrievedData = objectMap.get(filter.getValue());
          if (!argValue.equals(retrievedData)) {
            return true;
          }
        }

        return false;
      }
    };
  }
}
