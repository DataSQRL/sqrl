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
package com.datasqrl.graphql.kafka;

import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaSubscriptionCoords;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class KafkaDataFetcherFactory {

  public static DataFetcher<?> create(SinkConsumer consumer, KafkaSubscriptionCoords coords) {
    var deferredFlux =
        Flux.create(sink -> consumer.listen(sink::next, sink::error, (x) -> sink.complete()))
            .share();

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
          var argValue = args.get(filter.getKey());
          if (argValue == null) {
            continue;
          }

          Map<String, Object> objectMap;
          if (data instanceof Map map) {
            objectMap = map;
          } else if (data instanceof JsonObject object) {
            objectMap = object.getMap();
          } else {
            objectMap = Map.of();
          }

          var retrievedData = objectMap.get(filter.getValue());
          if (!argValue.equals(retrievedData)) {
            return true;
          }
        }

        return false;
      }
    };
  }
}
