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

import com.datasqrl.graphql.exec.StandardExecutionContext;
import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaSubscriptionCoords;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class KafkaDataFetcherFactory {

  public static DataFetcher<?> create(
      SinkConsumer consumer, KafkaSubscriptionCoords coords, Context context) {
    var deferredFlux =
        Flux.create(sink -> consumer.listen(sink::next, sink::error, (x) -> sink.complete()))
            .share();

    return new DataFetcher<>() {
      @Override
      public Publisher<Object> get(DataFetchingEnvironment env) throws Exception {
        var execContext =
            new StandardExecutionContext(
                context,
                env,
                RootGraphqlModel.VariableArgument.convertArguments(env.getArguments()));
        Map<String, Object> fieldNameToValue =
            coords.getEqualityConditions().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().accept(execContext, execContext)));
        return deferredFlux.filter(entry -> filterSubscription(entry, fieldNameToValue));
      }

      private boolean filterSubscription(Object data, Map<String, Object> fieldNameToValue) {
        for (Map.Entry<String, Object> filter : fieldNameToValue.entrySet()) {
          var argValue = filter.getValue();
          if (argValue == null) {
            return false;
          }

          Map<String, Object> objectMap;
          if (data instanceof Map map) {
            objectMap = map;
          } else if (data instanceof JsonObject object) {
            objectMap = object.getMap();
          } else {
            objectMap = Map.of();
          }

          var retrievedData = objectMap.get(filter.getKey());
          if (!argValue.equals(retrievedData)) {
            return false;
          }
        }

        return true;
      }
    };
  }
}
