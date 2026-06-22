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
package com.datasqrl.server.graphql;

import static java.util.Objects.requireNonNull;

import com.datasqrl.server.graphql.RootGraphQLModel.ArgumentLookupQueryCoords;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimplePerformantInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLNamedSchemaElement;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Records latency metrics for GraphQL queries generated in the server model. */
public class GraphQLQueryMetricsInstrumentation extends SimplePerformantInstrumentation {

  public static final String METRIC_NAME = "sqrl.graphql.query.duration";

  private final Map<QueryKey, Timer> timers;

  public GraphQLQueryMetricsInstrumentation(
      MeterRegistry registry, String modelVersion, RootGraphQLModel model) {

    this.timers =
        model.getQueries().stream()
            .filter(ArgumentLookupQueryCoords.class::isInstance)
            .map(coords -> new QueryKey(coords.getParentType(), coords.getFieldName()))
            .distinct()
            .collect(
                Collectors.toConcurrentMap(
                    Function.identity(), key -> createTimer(registry, modelVersion, key)));
  }

  @Override
  public DataFetcher<?> instrumentDataFetcher(
      DataFetcher<?> dataFetcher,
      InstrumentationFieldFetchParameters parameters,
      InstrumentationState state) {

    var environment = parameters.getEnvironment();
    var parentType = environment.getParentType();
    if (!(parentType instanceof GraphQLNamedSchemaElement namedParentType)) {
      return dataFetcher;
    }

    var key = new QueryKey(namedParentType.getName(), environment.getFieldDefinition().getName());
    var timer = timers.get(key);
    if (timer == null) {
      return dataFetcher;
    }

    return env -> {
      var startNanos = System.nanoTime();
      try {
        var result = dataFetcher.get(env);
        if (result instanceof CompletionStage<?> stage) {
          return stage.whenComplete((value, error) -> record(timer, startNanos));
        }

        record(timer, startNanos);
        return result;
      } catch (Exception e) {
        record(timer, startNanos);
        throw e;
      }
    };
  }

  private static Timer createTimer(MeterRegistry registry, String modelVersion, QueryKey key) {
    return Timer.builder(METRIC_NAME)
        .description("GraphQL query execution duration")
        .tag("version", modelVersion)
        .tag("type", key.parentType())
        .tag("name", key.fieldName())
        .publishPercentiles(0.5, 0.99)
        .register(registry);
  }

  private static void record(Timer timer, long startNanos) {
    timer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
  }

  private record QueryKey(String parentType, String fieldName) {

    private QueryKey {
      requireNonNull(parentType, "parentType must not be null");
      requireNonNull(fieldName, "fieldName must not be null");
    }
  }
}
