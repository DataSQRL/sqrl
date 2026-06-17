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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.server.graphql.RootGraphQLModel.ArgumentLookupQueryCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.FieldLookupQueryCoords;
import graphql.Scalars;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class GraphQLQueryMetricsInstrumentationTest {

  @Test
  void givenArgumentLookupQuery_whenDataFetcherCompletes_thenRecordsTimer() throws Exception {
    var registry = new SimpleMeterRegistry();
    var instrumentation = newInstrumentation(registry, argumentQuery("Query", "HighTempAlert"));
    var params = parameters("Query", "HighTempAlert");
    var environment = params.getEnvironment();

    DataFetcher<?> fetcher = env -> "result";
    var instrumented = instrumentation.instrumentDataFetcher(fetcher, params, null);

    assertThat(instrumented.get(environment)).isEqualTo("result");

    assertThat(timerCount(registry, "Query", "HighTempAlert")).isEqualTo(1);
  }

  @Test
  void givenArgumentLookupQuery_whenAsyncDataFetcherCompletes_thenRecordsTimerAfterCompletion()
      throws Exception {
    var registry = new SimpleMeterRegistry();
    var instrumentation = newInstrumentation(registry, argumentQuery("Query", "HighTempAlert"));
    var params = parameters("Query", "HighTempAlert");
    var environment = params.getEnvironment();
    var future = new CompletableFuture<String>();

    DataFetcher<?> fetcher = env -> future;
    var instrumented = instrumentation.instrumentDataFetcher(fetcher, params, null);
    var result = (CompletionStage<?>) instrumented.get(environment);

    assertThat(timerCount(registry, "Query", "HighTempAlert")).isZero();

    future.complete("result");
    assertThat(result.toCompletableFuture().get()).isEqualTo("result");
    assertThat(timerCount(registry, "Query", "HighTempAlert")).isEqualTo(1);
  }

  @Test
  void givenArgumentLookupQuery_whenDataFetcherFails_thenRecordsTimer() {
    var registry = new SimpleMeterRegistry();
    var instrumentation = newInstrumentation(registry, argumentQuery("Query", "HighTempAlert"));
    var params = parameters("Query", "HighTempAlert");
    var environment = params.getEnvironment();

    DataFetcher<?> fetcher =
        env -> {
          throw new IllegalStateException("boom");
        };
    var instrumented = instrumentation.instrumentDataFetcher(fetcher, params, null);

    assertThatThrownBy(() -> instrumented.get(environment))
        .isInstanceOf(IllegalStateException.class);
    assertThat(timerCount(registry, "Query", "HighTempAlert")).isEqualTo(1);
  }

  @Test
  void givenFieldLookupQuery_whenInstrumentationCreated_thenDoesNotRegisterTimer() {
    var registry = new SimpleMeterRegistry();
    var fieldLookupQuery =
        FieldLookupQueryCoords.builder()
            .parentType("SensorReading")
            .fieldName("eventTime")
            .columnName("event_time")
            .build();

    newInstrumentation(registry, fieldLookupQuery);

    assertThat(
            registry
                .find(GraphQLQueryMetricsInstrumentation.METRIC_NAME)
                .tag("version", "v1")
                .tag("type", "SensorReading")
                .tag("name", "eventTime")
                .timer())
        .isNull();
  }

  @Test
  void givenUnregisteredField_whenDataFetcherInstrumented_thenReturnsOriginalFetcher() {
    var registry = new SimpleMeterRegistry();
    var instrumentation = newInstrumentation(registry, argumentQuery("Query", "HighTempAlert"));
    var params = parameters("Query", "SensorReading");

    DataFetcher<?> fetcher = env -> "result";
    var instrumented = instrumentation.instrumentDataFetcher(fetcher, params, null);

    assertThat(instrumented).isSameAs(fetcher);
  }

  private static GraphQLQueryMetricsInstrumentation newInstrumentation(
      SimpleMeterRegistry registry, RootGraphQLModel.QueryCoords queryCoords) {

    var model = RootGraphQLModel.builder().query(queryCoords).build();
    return new GraphQLQueryMetricsInstrumentation(registry, "v1", model);
  }

  private static ArgumentLookupQueryCoords argumentQuery(String parentType, String fieldName) {
    return ArgumentLookupQueryCoords.builder().parentType(parentType).fieldName(fieldName).build();
  }

  private static InstrumentationFieldFetchParameters parameters(
      String parentTypeName, String fieldName) {
    var environment = mock(DataFetchingEnvironment.class);
    var parentType = GraphQLObjectType.newObject().name(parentTypeName).build();
    var fieldDefinition =
        GraphQLFieldDefinition.newFieldDefinition()
            .name(fieldName)
            .type(Scalars.GraphQLString)
            .build();

    when(environment.getParentType()).thenReturn(parentType);
    when(environment.getFieldDefinition()).thenReturn(fieldDefinition);

    var params = mock(InstrumentationFieldFetchParameters.class);
    when(params.getEnvironment()).thenReturn(environment);

    return params;
  }

  private static long timerCount(
      SimpleMeterRegistry registry, String parentType, String fieldName) {
    return registry
        .get(GraphQLQueryMetricsInstrumentation.METRIC_NAME)
        .tag("version", "v1")
        .tag("type", parentType)
        .tag("name", fieldName)
        .timer()
        .count();
  }
}
