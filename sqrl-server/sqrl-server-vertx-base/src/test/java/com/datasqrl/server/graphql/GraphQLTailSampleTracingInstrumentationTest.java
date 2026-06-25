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

import com.datasqrl.server.config.GraphQLTailSampleTracingConfig;
import com.datasqrl.server.graphql.RootGraphQLModel.ArgumentLookupQueryCoords;
import com.datasqrl.util.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import graphql.ExecutionResult;
import graphql.Scalars;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.ResultPath;
import graphql.execution.instrumentation.parameters.InstrumentationCreateStateParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

class GraphQLTailSampleTracingInstrumentationTest {

  @Test
  void givenSlowQuery_whenExecutionCompletes_thenLogsTraceWarning() throws Exception {
    var appender = attachAppender();
    try {
      var instrumentation = newInstrumentation(config(1, Map.of("Query.HighTempAlert", 1L)));
      var state = instrumentation.createState(mock(InstrumentationCreateStateParameters.class));
      var executionContext =
          instrumentation.beginExecution(
              executionParameters(
                  "query HighTempAlertQuery { HighTempAlert }", "HighTempAlertQuery"),
              state);
      var environment = environment("Query", "HighTempAlert", "/HighTempAlert");
      DataFetcher<?> fetcher = env -> "result";

      var instrumented =
          instrumentation.instrumentDataFetcher(fetcher, parameters(environment), state);
      assertThat(instrumented.get(environment)).isEqualTo("result");

      Thread.sleep(5);
      executionContext.onCompleted(executionResult(List.of()), null);

      var event = singleLogEvent(appender);
      assertThat(event.get("event").asText()).isEqualTo("sqrl.graphql.query.tail_trace");
      assertThat(event.get("modelVersion").asText()).isEqualTo("v1");
      assertThat(event.get("operationName").asText()).isEqualTo("HighTempAlertQuery");
      assertThat(event.get("sampleReason").asText()).isEqualTo("slow");
      assertThat(event.get("thresholdMs").asLong()).isEqualTo(1);
      assertThat(event.get("errors").asInt()).isZero();
      assertThat(event.get("queryHash").asText()).isNotBlank();
      assertThat(event.get("fields")).hasSize(1);
      assertThat(event.get("fields").get(0).get("path").asText()).isEqualTo("/HighTempAlert");
      assertThat(event.get("fields").get(0).get("thresholdMs").asLong()).isEqualTo(1);
      assertThat(event.get("fields").get(0).get("failed").asBoolean()).isFalse();
    } finally {
      detachAppender(appender);
    }
  }

  @Test
  void givenFastQuery_whenExecutionCompletes_thenDoesNotLogTraceWarning() throws Exception {
    var appender = attachAppender();
    try {
      var instrumentation = newInstrumentation(config(60_000, Map.of()));
      var state = instrumentation.createState(mock(InstrumentationCreateStateParameters.class));
      var executionContext =
          instrumentation.beginExecution(
              executionParameters(
                  "query HighTempAlertQuery { HighTempAlert }", "HighTempAlertQuery"),
              state);
      var environment = environment("Query", "HighTempAlert", "/HighTempAlert");
      DataFetcher<?> fetcher = env -> "result";

      var instrumented =
          instrumentation.instrumentDataFetcher(fetcher, parameters(environment), state);
      assertThat(instrumented.get(environment)).isEqualTo("result");
      executionContext.onCompleted(executionResult(List.of()), null);

      assertThat(appender.messages()).isEmpty();
    } finally {
      detachAppender(appender);
    }
  }

  @Test
  void givenFailedQuery_whenExecutionCompletes_thenLogsErrorTraceWarning() throws Exception {
    var appender = attachAppender();
    try {
      var instrumentation = newInstrumentation(config(60_000, Map.of()));
      var state = instrumentation.createState(mock(InstrumentationCreateStateParameters.class));
      var executionContext =
          instrumentation.beginExecution(
              executionParameters(
                  "query HighTempAlertQuery { HighTempAlert }", "HighTempAlertQuery"),
              state);
      var environment = environment("Query", "HighTempAlert", "/HighTempAlert");
      DataFetcher<?> fetcher =
          env -> {
            throw new IllegalStateException("boom");
          };

      var instrumented =
          instrumentation.instrumentDataFetcher(fetcher, parameters(environment), state);
      assertThatThrownBy(() -> instrumented.get(environment))
          .isInstanceOf(IllegalStateException.class);
      executionContext.onCompleted(executionResult(List.of()), new IllegalStateException("boom"));

      var event = singleLogEvent(appender);
      assertThat(event.get("sampleReason").asText()).isEqualTo("error");
      assertThat(event.get("exceptionType").asText())
          .isEqualTo(IllegalStateException.class.getName());
      assertThat(event.get("exceptionMessage").asText()).isEqualTo("boom");
      assertThat(event.get("fields").get(0).get("failed").asBoolean()).isTrue();
    } finally {
      detachAppender(appender);
    }
  }

  @Test
  void givenUnregisteredField_whenDataFetcherInstrumented_thenReturnsOriginalFetcher() {
    var instrumentation = newInstrumentation(config(1, Map.of()));
    var state = instrumentation.createState(mock(InstrumentationCreateStateParameters.class));
    var environment = environment("Query", "OtherField", "/OtherField");
    DataFetcher<?> fetcher = env -> "result";

    var instrumented =
        instrumentation.instrumentDataFetcher(fetcher, parameters(environment), state);

    assertThat(instrumented).isSameAs(fetcher);
  }

  private static GraphQLTailSampleTracingInstrumentation newInstrumentation(
      GraphQLTailSampleTracingConfig config) {
    var model = RootGraphQLModel.builder().query(argumentQuery("Query", "HighTempAlert")).build();
    return new GraphQLTailSampleTracingInstrumentation("v1", model, config);
  }

  private static GraphQLTailSampleTracingConfig config(
      long defaultThresholdMs, Map<String, Long> thresholds) {
    var config = new GraphQLTailSampleTracingConfig();
    config.setDefaultThresholdMs(defaultThresholdMs);
    config.setThresholds(thresholds);
    config.setMaxLoggedTracesPerMinute(0);
    return config;
  }

  private static ArgumentLookupQueryCoords argumentQuery(String parentType, String fieldName) {
    return ArgumentLookupQueryCoords.builder().parentType(parentType).fieldName(fieldName).build();
  }

  private static InstrumentationExecutionParameters executionParameters(
      String query, String operationName) {
    var parameters = mock(InstrumentationExecutionParameters.class);
    when(parameters.getQuery()).thenReturn(query);
    when(parameters.getOperation()).thenReturn(operationName);
    return parameters;
  }

  private static InstrumentationFieldFetchParameters parameters(
      DataFetchingEnvironment environment) {
    var params = mock(InstrumentationFieldFetchParameters.class);
    when(params.getEnvironment()).thenReturn(environment);
    return params;
  }

  private static DataFetchingEnvironment environment(
      String parentTypeName, String fieldName, String path) {
    var environment = mock(DataFetchingEnvironment.class);
    var parentType = GraphQLObjectType.newObject().name(parentTypeName).build();
    var fieldDefinition =
        GraphQLFieldDefinition.newFieldDefinition()
            .name(fieldName)
            .type(Scalars.GraphQLString)
            .build();
    var stepInfo = mock(ExecutionStepInfo.class);

    when(stepInfo.getPath()).thenReturn(ResultPath.parse(path));
    when(environment.getParentType()).thenReturn(parentType);
    when(environment.getFieldDefinition()).thenReturn(fieldDefinition);
    when(environment.getExecutionStepInfo()).thenReturn(stepInfo);

    return environment;
  }

  private static ExecutionResult executionResult(List<?> errors) {
    var result = mock(ExecutionResult.class);
    when(result.getErrors()).thenReturn((List) errors);
    return result;
  }

  private static JsonNode singleLogEvent(CapturingAppender appender) throws Exception {
    assertThat(appender.messages()).hasSize(1);
    return JsonUtils.MAPPER.readTree(appender.messages().get(0));
  }

  private static CapturingAppender attachAppender() {
    var appender = new CapturingAppender();
    appender.start();
    logger().setLevel(Level.WARN);
    logger().addAppender(appender);
    return appender;
  }

  private static void detachAppender(CapturingAppender appender) {
    logger().removeAppender(appender);
    appender.stop();
  }

  private static Logger logger() {
    return (Logger) LogManager.getLogger(GraphQLTailSampleTracingInstrumentation.class);
  }

  private static class CapturingAppender extends AbstractAppender {

    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    private CapturingAppender() {
      super("graphql-tail-sample-tracing-test", null, null, false, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }

    private List<String> messages() {
      return events.stream().map(event -> event.getMessage().getFormattedMessage()).toList();
    }
  }
}
