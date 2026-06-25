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

import com.datasqrl.server.config.GraphQLTailSampleTracingConfig;
import com.datasqrl.server.graphql.RootGraphQLModel.ArgumentLookupQueryCoords;
import com.datasqrl.util.JsonUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import graphql.ExecutionResult;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimpleInstrumentationContext;
import graphql.execution.instrumentation.SimplePerformantInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationCreateStateParameters;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLNamedSchemaElement;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/** Logs one structured warning for slow or failed GraphQL executions after execution completes. */
@Slf4j
public class GraphQLTailSampleTracingInstrumentation extends SimplePerformantInstrumentation {

  private static final String METRIC_NAME = "sqrl.graphql.query.tail_trace";

  private final Object rateLimitLock = new Object();

  private final String modelVersion;
  private final Map<QueryKey, Long> thresholds;
  private final long defaultThresholdMs;
  private final boolean alwaysLogErrors;
  private final int maxLoggedTracesPerMinute;
  private final int maxFieldTraces;

  private long windowStartMillis = System.currentTimeMillis();
  private int loggedInWindow = 0;

  /**
   * Creates tail-sampling instrumentation for SQRL-backed GraphQL query fetchers in the supplied
   * model. Only {@link ArgumentLookupQueryCoords} entries are traced because those correspond to
   * generated query execution work rather than simple property lookup fields.
   */
  public GraphQLTailSampleTracingInstrumentation(
      @NonNull String modelVersion,
      @NonNull RootGraphQLModel model,
      @NonNull GraphQLTailSampleTracingConfig config) {

    this.modelVersion = modelVersion;
    this.defaultThresholdMs = Math.max(1, config.getDefaultThresholdMs());
    this.alwaysLogErrors = config.isAlwaysLogErrors();
    this.maxLoggedTracesPerMinute = config.getMaxLoggedTracesPerMinute();
    this.maxFieldTraces = Math.max(1, config.getMaxFieldTraces());
    this.thresholds =
        model.getQueries().stream()
            .filter(ArgumentLookupQueryCoords.class::isInstance)
            .map(coords -> new QueryKey(coords.getParentType(), coords.getFieldName()))
            .distinct()
            .collect(
                Collectors.toConcurrentMap(
                    Function.identity(), key -> thresholdFor(config.getThresholds(), key)));
  }

  /** Creates per-execution mutable trace state shared by the instrumentation callbacks. */
  @Override
  public InstrumentationState createState(InstrumentationCreateStateParameters parameters) {
    return new TraceState();
  }

  /**
   * Starts the end-to-end GraphQL execution timer and registers the completion callback that makes
   * the tail-sampling decision after the operation result is known.
   */
  @Override
  public InstrumentationContext<ExecutionResult> beginExecution(
      InstrumentationExecutionParameters parameters, InstrumentationState state) {

    if (state instanceof TraceState traceState) {
      traceState.operationName = parameters.getOperation();
      traceState.startNanos = System.nanoTime();

      return SimpleInstrumentationContext.whenCompleted(
          (result, throwable) -> logIfSampled(parameters, traceState, result, throwable));
    }

    return SimpleInstrumentationContext.noOp();
  }

  /**
   * Wraps generated SQRL query data fetchers so their individual latencies can be included in the
   * tail trace when the full GraphQL execution is slow or failed.
   */
  @Override
  public DataFetcher<?> instrumentDataFetcher(
      DataFetcher<?> dataFetcher,
      InstrumentationFieldFetchParameters parameters,
      InstrumentationState state) {

    if (!(state instanceof TraceState traceState)) {
      return dataFetcher;
    }

    var environment = parameters.getEnvironment();
    var parentType = environment.getParentType();
    if (!(parentType instanceof GraphQLNamedSchemaElement namedParentType)) {
      return dataFetcher;
    }

    var key = new QueryKey(namedParentType.getName(), environment.getFieldDefinition().getName());
    if (!thresholds.containsKey(key)) {
      return dataFetcher;
    }

    return env -> {
      var startNanos = System.nanoTime();
      try {
        var result = dataFetcher.get(env);
        if (result instanceof CompletionStage<?> stage) {
          return stage.whenComplete(
              (value, error) ->
                  recordFieldTrace(
                      traceState,
                      key,
                      env.getExecutionStepInfo().getPath().toString(),
                      startNanos,
                      error));
        }

        recordFieldTrace(
            traceState, key, env.getExecutionStepInfo().getPath().toString(), startNanos, null);
        return result;
      } catch (Exception e) {
        recordFieldTrace(
            traceState, key, env.getExecutionStepInfo().getPath().toString(), startNanos, e);
        throw e;
      }
    };
  }

  /**
   * Evaluates the completed execution against the slow-query and error policies, then emits a
   * single structured warning if the request should be retained by the tail sampler.
   */
  private void logIfSampled(
      InstrumentationExecutionParameters parameters,
      TraceState state,
      ExecutionResult result,
      Throwable throwable) {

    var durationMs = elapsedMs(state.startNanos);
    var errorCount = result == null ? 0 : result.getErrors().size();
    var failed = throwable != null || errorCount > 0;
    var slowThresholdMs = slowThresholdMs(state);
    var slow = durationMs >= slowThresholdMs;

    if ((!slow && !(alwaysLogErrors && failed)) || !tryAcquireLogPermit()) {
      return;
    }

    var logEvent =
        new TailTraceLogEvent(
            METRIC_NAME,
            modelVersion,
            state.operationName,
            durationMs,
            slowThresholdMs,
            slow ? "slow" : "error",
            errorCount,
            hash(parameters.getQuery()),
            fieldLogEvents(state),
            throwable == null ? null : throwable.getClass().getName(),
            throwable == null ? null : throwable.getMessage());

    logEvent(logEvent);
  }

  /** Converts the bounded set of captured field fetch timings into the serializable log DTO. */
  private List<FieldTraceLogEvent> fieldLogEvents(TraceState state) {
    var fields = new ArrayList<FieldTraceLogEvent>();
    for (var fieldTrace : state.fieldTraces) {
      if (fields.size() >= maxFieldTraces) {
        break;
      }
      fields.add(
          new FieldTraceLogEvent(
              fieldTrace.path(),
              fieldTrace.durationMs(),
              thresholds.getOrDefault(fieldTrace.key(), defaultThresholdMs),
              fieldTrace.failed()));
    }
    return fields;
  }

  private void logEvent(TailTraceLogEvent logEvent) {
    try {
      log.warn("{}", JsonUtils.MAPPER.writeValueAsString(logEvent));
    } catch (JsonProcessingException e) {
      log.warn("Failed to serialize GraphQL tail trace log event", e);
    }
  }

  /**
   * Returns the threshold that applies to this execution. For multi-fetcher operations, the
   * strictest configured field threshold is used so any slow component can retain the trace.
   */
  private long slowThresholdMs(TraceState state) {
    return state.fieldTraces.stream()
        .map(FieldTrace::key)
        .map(key -> thresholds.getOrDefault(key, defaultThresholdMs))
        .min(Long::compareTo)
        .orElse(defaultThresholdMs);
  }

  /** Records one generated SQRL field fetch timing without blocking the request path. */
  private void recordFieldTrace(
      TraceState state, QueryKey key, String path, long startNanos, Throwable throwable) {

    state.fieldTraces.add(new FieldTrace(key, path, elapsedMs(startNanos), throwable != null));
  }

  /** Applies a simple process-local per-minute cap to avoid flooding logs during incidents. */
  private boolean tryAcquireLogPermit() {
    if (maxLoggedTracesPerMinute <= 0) {
      return true;
    }

    synchronized (rateLimitLock) {
      var now = System.currentTimeMillis();
      if (now - windowStartMillis >= TimeUnit.MINUTES.toMillis(1)) {
        windowStartMillis = now;
        loggedInWindow = 0;
      }
      if (loggedInWindow >= maxLoggedTracesPerMinute) {
        return false;
      }
      loggedInWindow++;
      return true;
    }
  }

  private long elapsedMs(long startNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
  }

  /** Returns the configured per-field threshold or the default threshold if none is configured. */
  private long thresholdFor(Map<String, Long> configuredThresholds, QueryKey key) {
    return configuredThresholds.getOrDefault(key.toString(), defaultThresholdMs);
  }

  /**
   * Hashes the GraphQL query text so traces can be correlated without logging raw query strings.
   */
  private static String hash(String value) {
    if (value == null) {
      return null;
    }

    try {
      var digest = MessageDigest.getInstance("SHA-256");
      var hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(hash, 0, 8);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 digest is unavailable", e);
    }
  }

  private static class TraceState implements InstrumentationState {
    private final Queue<FieldTrace> fieldTraces = new ConcurrentLinkedQueue<>();
    private long startNanos;
    private String operationName;
  }

  private record FieldTrace(QueryKey key, String path, long durationMs, boolean failed) {}

  private record TailTraceLogEvent(
      String event,
      String modelVersion,
      @JsonInclude(JsonInclude.Include.NON_NULL) String operationName,
      long durationMs,
      long thresholdMs,
      String sampleReason,
      int errors,
      @JsonInclude(JsonInclude.Include.NON_NULL) String queryHash,
      List<FieldTraceLogEvent> fields,
      @JsonInclude(JsonInclude.Include.NON_NULL) String exceptionType,
      @JsonInclude(JsonInclude.Include.NON_NULL) String exceptionMessage) {}

  private record FieldTraceLogEvent(
      String path, long durationMs, long thresholdMs, boolean failed) {}
}
