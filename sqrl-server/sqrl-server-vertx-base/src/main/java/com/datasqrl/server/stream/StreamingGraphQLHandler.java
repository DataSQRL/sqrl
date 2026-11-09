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
package com.datasqrl.server.stream;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.Pool;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;

/**
 * Serves eligible GraphQL requests by streaming the rows of their single list field from a database
 * cursor into a chunked response. Every other request goes on to the regular GraphQL handler
 * untouched. Only single JSON POST requests are considered.
 */
@RequiredArgsConstructor
public class StreamingGraphQLHandler implements Handler<RoutingContext> {

  private static final Set<String> REQUEST_KEYS = Set.of("query", "operationName", "variables");
  private static final Set<String> HANDLER_QUERY_PARAMS =
      Set.of("query", "operationName", "variables", "extensions", "initialValue");

  private final GraphQL graphQL;
  private final StreamEligibility eligibility;
  private final Pool pool;
  private final int fetchSize;

  @Override
  public void handle(RoutingContext rc) {
    var request = Request.from(rc);
    var plan = request.flatMap(r -> eligibility.plan(r.query(), r.operationName()));
    if (plan.isEmpty()) {
      rc.next();
      return;
    }
    execute(rc, request.get(), plan.get());
  }

  private void execute(RoutingContext rc, Request request, StreamPlan plan) {
    var pending = new PendingStream();
    var input =
        ExecutionInput.newExecutionInput()
            .query(request.query())
            .operationName(request.operationName())
            .variables(request.variables())
            .graphQLContext(Map.of(RoutingContext.class, rc, PendingStream.class, pending))
            .build();
    Future.fromCompletionStage(graphQL.executeAsync(input), rc.vertx().getOrCreateContext())
        .onFailure(rc::fail)
        .onSuccess(
            result -> {
              if (pending.isBound()) {
                var encoder =
                    new RowJsonEncoder(plan, input.getGraphQLContext(), input.getLocale());
                new RowStreamResponse(rc.response(), plan, encoder).start(pool, pending, fetchSize);
              } else {
                send(rc, result);
              }
            });
  }

  private static void send(RoutingContext rc, ExecutionResult result) {
    rc.response()
        .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        .end(new JsonObject(result.toSpecification()).toBuffer());
  }

  record Request(String query, String operationName, Map<String, Object> variables) {

    static Optional<Request> from(RoutingContext rc) {
      if (rc.request().method() != HttpMethod.POST
          || !isJson(rc.parsedHeaders().contentType().value())
          || rc.queryParams().names().stream().anyMatch(HANDLER_QUERY_PARAMS::contains)
          || !rc.body().available()
          || rc.body().buffer() == null) {
        return Optional.empty();
      }
      try {
        return from(Json.decodeValue(rc.body().buffer()));
      } catch (DecodeException e) {
        return Optional.empty();
      }
    }

    private static boolean isJson(String contentType) {
      return contentType.isEmpty() || contentType.equalsIgnoreCase("application/json");
    }

    private static Optional<Request> from(Object body) {
      if (!(body instanceof JsonObject json)
          || !REQUEST_KEYS.containsAll(json.fieldNames())
          || !(json.getValue("query") instanceof String query)
          || !(json.getValue("operationName") == null
              || json.getValue("operationName") instanceof String)
          || !(json.getValue("variables") == null
              || json.getValue("variables") instanceof JsonObject)) {
        return Optional.empty();
      }
      var variables = json.getJsonObject("variables");
      return Optional.of(
          new Request(
              query,
              json.getString("operationName"),
              variables == null ? Map.of() : variables.getMap()));
    }
  }
}
