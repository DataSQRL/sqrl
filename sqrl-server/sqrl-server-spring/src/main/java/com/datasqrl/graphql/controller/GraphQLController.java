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
package com.datasqrl.graphql.controller;

import com.datasqrl.graphql.config.ServerConfigProperties;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * GraphQL HTTP endpoint controller. Handles GraphQL queries, mutations, and subscriptions over
 * HTTP.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class GraphQLController {

  private final Map<String, GraphQL> graphQLEngines;
  private final ServerConfigProperties config;

  @PostMapping(
      value = "/graphql",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Mono<Map<String, Object>> executeGraphQL(
      @RequestBody GraphQLRequest request, Principal principal) {
    return executeQuery("", request, principal);
  }

  @PostMapping(
      value = "/{version}/graphql",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Mono<Map<String, Object>> executeVersionedGraphQL(
      @PathVariable String version, @RequestBody GraphQLRequest request, Principal principal) {
    return executeQuery(version, request, principal);
  }

  private Mono<Map<String, Object>> executeQuery(
      String version, GraphQLRequest request, Principal principal) {

    var graphQL = graphQLEngines.get(version);
    if (graphQL == null) {
      var errorResponse = new HashMap<String, Object>();
      errorResponse.put(
          "errors",
          java.util.List.of(Map.of("message", "GraphQL engine not found for version: " + version)));
      return Mono.just(errorResponse);
    }

    var executionInput =
        ExecutionInput.newExecutionInput()
            .query(request.query())
            .operationName(request.operationName())
            .variables(request.variables() != null ? request.variables() : Map.of())
            .graphQLContext(
                builder -> {
                  if (principal instanceof Authentication auth
                      && auth.getPrincipal() instanceof Jwt jwt) {
                    builder.of("claims", jwt.getClaims());
                  }
                })
            .build();

    CompletableFuture<ExecutionResult> futureResult = graphQL.executeAsync(executionInput);

    return Mono.fromFuture(futureResult).map(this::toSpecificationResult);
  }

  private Map<String, Object> toSpecificationResult(ExecutionResult executionResult) {
    var result = new HashMap<String, Object>();

    if (executionResult.getData() != null) {
      result.put("data", executionResult.getData());
    }

    if (!executionResult.getErrors().isEmpty()) {
      result.put(
          "errors",
          executionResult.getErrors().stream()
              .map(
                  error -> {
                    var errorMap = new HashMap<String, Object>();
                    errorMap.put("message", error.getMessage());
                    if (error.getPath() != null) {
                      errorMap.put("path", error.getPath());
                    }
                    if (error.getLocations() != null) {
                      errorMap.put("locations", error.getLocations());
                    }
                    if (error.getExtensions() != null) {
                      errorMap.put("extensions", error.getExtensions());
                    }
                    return errorMap;
                  })
              .toList());
    }

    if (executionResult.getExtensions() != null && !executionResult.getExtensions().isEmpty()) {
      result.put("extensions", executionResult.getExtensions());
    }

    return result;
  }

  public record GraphQLRequest(String query, String operationName, Map<String, Object> variables) {}
}
