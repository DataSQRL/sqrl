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
package com.datasqrl.tests;

import com.datasqrl.UseCaseParam;
import com.datasqrl.env.GlobalEnvironmentStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Locale;
import java.util.stream.IntStream;
import lombok.SneakyThrows;

/**
 * Stands in for TypeSafe AI's Jev endpoint while the {@code stdlib-typesafe} use case runs, and
 * exposes its address as the {@value #ENDPOINT_ENV} environment variable.
 *
 * <p>The mock answers from keywords in the request's state, so the snapshots show which answer
 * belongs to which row:
 *
 * <ul>
 *   <li>Noul: 0.9 if the state mentions "verified", 0.1 otherwise.
 *   <li>Choice: the first option whose name, with underscores read as spaces, the state mentions;
 *       the last option otherwise.
 *   <li>Score: the first level whose criterion the state mentions; level 0 otherwise.
 * </ul>
 *
 * <p>Like the real API, it rejects states over its token limit with a 422 and requests without the
 * expected API key with a 401.
 */
public class TypeSafeTestExtension extends AbstractUseCaseExtension {

  public static final String ENDPOINT_ENV = "TYPESAFE_ENDPOINT";

  private static final String API_KEY = "test-key";
  private static final int MAX_STATE_LENGTH = 800;
  private static final double CHOSEN_PROBABILITY = 0.8;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private HttpServer server;

  private record Response(int status, JsonNode body) {}

  @Override
  protected boolean supports(UseCaseParam param) {
    return param.getUseCaseName().equals("stdlib-typesafe");
  }

  @Override
  @SneakyThrows
  protected void setup(UseCaseParam param) {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/v1/systemone", TypeSafeTestExtension::handle);
    server.start();
    GlobalEnvironmentStore.put(
        ENDPOINT_ENV,
        "http://%s:%d/v1/systemone"
            .formatted(server.getAddress().getHostString(), server.getAddress().getPort()));
  }

  @Override
  protected void teardown(UseCaseParam param) {
    if (server != null) {
      server.stop(0);
      server = null;
    }
  }

  private static void handle(HttpExchange exchange) throws IOException {
    try {
      var response =
          respond(
              exchange.getRequestHeaders().getFirst("Authorization"),
              MAPPER.readTree(exchange.getRequestBody()));
      var bytes = MAPPER.writeValueAsBytes(response.body());
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(response.status(), bytes.length);
      exchange.getResponseBody().write(bytes);
    } finally {
      exchange.close();
    }
  }

  private static Response respond(String authorization, JsonNode request) {
    if (!("Bearer " + API_KEY).equals(authorization)) {
      return error(401, "invalid_api_key");
    }

    var state = request.path("state").toString();
    if (state.length() > MAX_STATE_LENGTH) {
      return error(422, "token_limit_exceeded");
    }

    var text = state.toLowerCase(Locale.ROOT);
    var response = MAPPER.createObjectNode().put("model", request.path("model").asText());
    var answers = response.putObject("answers");
    request
        .path("questions")
        .fields()
        .forEachRemaining(q -> answers.set(q.getKey(), answer(q.getValue(), text)));
    response.putObject("usage").put("input_tokens", state.length()).put("output_tokens", 10);

    return new Response(200, response);
  }

  private static ObjectNode answer(JsonNode question, String state) {
    var type = question.path("type").asText();
    var criteria = question.path("criteria");
    var answer = MAPPER.createObjectNode().put("type", type);
    switch (type) {
      case "noul" -> answer.put("noul", state.contains("verified") ? 0.9 : 0.1);
      case "choice" -> {
        var options = new ArrayList<String>();
        criteria.fieldNames().forEachRemaining(options::add);
        var choice =
            options.stream()
                .filter(option -> state.contains(option.replace('_', ' ')))
                .findFirst()
                .orElse(options.get(options.size() - 1));
        answer.put("choice", choice).put("confidence", CHOSEN_PROBABILITY);
        var probabilities = answer.putObject("probabilities");
        options.forEach(
            option ->
                probabilities.put(option, probability(option.equals(choice), options.size())));
      }
      case "score" -> {
        var levels = criteria.size();
        var score =
            IntStream.range(0, levels)
                .filter(i -> state.contains(criteria.get(i).asText().toLowerCase(Locale.ROOT)))
                .findFirst()
                .orElse(0);
        answer.put("score", (double) score).put("confidence", CHOSEN_PROBABILITY);
        var probabilities = answer.putObject("probabilities");
        IntStream.range(0, levels)
            .forEach(i -> probabilities.put(Integer.toString(i), probability(i == score, levels)));
      }
      default -> throw new IllegalArgumentException("Unknown question type: " + type);
    }

    return answer;
  }

  /** The chosen answer gets {@link #CHOSEN_PROBABILITY}, the others share the rest evenly. */
  private static double probability(boolean chosen, int count) {
    if (chosen) {
      return CHOSEN_PROBABILITY;
    }

    return Math.round((1 - CHOSEN_PROBABILITY) / (count - 1) * 10_000) / 10_000.0;
  }

  private static Response error(int status, String code) {
    var body = MAPPER.createObjectNode();
    body.putObject("error").put("code", code).put("message", "Rejected by the mock Jev server");

    return new Response(status, body);
  }
}
