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
package com.datasqrl.graphql;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DetailedRequestTracer implements Handler<RoutingContext> {

  private DetailedRequestTracer() {
    log.info("DetailedRequestTracer enabled");
  }

  @Override
  public void handle(RoutingContext context) {
    var request = context.request();
    var response = context.response();
    var startTime = System.currentTimeMillis();

    // Generate unique request ID for correlation
    var requestId = generateRequestId();

    // Log incoming request details
    logIncomingRequest(request, requestId);

    // Capture request body
    var body = context.body();
    log.debug("[{}] - context.body() returned: {}", requestId, body);
    if (body != null) {
      log.debug("[{}] - body.buffer() returned: {}", requestId, body.buffer());
    }
    logRequestBody(body != null ? body.buffer() : null, requestId);

    // Hook into response end to log final details
    response.endHandler(
        v -> {
          var endTime = System.currentTimeMillis();
          var duration = endTime - startTime;
          logOutgoingResponse(response, requestId, duration);
        });

    context.next();
  }

  private void logIncomingRequest(HttpServerRequest request, String requestId) {
    var headers =
        request.headers().entries().stream()
            .map(
                entry ->
                    entry.getKey() + ": " + maskSensitiveHeader(entry.getKey(), entry.getValue()))
            .collect(Collectors.joining(", "));

    var params =
        request.params().entries().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining("&"));

    log.info(
        "INCOMING REQUEST [{}] - Method: {}, URI: {}, Headers: [{}], Params: [{}], RemoteAddress: {}",
        requestId,
        request.method(),
        request.uri(),
        headers.isEmpty() ? "none" : headers,
        params.isEmpty() ? "none" : params,
        request.remoteAddress());
  }

  private void logRequestBody(Buffer body, String requestId) {
    if (body == null) {
      log.info("REQUEST BODY [{}] - No body (null)", requestId);
      return;
    }

    var bodyStr = body.toString();
    if (bodyStr.isEmpty()) {
      log.info("REQUEST BODY [{}] - Size: {} bytes, Content: [EMPTY]", requestId, body.length());
      return;
    }

    // Limit body size in logs to prevent overwhelming output
    var truncatedBody =
        bodyStr.length() > 2000 ? bodyStr.substring(0, 2000) + "... [TRUNCATED]" : bodyStr;

    log.info(
        "REQUEST BODY [{}] - Size: {} bytes, Content: {}", requestId, body.length(), truncatedBody);
  }

  private void logOutgoingResponse(HttpServerResponse response, String requestId, long durationMs) {
    var headers =
        response.headers().entries().stream()
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining(", "));

    log.info(
        "OUTGOING RESPONSE [{}] - Status: {}, Headers: [{}], Duration: {}ms",
        requestId,
        response.getStatusCode(),
        headers.isEmpty() ? "none" : headers,
        durationMs);
  }

  private String maskSensitiveHeader(String headerName, String value) {
    var lowerName = headerName.toLowerCase();
    if (lowerName.contains("authorization")
        || lowerName.contains("cookie")
        || lowerName.contains("x-api-key")
        || lowerName.contains("bearer")) {
      return "[MASKED]";
    }
    return value;
  }

  private String generateRequestId() {
    return "REQ-" + System.nanoTime() + "-" + Thread.currentThread().getId();
  }

  public static DetailedRequestTracer create() {
    return new DetailedRequestTracer();
  }
}
