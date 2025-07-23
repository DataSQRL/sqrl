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
import java.util.ArrayList;
import java.util.List;
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
    logRequestBody(body != null ? body.buffer() : null, requestId);

    // Wrap response to capture body data
    var responseBodyCapture = new ArrayList<Buffer>();
    wrapResponse(response, responseBodyCapture);

    // Hook into response end to log final details
    response.endHandler(
        v -> {
          var endTime = System.currentTimeMillis();
          var duration = endTime - startTime;
          logOutgoingResponse(response, requestId, duration, responseBodyCapture);
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

    log.debug(
        "[{}] INCOMING REQUEST - Method: {}, URI: {}, Headers: [{}], Params: [{}], RemoteAddress: {}",
        requestId,
        request.method(),
        request.uri(),
        headers.isEmpty() ? "none" : headers,
        params.isEmpty() ? "none" : params,
        request.remoteAddress());
  }

  private void logRequestBody(Buffer body, String requestId) {
    if (body == null) {
      log.debug("[{}] REQUEST BODY - No body (null)", requestId);
      return;
    }

    var bodyStr = body.toString();
    if (bodyStr.isEmpty()) {
      log.debug("[{}] REQUEST BODY - Size: {} bytes, Content: [EMPTY]", requestId, body.length());
      return;
    }

    log.debug("[{}] REQUEST BODY - Size: {} bytes, Content: {}", requestId, body.length(), bodyStr);
  }

  private void logOutgoingResponse(
      HttpServerResponse response,
      String requestId,
      long durationMs,
      List<Buffer> responseBodyCapture) {
    var headers =
        response.headers().entries().stream()
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining(", "));

    log.debug(
        "[{}] OUTGOING RESPONSE - Status: {}, Headers: [{}], Duration: {}ms",
        requestId,
        response.getStatusCode(),
        headers.isEmpty() ? "none" : headers,
        durationMs);

    logResponseBody(responseBodyCapture, requestId);
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

  private void wrapResponse(HttpServerResponse response, List<Buffer> responseBodyCapture) {
    // Response body logging is complex in Vert.x due to the streaming nature
    // For now, we'll log that response body capture is not yet implemented
    // Future enhancement could use a custom HttpServerResponse wrapper
    log.trace(
        "Response body capture initialized (requires custom response wrapper for full implementation)");
  }

  private void logResponseBody(List<Buffer> responseBodyCapture, String requestId) {
    if (responseBodyCapture.isEmpty()) {
      log.debug(
          "[{}] RESPONSE BODY - No body data captured (requires response wrapping enhancement)",
          requestId);
      return;
    }

    var totalSize = responseBodyCapture.stream().mapToInt(Buffer::length).sum();
    var combinedBody = Buffer.buffer();
    responseBodyCapture.forEach(combinedBody::appendBuffer);

    var bodyStr = combinedBody.toString();
    if (bodyStr.length() > 10000) {
      // Truncate very large responses
      bodyStr = bodyStr.substring(0, 10000) + "... [TRUNCATED]";
    }

    log.debug("[{}] RESPONSE BODY - Size: {} bytes, Content: {}", requestId, totalSize, bodyStr);
  }

  public static DetailedRequestTracer create() {
    return new DetailedRequestTracer();
  }
}
