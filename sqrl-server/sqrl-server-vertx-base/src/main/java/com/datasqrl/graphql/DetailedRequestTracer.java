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

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.stream.Collectors;
import lombok.experimental.Delegate;
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
    var requestId = generateRequestId();

    // Capture request data
    var requestData = captureRequestData(request, context.body());

    // Capture response body by wrapping the response
    var responseBodyCapture = Buffer.buffer();
    var wrappedResponse =
        new ResponseCapturingWrapper(
            response, responseBodyCapture, requestId, requestData, startTime);

    // Replace the response in the routing context using reflection
    try {
      var implClass = context.getClass();
      var field = implClass.getDeclaredField("response");
      field.setAccessible(true);
      field.set(context, wrappedResponse);
    } catch (Exception e) {
      log.warn("[{}] Could not wrap response for body capture: {}", requestId, e.getMessage());
      // Fall back to logging without response body
      response.endHandler(
          v -> {
            var duration = System.currentTimeMillis() - startTime;
            logCompleteTrace(requestId, requestData, response, responseBodyCapture, duration);
          });
    }

    context.next();
  }

  private RequestData captureRequestData(
      HttpServerRequest request, io.vertx.ext.web.RequestBody body) {
    var headers =
        request.headers().entries().stream()
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining(", "));

    var params =
        request.params().entries().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining("&"));

    String requestBody = null;
    Integer requestBodySize = null;
    if (body != null && body.buffer() != null) {
      var buffer = body.buffer();
      requestBodySize = buffer.length();
      requestBody = buffer.toString();
      if (requestBody.length() > 10000) {
        requestBody = requestBody.substring(0, 10000) + "... [TRUNCATED]";
      }
    }

    return new RequestData(
        request.method().toString(),
        request.uri(),
        headers.isEmpty() ? "none" : headers,
        params.isEmpty() ? "none" : params,
        request.remoteAddress() != null ? request.remoteAddress().toString() : "unknown",
        requestBody,
        requestBodySize);
  }

  private void logCompleteTrace(
      String requestId,
      RequestData requestData,
      HttpServerResponse response,
      Buffer responseBodyCapture,
      long durationMs) {
    var responseHeaders =
        response.headers().entries().stream()
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining(", "));

    String responseBody = null;
    Integer responseBodySize = null;
    if (responseBodyCapture.length() > 0) {
      responseBodySize = responseBodyCapture.length();
      responseBody = responseBodyCapture.toString();
      if (responseBody.length() > 10000) {
        responseBody = responseBody.substring(0, 10000) + "... [TRUNCATED]";
      }
    }

    log.debug(
        "[{}] {} {} | Status: {} | Duration: {}ms | RemoteAddr: {} | ReqHeaders: [{}] | ReqParams: [{}] | ReqBody: {} bytes {} | RespHeaders: [{}] | RespBody: {} bytes {}",
        requestId,
        requestData.method,
        requestData.uri,
        response.getStatusCode(),
        durationMs,
        requestData.remoteAddress,
        requestData.headers,
        requestData.params,
        requestData.requestBodySize != null ? requestData.requestBodySize : 0,
        requestData.requestBody != null ? "- " + requestData.requestBody : "- [EMPTY]",
        responseHeaders.isEmpty() ? "none" : responseHeaders,
        responseBodySize != null ? responseBodySize : 0,
        responseBody != null ? "- " + responseBody : "- [EMPTY]");
  }

  private String generateRequestId() {
    return "REQ-" + System.nanoTime() + "-" + Thread.currentThread().getId();
  }

  private static class RequestData {
    final String method;
    final String uri;
    final String headers;
    final String params;
    final String remoteAddress;
    final String requestBody;
    final Integer requestBodySize;

    RequestData(
        String method,
        String uri,
        String headers,
        String params,
        String remoteAddress,
        String requestBody,
        Integer requestBodySize) {
      this.method = method;
      this.uri = uri;
      this.headers = headers;
      this.params = params;
      this.remoteAddress = remoteAddress;
      this.requestBody = requestBody;
      this.requestBodySize = requestBodySize;
    }
  }

  public static DetailedRequestTracer create() {
    return new DetailedRequestTracer();
  }

  private class ResponseCapturingWrapper implements HttpServerResponse {
    @Delegate(excludes = BodyCaptureMethods.class)
    private final HttpServerResponse delegate;

    private final Buffer capture;
    private final String requestId;
    private final RequestData requestData;
    private final long startTime;
    private boolean logged = false;

    ResponseCapturingWrapper(
        HttpServerResponse delegate,
        Buffer capture,
        String requestId,
        RequestData requestData,
        long startTime) {
      this.delegate = delegate;
      this.capture = capture;
      this.requestId = requestId;
      this.requestData = requestData;
      this.startTime = startTime;
    }

    @Override
    public Future<Void> write(Buffer data) {
      if (data != null) {
        capture.appendBuffer(data);
      }
      return delegate.write(data);
    }

    @Override
    public Future<Void> write(String chunk) {
      if (chunk != null) {
        capture.appendString(chunk);
      }
      return delegate.write(chunk);
    }

    @Override
    public Future<Void> write(String chunk, String enc) {
      if (chunk != null) {
        capture.appendString(chunk, enc);
      }
      return delegate.write(chunk, enc);
    }

    @Override
    public Future<Void> end() {
      logIfNotLogged();
      return delegate.end();
    }

    @Override
    public Future<Void> end(Buffer chunk) {
      if (chunk != null) {
        capture.appendBuffer(chunk);
      }
      logIfNotLogged();
      return delegate.end(chunk);
    }

    @Override
    public Future<Void> end(String chunk) {
      if (chunk != null) {
        capture.appendString(chunk);
      }
      logIfNotLogged();
      return delegate.end(chunk);
    }

    @Override
    public Future<Void> end(String chunk, String enc) {
      if (chunk != null) {
        capture.appendString(chunk, enc);
      }
      logIfNotLogged();
      return delegate.end(chunk, enc);
    }

    private void logIfNotLogged() {
      if (!logged) {
        logged = true;
        var duration = System.currentTimeMillis() - startTime;
        logCompleteTrace(requestId, requestData, delegate, capture, duration);
      }
    }

    private interface BodyCaptureMethods {
      Future<Void> write(Buffer data);

      Future<Void> write(String chunk);

      Future<Void> write(String chunk, String enc);

      Future<Void> end();

      Future<Void> end(Buffer chunk);

      Future<Void> end(String chunk);

      Future<Void> end(String chunk, String enc);
    }
  }
}
