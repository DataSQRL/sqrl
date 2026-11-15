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
package com.datasqrl.server.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.server.McpStatelessServerHandler;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.ErrorCodes;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCNotification;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCResponse.JSONRPCError;
import io.modelcontextprotocol.spec.McpStatelessServerTransport;
import io.modelcontextprotocol.spec.ProtocolVersions;
import io.vertx.core.Future;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Serves the MCP Streamable HTTP transport in stateless mode on Vert.x routes, for the stateless
 * server of the MCP Java SDK. Each POST carries one JSON-RPC message: a request is answered with a
 * single JSON response, and a notification or response is accepted with 202. The server keeps no
 * sessions and opens no server-to-client stream, so a GET is answered with 405.
 */
@Slf4j
@RequiredArgsConstructor
public class VertxMcpTransport implements McpStatelessServerTransport {

  /** The key of the request's {@link RoutingContext} in its {@link McpTransportContext}. */
  public static final String ROUTING_CONTEXT_KEY = RoutingContext.class.getName();

  /**
   * The SDK's versions for stateless servers, plus 2024-11-05 for clients that still ask for it.
   */
  private static final List<String> PROTOCOL_VERSIONS =
      List.of(
          ProtocolVersions.MCP_2024_11_05,
          ProtocolVersions.MCP_2025_03_26,
          ProtocolVersions.MCP_2025_06_18,
          ProtocolVersions.MCP_2025_11_25);

  private static final String APPLICATION_JSON = "application/json";

  private final McpJsonMapper jsonMapper;
  private McpStatelessServerHandler mcpHandler;

  @Override
  public void setMcpHandler(McpStatelessServerHandler mcpHandler) {
    this.mcpHandler = mcpHandler;
  }

  @Override
  public List<String> protocolVersions() {
    return PROTOCOL_VERSIONS;
  }

  @Override
  public Mono<Void> closeGracefully() {
    return Mono.empty();
  }

  /** Handles a POST of a JSON-RPC message to the MCP endpoint. */
  public void handlePost(RoutingContext ctx) {
    var body = ctx.body().asString();
    try {
      var json = jsonMapper.readValue(body, JsonNode.class);
      if (json == null || !json.isObject()) {
        sendError(ctx, ErrorCodes.INVALID_REQUEST, "Expected a single JSON-RPC message");
        return;
      }
    } catch (IOException | IllegalArgumentException e) {
      sendError(ctx, ErrorCodes.PARSE_ERROR, "Parse error");
      return;
    }

    final McpSchema.JSONRPCMessage message;
    try {
      message = McpSchema.deserializeJsonRpcMessage(jsonMapper, body);
    } catch (IOException | IllegalArgumentException e) {
      sendError(ctx, ErrorCodes.INVALID_REQUEST, "Invalid JSON-RPC message");
      return;
    }

    var transportContext = McpTransportContext.create(Map.of(ROUTING_CONTEXT_KEY, ctx));
    if (message instanceof JSONRPCRequest request) {
      toFuture(ctx, transportContext, () -> mcpHandler.handleRequest(transportContext, request))
          .recover(
              error -> {
                log.error("Failed to handle MCP request {}", request.method(), error);
                return Future.succeededFuture(
                    JSONRPCResponse.error(
                        request.id(),
                        new JSONRPCError(ErrorCodes.INTERNAL_ERROR, "Internal error")));
              })
          .onSuccess(response -> send(ctx, response));
    } else if (message instanceof JSONRPCNotification notification) {
      toFuture(
              ctx,
              transportContext,
              () -> mcpHandler.handleNotification(transportContext, notification))
          .onComplete(
              result -> {
                if (result.failed()) {
                  log.warn(
                      "Failed to handle MCP notification {}",
                      notification.method(),
                      result.cause());
                }
                end(ctx, 202);
              });
    } else {
      // A response to a server-to-client request, which this server never sends
      end(ctx, 202);
    }
  }

  /** Handles a GET to the MCP endpoint, which would open a server-to-client stream. */
  public void handleGet(RoutingContext ctx) {
    if (!ctx.response().closed()) {
      ctx.response().setStatusCode(405).putHeader(HttpHeaders.ALLOW, "POST").end();
    }
  }

  /**
   * Runs the handler and completes on the request's Vert.x context, so that the response is written
   * there. A handler that throws, rather than return an error, completes with that error.
   */
  private static <T> Future<T> toFuture(
      RoutingContext ctx, McpTransportContext transportContext, Supplier<Mono<T>> handler) {
    var completion =
        Mono.defer(handler)
            .contextWrite(context -> context.put(McpTransportContext.KEY, transportContext))
            .toFuture();
    return Future.fromCompletionStage(completion, ctx.vertx().getOrCreateContext());
  }

  /**
   * Answers a message that could not be read with a JSON-RPC error. Its id is null, as JSON-RPC
   * requires when the id is unknown, which {@link JSONRPCResponse} does not allow.
   */
  private void sendError(RoutingContext ctx, int code, String message) {
    var response = new LinkedHashMap<String, Object>();
    response.put("jsonrpc", McpSchema.JSONRPC_VERSION);
    response.put("id", null);
    response.put("error", new JSONRPCError(code, message));
    ctx.response().setStatusCode(400);
    send(ctx, response);
  }

  /**
   * Sends the JSON-RPC message with the response's status, which is 200 unless the request handling
   * set another one, such as 403 for a token without a claim the operation requires.
   */
  private void send(RoutingContext ctx, Object response) {
    if (ctx.response().closed()) {
      return;
    }
    final String json;
    try {
      json = jsonMapper.writeValueAsString(response);
    } catch (IOException e) {
      log.error("Failed to serialize MCP response", e);
      end(ctx, 500);
      return;
    }
    ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON).end(json);
  }

  private static void end(RoutingContext ctx, int statusCode) {
    if (!ctx.response().closed()) {
      ctx.response().setStatusCode(statusCode).end();
    }
  }
}
