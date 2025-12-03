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
package com.datasqrl.graphql.auth;

import com.datasqrl.graphql.config.OAuthConfig;
import com.datasqrl.graphql.config.ServerConfig;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Handler for OAuth 2.0 Protected Resource Metadata (RFC 9728). This enables MCP clients like
 * Claude Code to discover the OAuth authorization server.
 */
@Slf4j
@RequiredArgsConstructor
public class OAuthDiscoveryHandler {

  private final ServerConfig config;

  /**
   * Registers OAuth discovery routes on the router. Only registers if OAuth configuration is
   * present.
   */
  public void registerRoutes(Router router) {
    var oauthConfig = config.getOauthConfig();
    if (oauthConfig == null) {
      log.debug("OAuth config not present, skipping discovery endpoints");
      return;
    }

    if (oauthConfig.getIssuer() == null || oauthConfig.getIssuer().isBlank()) {
      log.warn("OAuth config present but issuer is missing, skipping discovery endpoints");
      return;
    }

    log.info("Registering OAuth discovery endpoints for issuer: {}", oauthConfig.getIssuer());

    router
        .get("/.well-known/oauth-protected-resource")
        .handler(this::handleProtectedResourceMetadata);
  }

  /**
   * Returns OAuth 2.0 Protected Resource Metadata per RFC 9728. This tells MCP clients which
   * authorization server to use.
   */
  private void handleProtectedResourceMetadata(RoutingContext ctx) {
    var oauthConfig = config.getOauthConfig();

    var resource = determineResourceUri(ctx, oauthConfig);

    var metadata =
        new JsonObject()
            .put("resource", resource)
            .put(
                "authorization_servers",
                new JsonArray().add(oauthConfig.getEffectiveAuthorizationServer()))
            .put("scopes_supported", new JsonArray(oauthConfig.getScopesSupported()))
            .put("bearer_methods_supported", new JsonArray().add("header"));

    log.debug("Returning protected resource metadata: {}", metadata.encodePrettily());

    ctx.response()
        .putHeader("Content-Type", "application/json")
        .putHeader("Cache-Control", "public, max-age=3600")
        .end(metadata.encode());
  }

  private String determineResourceUri(RoutingContext ctx, OAuthConfig oauthConfig) {
    if (oauthConfig.getResource() != null && !oauthConfig.getResource().isBlank()) {
      return oauthConfig.getResource();
    }

    var host = ctx.request().getHeader("Host");
    var forwardedProto = ctx.request().getHeader("X-Forwarded-Proto");
    var scheme = forwardedProto != null ? forwardedProto : ctx.request().scheme();
    var mcpEndpoint = config.getServletConfig().getMcpEndpoint("v1");

    return scheme + "://" + host + mcpEndpoint;
  }
}
