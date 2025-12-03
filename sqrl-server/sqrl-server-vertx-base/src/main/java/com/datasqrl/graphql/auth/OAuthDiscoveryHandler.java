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

import com.datasqrl.graphql.config.OAuthDiscoveryConfig;
import com.datasqrl.graphql.config.ServerConfig;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Handler for OAuth 2.0 Protected Resource Metadata (RFC 9728). This enables MCP clients like
 * Claude Code to discover the OAuth authorization server.
 */
@Slf4j
@RequiredArgsConstructor
public class OAuthDiscoveryHandler {

  private static final List<String> DEFAULT_SCOPES = List.of("mcp:tools", "mcp:resources");

  private final ServerConfig config;

  /**
   * Registers OAuth discovery routes on the router. Only registers if OAuth configuration is
   * present.
   */
  public void registerRoutes(Router router) {
    var oauth2Options = config.getOauth2Options();
    if (oauth2Options == null) {
      log.debug("OAuth2 options not present, skipping discovery endpoints");
      return;
    }

    if (oauth2Options.getSite() == null || oauth2Options.getSite().isBlank()) {
      log.warn("OAuth2 options present but site is missing, skipping discovery endpoints");
      return;
    }

    log.info("Registering OAuth discovery endpoints for site: {}", oauth2Options.getSite());

    router
        .get("/.well-known/oauth-protected-resource")
        .handler(this::handleProtectedResourceMetadata);
  }

  /**
   * Returns OAuth 2.0 Protected Resource Metadata per RFC 9728. This tells MCP clients which
   * authorization server to use.
   */
  private void handleProtectedResourceMetadata(RoutingContext ctx) {
    var oauth2Options = config.getOauth2Options();
    var discoveryConfig = config.getOauthDiscovery();

    var resource = determineResourceUri(ctx, discoveryConfig);
    var authServer = getAuthorizationServerUrl(oauth2Options, discoveryConfig);
    var scopes = getScopes(discoveryConfig);

    var metadata =
        new JsonObject()
            .put("resource", resource)
            .put("authorization_servers", new JsonArray().add(authServer))
            .put("scopes_supported", new JsonArray(scopes))
            .put("bearer_methods_supported", new JsonArray().add("header"));

    log.debug("Returning protected resource metadata: {}", metadata.encodePrettily());

    ctx.response()
        .putHeader("Content-Type", "application/json")
        .putHeader("Cache-Control", "public, max-age=3600")
        .end(metadata.encode());
  }

  private String getAuthorizationServerUrl(
      OAuth2Options oauth2Options, OAuthDiscoveryConfig discoveryConfig) {
    if (discoveryConfig != null
        && discoveryConfig.getAuthorizationServerUrl() != null
        && !discoveryConfig.getAuthorizationServerUrl().isBlank()) {
      return discoveryConfig.getAuthorizationServerUrl();
    }
    var site = oauth2Options.getSite();
    return site.endsWith("/") ? site : site + "/";
  }

  private List<String> getScopes(OAuthDiscoveryConfig discoveryConfig) {
    if (discoveryConfig != null && discoveryConfig.getScopesSupported() != null) {
      return discoveryConfig.getScopesSupported();
    }
    return DEFAULT_SCOPES;
  }

  private String determineResourceUri(RoutingContext ctx, OAuthDiscoveryConfig discoveryConfig) {
    if (discoveryConfig != null
        && discoveryConfig.getResource() != null
        && !discoveryConfig.getResource().isBlank()) {
      return discoveryConfig.getResource();
    }

    var host = ctx.request().getHeader("Host");
    var forwardedProto = ctx.request().getHeader("X-Forwarded-Proto");
    var scheme = forwardedProto != null ? forwardedProto : ctx.request().scheme();
    var mcpEndpoint = config.getServletConfig().getMcpEndpoint("v1");

    return scheme + "://" + host + mcpEndpoint;
  }
}
