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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.graphql.config.OAuthConfig;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.config.ServletConfig;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class OAuthDiscoveryHandlerTest {

  private ServerConfig serverConfig;
  private Router router;
  private Route route;

  @BeforeEach
  void setUp() {
    serverConfig = mock(ServerConfig.class);
    router = mock(Router.class);
    route = mock(Route.class);

    when(router.get(any(String.class))).thenReturn(route);
    when(route.handler(any())).thenReturn(route);

    var servletConfig = new ServletConfig();
    when(serverConfig.getServletConfig()).thenReturn(servletConfig);
  }

  @Test
  void givenNoOAuthConfig_whenRegisterRoutes_thenNoRoutesAdded() {
    when(serverConfig.getOauthConfig()).thenReturn(null);

    var handler = new OAuthDiscoveryHandler(serverConfig);
    handler.registerRoutes(router);

    verify(router, never()).get(any(String.class));
  }

  @Test
  void givenOAuthConfigWithNullSite_whenRegisterRoutes_thenNoRoutesAdded() {
    var oauthConfig = new OAuthConfig();
    when(serverConfig.getOauthConfig()).thenReturn(oauthConfig);

    var handler = new OAuthDiscoveryHandler(serverConfig);
    handler.registerRoutes(router);

    verify(router, never()).get(any(String.class));
  }

  @Test
  void givenValidOAuthConfig_whenRegisterRoutes_thenDiscoveryRouteAdded() {
    var oauthConfig = createOAuthConfig("https://auth.example.com");
    when(serverConfig.getOauthConfig()).thenReturn(oauthConfig);

    var handler = new OAuthDiscoveryHandler(serverConfig);
    handler.registerRoutes(router);

    verify(router).get(eq("/.well-known/oauth-protected-resource"));
    verify(route).handler(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void givenValidOAuthConfig_whenFetchDiscoveryEndpoint_thenReturnsValidMetadata() {
    var oauthConfig = createOAuthConfig("https://auth.example.com");
    oauthConfig.setScopesSupported(List.of("mcp:tools", "mcp:resources"));
    when(serverConfig.getOauthConfig()).thenReturn(oauthConfig);

    var handler = new OAuthDiscoveryHandler(serverConfig);
    handler.registerRoutes(router);

    var handlerCaptor = ArgumentCaptor.forClass(Handler.class);
    verify(route).handler(handlerCaptor.capture());

    var ctx = mockRoutingContext();
    var responseBody = ArgumentCaptor.forClass(String.class);
    when(ctx.response().end(responseBody.capture())).thenReturn(null);

    handlerCaptor.getValue().handle(ctx);

    var json = responseBody.getValue();
    assertThat(json).contains("authorization_servers");
    assertThat(json).contains("https://auth.example.com/");
    assertThat(json).contains("scopes_supported");
    assertThat(json).contains("mcp:tools");
    assertThat(json).contains("bearer_methods_supported");
    assertThat(json).contains("header");
  }

  @Test
  @SuppressWarnings("unchecked")
  void givenExternalAuthUrl_whenFetchDiscoveryEndpoint_thenUsesExternalUrl() {
    var oauthConfig = createOAuthConfig("http://internal:8080");
    oauthConfig.setAuthorizationServerUrl("https://external.example.com/");
    when(serverConfig.getOauthConfig()).thenReturn(oauthConfig);

    var handler = new OAuthDiscoveryHandler(serverConfig);
    handler.registerRoutes(router);

    var handlerCaptor = ArgumentCaptor.forClass(Handler.class);
    verify(route).handler(handlerCaptor.capture());

    var ctx = mockRoutingContext();
    var responseBody = ArgumentCaptor.forClass(String.class);
    when(ctx.response().end(responseBody.capture())).thenReturn(null);

    handlerCaptor.getValue().handle(ctx);

    var json = responseBody.getValue();
    assertThat(json).contains("https://external.example.com/");
    assertThat(json).doesNotContain("http://internal:8080");
  }

  private OAuthConfig createOAuthConfig(String site) {
    var oauthConfig = new OAuthConfig();
    var oauth2Options = new OAuth2Options().setSite(site);
    oauthConfig.setOauth2Options(oauth2Options);
    return oauthConfig;
  }

  private RoutingContext mockRoutingContext() {
    var ctx = mock(RoutingContext.class);
    var request = mock(HttpServerRequest.class);
    var response = mock(HttpServerResponse.class);

    when(ctx.request()).thenReturn(request);
    when(ctx.response()).thenReturn(response);
    when(request.getHeader("Host")).thenReturn("localhost:8888");
    when(request.getHeader("X-Forwarded-Proto")).thenReturn(null);
    when(request.scheme()).thenReturn("http");
    when(response.putHeader(any(String.class), any(String.class))).thenReturn(response);

    return ctx;
  }
}
