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
package com.datasqrl.server.auth;

import static com.datasqrl.server.auth.ProtectedResourceUrlsTest.request;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.server.config.OAuthConfig;
import com.datasqrl.server.config.ServerConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.oauth2.OAuth2Options;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OAuthDiscoveryHandlerTest {

  private Vertx vertx;

  @BeforeEach
  void setup() {
    vertx = Vertx.vertx();
  }

  @AfterEach
  void teardown() {
    await(vertx.close());
  }

  @Test
  void givenAuthIssuers_whenMetadataRequested_thenIssuersAreTheAuthorizationServers() {
    var config = authConfig();

    var metadata = fetchMetadata(config);

    assertThat(metadata.getJsonArray("authorization_servers").getList())
        .containsExactly("https://login.example.com/", "https://tenant.auth0.com/");
    assertThat(metadata.getString("resource")).isEqualTo("https://api.example.com/v1/mcp");
    assertThat(metadata.getJsonArray("scopes_supported").getList()).containsExactly("mcp:tools");
  }

  @Test
  void givenDeprecatedOauthConfig_whenMetadataRequested_thenEffectiveAuthorizationServerIsListed() {
    var oauthConfig = new OAuthConfig();
    oauthConfig.setOauth2Options(new OAuth2Options().setSite("https://idp.example.com"));
    var config = new ServerConfig();
    config.setOauthConfig(oauthConfig);

    var metadata = fetchMetadata(config);

    assertThat(metadata.getJsonArray("authorization_servers").getList())
        .containsExactly("https://idp.example.com/");
    assertThat(metadata.getJsonArray("scopes_supported").getList())
        .containsExactly("mcp:tools", "mcp:resources");
  }

  @Test
  void givenAuthIssuers_whenAuthFails_thenWwwAuthenticatePointsToMetadata() {
    var response = failAuthentication(authConfig());

    verify(response)
        .putHeader(
            "WWW-Authenticate",
            "Bearer resource_metadata=\"https://api.example.com/.well-known/oauth-protected-resource\"");
  }

  @Test
  void givenOnlyInlineKeyIssuers_whenDiscoveryAndAuthFailure_thenNoMetadataAndNoWwwAuthenticate() {
    var config = new ServerConfig();
    config.setAuth(
        Map.of(
            "issuers",
            List.of(
                Map.of(
                    "issuer",
                    "my-backend",
                    "audiences",
                    List.of("api"),
                    "keys",
                    List.of(Map.of("kty", "oct", "k", "c2VjcmV0"))))));
    config.validated();

    assertThat(metadataResponse(config).statusCode()).isEqualTo(404);
    verify(failAuthentication(config), never()).putHeader(eq("WWW-Authenticate"), anyString());
  }

  private static ServerConfig authConfig() {
    var config = new ServerConfig();
    config.setAuth(
        Map.of(
            "issuers",
            List.of(
                Map.of("issuer", "https://login.example.com/", "audiences", List.of("app")),
                Map.of("issuer", "https://tenant.auth0.com/", "audiences", List.of("api"))),
            "resource",
            "https://api.example.com/v1/mcp",
            "scopesSupported",
            List.of("mcp:tools")));
    return config.validated();
  }

  private static HttpServerResponse failAuthentication(ServerConfig config) {
    var request = request("http", "api.example.com", "http");
    var response = mock(HttpServerResponse.class, RETURNS_SELF);
    var ctx = mock(RoutingContext.class);
    when(ctx.statusCode()).thenReturn(401);
    when(ctx.request()).thenReturn(request);
    when(ctx.response()).thenReturn(response);

    new OAuthFailureHandler(config.getProtectedResourceMetadata().orElse(null)).handle(ctx);
    return response;
  }

  private JsonObject fetchMetadata(ServerConfig config) {
    var response = metadataResponse(config);
    assertThat(response.statusCode()).isEqualTo(200);
    return response.bodyAsJsonObject();
  }

  private HttpResponse<Buffer> metadataResponse(ServerConfig config) {
    var router = Router.router(vertx);
    new OAuthDiscoveryHandler(config).registerRoutes(router);
    var server = await(vertx.createHttpServer().requestHandler(router).listen(0));

    return await(
        WebClient.create(vertx)
            .get(server.actualPort(), "localhost", ProtectedResourceUrls.METADATA_PATH)
            .send());
  }

  @SneakyThrows
  private static <T> T await(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }
}
