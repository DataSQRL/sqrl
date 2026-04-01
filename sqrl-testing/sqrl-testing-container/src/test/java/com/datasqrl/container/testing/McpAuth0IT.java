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
package com.datasqrl.container.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Manual integration tests for MCP OAuth authentication with Auth0. Tests the full OAuth flow
 * including OIDC discovery, protected resource metadata, and token validation against a real Auth0
 * tenant.
 *
 * <p>Requires the following environment variables to be set:
 *
 * <ul>
 *   <li>{@code AUTH0_CLIENT_ID} - Auth0 M2M application client ID
 *   <li>{@code AUTH0_CLIENT_SECRET} - Auth0 M2M application client secret
 *   <li>{@code AUTH0_AUDIENCE} - Auth0 API identifier (audience), e.g. {@code
 *       https://my-mcp-server/}
 * </ul>
 *
 * <p>The Auth0 tenant is fixed to {@code test-x2k6ziops74yyzik.auth0.com}. If credentials are not
 * configured the tests are skipped automatically.
 */
@Disabled("Intended for manual usage")
@Testcontainers
@Slf4j
class McpAuth0IT {
  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("oauth-authorized-compile");

  private static final String AUTH0_DOMAIN = ""; // Set Auth0 domain to test
  private static final String AUTH0_ISSUER_URL = "https://" + AUTH0_DOMAIN + "/";
  private static final String AUTH0_TOKEN_URL = "https://" + AUTH0_DOMAIN + "/oauth/token";

  // Credentials are read from environment variables so they are never hard-coded.
  private static final String CLIENT_ID = System.getenv("AUTH0_CLIENT_ID");
  private static final String CLIENT_SECRET = System.getenv("AUTH0_CLIENT_SECRET");
  private static final String AUDIENCE = System.getenv("AUTH0_AUDIENCE");

  private final ObjectMapper objectMapper = new ObjectMapper();

  @BeforeEach
  void requireAuth0Credentials() {
    assumeTrue(
        CLIENT_ID != null && !CLIENT_ID.isBlank(),
        "AUTH0_CLIENT_ID env var is not set — skipping Auth0 integration tests");
    assumeTrue(
        CLIENT_SECRET != null && !CLIENT_SECRET.isBlank(),
        "AUTH0_CLIENT_SECRET env var is not set — skipping Auth0 integration tests");
    assumeTrue(
        AUDIENCE != null && !AUDIENCE.isBlank(),
        "AUTH0_AUDIENCE env var is not set — skipping Auth0 integration tests");
  }

  /**
   * Obtains an access token from Auth0 using the client credentials (M2M) grant.
   *
   * <p>Auth0 requires the {@code audience} parameter for client credentials requests; without it
   * the returned token is an opaque token rather than a JWT and cannot be validated locally.
   */
  @SneakyThrows
  private String getAuth0Token() {
    log.info("Requesting M2M access token from Auth0: {}", AUTH0_TOKEN_URL);

    var requestBody =
        String.format(
            "{\"grant_type\":\"client_credentials\",\"client_id\":\"%s\","
                + "\"client_secret\":\"%s\",\"audience\":\"%s\"}",
            CLIENT_ID, CLIENT_SECRET, AUDIENCE);

    var request = new HttpPost(AUTH0_TOKEN_URL);
    request.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON));

    String body;
    int statusCode;
    try (var response = sqrl.getHttpClient().execute(request)) {
      body = EntityUtils.toString(response.getEntity());
      statusCode = response.getStatusLine().getStatusCode();
    }

    if (statusCode != 200) {
      throw new RuntimeException("Failed to obtain Auth0 token: HTTP " + statusCode + " — " + body);
    }

    var json = objectMapper.readTree(body);
    if (json.has("error")) {
      throw new RuntimeException(
          "Auth0 token error: "
              + json.get("error").asText()
              + " — "
              + json.path("error_description").asText());
    }

    log.info("Auth0 M2M token obtained successfully");
    return json.get("access_token").asText();
  }

  private void compileAndStartServerWithAuth0() {
    Consumer<GenericContainer<?>> auth0Env =
        c -> {
          // Auth0 is a public cloud service; the issuer URL is the same from inside and outside
          // the Docker network.
          c.withEnv("KEYCLOAK_ISSUER", AUTH0_ISSUER_URL);
          c.withEnv("KEYCLOAK_EXTERNAL_URL", AUTH0_ISSUER_URL);
        };

    sqrl.compileSqrlProject(null, auth0Env);
    sqrl.startGraphQLServer(auth0Env);
  }

  @Test
  @SneakyThrows
  void givenAuth0Config_whenFetchDiscoveryEndpoint_thenReturnsProtectedResourceMetadata() {
    compileAndStartServerWithAuth0();

    var discoveryUrl = sqrl.getBaseUrl() + "/.well-known/oauth-protected-resource";
    log.info("Testing OAuth discovery endpoint: {}", discoveryUrl);

    String body;
    try (var response = sqrl.getHttpClient().execute(new HttpGet(discoveryUrl))) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
      body = EntityUtils.toString(response.getEntity());
    }

    log.info("Discovery response: {}", body);

    var json = objectMapper.readTree(body);

    assertThat(json.has("authorization_servers")).isTrue();
    assertThat(json.get("authorization_servers").isArray()).isTrue();
    assertThat(json.get("authorization_servers").get(0).asText()).contains(AUTH0_DOMAIN);

    assertThat(json.has("scopes_supported")).isTrue();
    assertThat(json.get("scopes_supported").isArray()).isTrue();

    assertThat(json.has("bearer_methods_supported")).isTrue();
    assertThat(json.get("bearer_methods_supported").get(0).asText()).isEqualTo("header");

    assertThat(json.has("resource")).isTrue();
  }

  @Test
  @SneakyThrows
  void givenAuth0Config_whenMcpRequestWithoutToken_thenReturns401WithWWWAuthenticate() {
    compileAndStartServerWithAuth0();

    var mcpUrl = sqrl.getBaseUrl() + "/v1/mcp";
    log.info("Testing MCP endpoint without token: {}", mcpUrl);

    var request = new HttpPost(mcpUrl);
    request.setEntity(
        new StringEntity(
            "{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\",\"id\":1}",
            ContentType.APPLICATION_JSON));

    try (var response = sqrl.getHttpClient().execute(request)) {
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(401);

      var wwwAuth = response.getFirstHeader("WWW-Authenticate");
      assertThat(wwwAuth).isNotNull();
      assertThat(wwwAuth.getValue()).contains("resource_metadata=");
      assertThat(wwwAuth.getValue()).contains(".well-known/oauth-protected-resource");
    }
  }

  @Test
  @SneakyThrows
  void givenAuth0Config_whenMcpRequestWithValidToken_thenReturns200() {
    compileAndStartServerWithAuth0();

    var token = getAuth0Token();

    var mcpUrl = sqrl.getBaseUrl() + "/v1/mcp";
    log.info("Testing MCP endpoint with valid Auth0 token: {}", mcpUrl);

    var request = new HttpPost(mcpUrl);
    request.setHeader("Authorization", "Bearer " + token);
    request.setEntity(
        new StringEntity(
            "{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\",\"id\":1}",
            ContentType.APPLICATION_JSON));

    String body;
    try (var response = sqrl.getHttpClient().execute(request)) {
      body = EntityUtils.toString(response.getEntity());
      log.info("MCP response status: {}, body: {}", response.getStatusLine().getStatusCode(), body);
      assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    }

    var json = objectMapper.readTree(body);
    assertThat(json.has("result")).isTrue();
    assertThat(json.get("result").has("tools")).isTrue();
  }
}
