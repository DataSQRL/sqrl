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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for MCP OAuth authentication with Keycloak. Tests the full OAuth flow including
 * discovery endpoints and token validation.
 */
@Testcontainers
@Slf4j
class McpOAuthIT {
  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("oauth-authorized-compile");

  @RegisterExtension
  static KeycloakContainerExtension keycloak = new KeycloakContainerExtension(sqrl);

  private static final String REALM_NAME = "datasqrl";

  private final ObjectMapper objectMapper = new ObjectMapper();

  private static Consumer<GenericContainer<?>> keycloakEnv() {
    return c -> {
      c.withEnv("KEYCLOAK_ISSUER", KeycloakContainerExtension.issuer(REALM_NAME) + "/");
      c.withEnv("KEYCLOAK_EXTERNAL_URL", keycloak.getHostUrl() + "/realms/" + REALM_NAME + "/");
    };
  }

  @BeforeAll
  static void createRealm() {
    keycloak.createRealm(
        """
        { "realm": "%s", "enabled": true, "sslRequired": "none" }
        """
            .formatted(REALM_NAME));
  }

  @Test
  @SneakyThrows
  void givenOAuthConfig_whenFetchDiscoveryEndpoint_thenReturnsProtectedResourceMetadata() {
    var env = keycloakEnv();
    sqrl.compileSqrlProject(null, env);
    sqrl.startGraphQLServer(env);

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
    assertThat(json.get("authorization_servers").get(0).asText())
        .contains("localhost:")
        .contains("/realms/" + REALM_NAME);

    assertThat(json.has("scopes_supported")).isTrue();
    assertThat(json.get("scopes_supported").isArray()).isTrue();

    assertThat(json.has("bearer_methods_supported")).isTrue();
    assertThat(json.get("bearer_methods_supported").get(0).asText()).isEqualTo("header");

    assertThat(json.has("resource")).isTrue();
  }

  @Test
  @SneakyThrows
  void givenOAuthConfig_whenMcpRequestWithoutToken_thenReturns401WithWWWAuthenticate() {
    var env = keycloakEnv();
    sqrl.compileSqrlProject(null, env);
    sqrl.startGraphQLServer(env);

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
}
