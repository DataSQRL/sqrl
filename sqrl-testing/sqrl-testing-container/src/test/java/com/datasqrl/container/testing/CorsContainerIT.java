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
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Test;

/**
 * Container integration tests for CORS (Cross-Origin Resource Sharing) behaviour of the Vert.x
 * sqrl-server.
 *
 * <p>Scenarios covered:
 *
 * <ul>
 *   <li>Preflight OPTIONS from wildcard origins (production, staging, localhost, arbitrary)
 *   <li>Preflight with individual request headers: Authorization, Content-Type, custom
 *   <li>Preflight with multiple combined request headers
 *   <li>OPTIONS explicitly listed in allowed methods
 *   <li>Actual cross-origin POST / GET – response carries Allow-Origin header
 *   <li>Request with no Origin header – request succeeds normally (no CORS check)
 *   <li>Specific-origin config: allowed origin → preflight + request accepted
 *   <li>Specific-origin config: unlisted origin → preflight rejected (403)
 * </ul>
 */
@Slf4j
public class CorsContainerIT extends SqrlContainerTestBase {

  private static final String PRODUCTION_ORIGIN = "https://dev.datasqrl.com";
  private static final String STAGING_ORIGIN =
      "https://sqrl-repository-frontend-git-staging-datasqrl.vercel.app";
  private static final String LOCAL_ORIGIN = "http://localhost:3000";
  private static final String ARBITRARY_ORIGIN = "https://some-app.example.com";
  private static final String GRAPHQL_QUERY = "{\"query\":\"query { __typename }\"}";

  @Override
  protected String getTestCaseName() {
    return "avro-schema";
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Preflight (OPTIONS) — wildcard origin config
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @SneakyThrows
  void given_wildcardOriginConfig_when_preflightFromKnownFrontendOrigins_then_corsHeadersPresent() {
    compileAndStartServer(testDir);

    assertPreflightAllowed(PRODUCTION_ORIGIN, "POST", "Content-Type");
    assertPreflightAllowed(STAGING_ORIGIN, "POST", "Content-Type");
    assertPreflightAllowed(LOCAL_ORIGIN, "POST", "Content-Type, Authorization");
    assertPreflightAllowed(ARBITRARY_ORIGIN, "POST", "Content-Type");
  }

  @Test
  @SneakyThrows
  void given_wildcardOriginConfig_when_preflightWithAuthorizationHeader_then_headerAllowed() {
    compileAndStartServer(testDir);

    assertPreflightAllowed(PRODUCTION_ORIGIN, "POST", "Authorization");
  }

  @Test
  @SneakyThrows
  void given_wildcardOriginConfig_when_preflightWithContentTypeHeader_then_headerAllowed() {
    compileAndStartServer(testDir);

    assertPreflightAllowed(PRODUCTION_ORIGIN, "POST", "Content-Type");
  }

  @Test
  @SneakyThrows
  void given_wildcardOriginConfig_when_preflightWithCustomHeader_then_headerAllowed() {
    compileAndStartServer(testDir);

    assertPreflightAllowed(PRODUCTION_ORIGIN, "POST", "X-Custom-Header");
  }

  @Test
  @SneakyThrows
  void
      given_wildcardOriginConfig_when_preflightWithMultipleRequestHeaders_then_allHeadersAllowed() {
    compileAndStartServer(testDir);

    assertPreflightAllowed(PRODUCTION_ORIGIN, "POST", "Content-Type, Authorization");
  }

  @Test
  @SneakyThrows
  void given_wildcardOriginConfig_when_preflightForGetMethod_then_getAllowed() {
    compileAndStartServer(testDir);

    assertPreflightAllowed(PRODUCTION_ORIGIN, "GET", "Content-Type");
  }

  @Test
  @SneakyThrows
  void given_wildcardOriginConfig_when_preflightRequested_then_optionsInAllowedMethods() {
    compileAndStartServer(testDir);

    var request = new HttpOptions(getGraphQLEndpoint());
    request.setHeader("Origin", PRODUCTION_ORIGIN);
    request.setHeader("Access-Control-Request-Method", "POST");

    var response = sharedHttpClient.execute(request);

    var allowMethods = response.getFirstHeader("Access-Control-Allow-Methods");
    assertThat(allowMethods).as("Access-Control-Allow-Methods header must be present").isNotNull();
    assertThat(allowMethods.getValue())
        .as("OPTIONS must be explicitly listed in allowed methods for preflight to work")
        .containsIgnoringCase("OPTIONS");
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Actual cross-origin requests (non-preflight) — wildcard origin config
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @SneakyThrows
  void given_wildcardOriginConfig_when_graphqlPostWithOriginHeader_then_allowOriginInResponse() {
    compileAndStartServer(testDir);

    var request = new HttpPost(getGraphQLEndpoint());
    request.setHeader("Origin", PRODUCTION_ORIGIN);
    request.setEntity(new StringEntity(GRAPHQL_QUERY, ContentType.APPLICATION_JSON));

    var response = sharedHttpClient.execute(request);

    assertSoftly(
        softly -> {
          softly
              .assertThat(response.getStatusLine().getStatusCode())
              .as("Cross-origin GraphQL POST should succeed")
              .isEqualTo(200);
          softly
              .assertThat(response.getFirstHeader("Access-Control-Allow-Origin"))
              .as("Access-Control-Allow-Origin must be present on actual request response")
              .isNotNull();
          softly
              .assertThat(response.getFirstHeader("Access-Control-Allow-Origin").getValue())
              .as("Wildcard config must return '*' as allowed origin")
              .isEqualTo("*");
        });

    validateBasicGraphQLResponse(response);
  }

  @Test
  @SneakyThrows
  void given_wildcardOriginConfig_when_graphqlPostFromMultipleOrigins_then_allAllowed() {
    compileAndStartServer(testDir);

    for (var origin :
        new String[] {PRODUCTION_ORIGIN, STAGING_ORIGIN, LOCAL_ORIGIN, ARBITRARY_ORIGIN}) {
      var request = new HttpPost(getGraphQLEndpoint());
      request.setHeader("Origin", origin);
      request.setEntity(new StringEntity(GRAPHQL_QUERY, ContentType.APPLICATION_JSON));

      var response = sharedHttpClient.execute(request);
      assertSoftly(
          softly -> {
            softly
                .assertThat(response.getStatusLine().getStatusCode())
                .as("Cross-origin POST from %s should succeed", origin)
                .isEqualTo(200);
            softly
                .assertThat(response.getFirstHeader("Access-Control-Allow-Origin"))
                .as("Access-Control-Allow-Origin must be present for %s", origin)
                .isNotNull();
          });
    }
  }

  @Test
  @SneakyThrows
  void given_wildcardOriginConfig_when_getRequestWithOriginHeader_then_allowOriginInResponse() {
    compileAndStartServer(testDir);

    // Health endpoint is a simple GET that doesn't require auth
    var request = new HttpGet(getHealthEndpoint());
    request.setHeader("Origin", PRODUCTION_ORIGIN);

    var response = sharedHttpClient.execute(request);

    assertSoftly(
        softly -> {
          softly
              .assertThat(response.getStatusLine().getStatusCode())
              .as("Cross-origin GET to health endpoint should succeed")
              .isIn(200, 204);
          softly
              .assertThat(response.getFirstHeader("Access-Control-Allow-Origin"))
              .as("Access-Control-Allow-Origin must be present on GET response")
              .isNotNull();
        });
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Non-CORS request (no Origin header)
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @SneakyThrows
  void given_wildcardOriginConfig_when_requestWithoutOriginHeader_then_requestSucceeds() {
    compileAndStartServer(testDir);

    var response = executeGraphQLQuery(GRAPHQL_QUERY);

    assertThat(response.getStatusLine().getStatusCode())
        .as("Direct request without Origin header must always succeed")
        .isEqualTo(200);

    validateBasicGraphQLResponse(response);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Specific allowed-origins config
  // ─────────────────────────────────────────────────────────────────────────

  @Test
  @SneakyThrows
  void given_specificOriginsConfig_when_requestFromAllowedOrigin_then_originEchoedInResponse() {
    compileSqrlProject(testDir);
    configureSpecificOrigins(testDir, PRODUCTION_ORIGIN, STAGING_ORIGIN, LOCAL_ORIGIN);
    startGraphQLServer(testDir);

    var request = new HttpPost(getGraphQLEndpoint());
    request.setHeader("Origin", PRODUCTION_ORIGIN);
    request.setEntity(new StringEntity(GRAPHQL_QUERY, ContentType.APPLICATION_JSON));

    var response = sharedHttpClient.execute(request);

    assertSoftly(
        softly -> {
          softly
              .assertThat(response.getStatusLine().getStatusCode())
              .as("Request from allowed origin should succeed")
              .isEqualTo(200);
          softly
              .assertThat(response.getFirstHeader("Access-Control-Allow-Origin"))
              .as("Access-Control-Allow-Origin must be present for allowed origin")
              .isNotNull();
          softly
              .assertThat(response.getFirstHeader("Access-Control-Allow-Origin").getValue())
              .as("Server must echo back the specific allowed origin (not wildcard)")
              .isEqualTo(PRODUCTION_ORIGIN);
        });
  }

  @Test
  @SneakyThrows
  void given_specificOriginsConfig_when_preflightFromAllowedOrigin_then_preflightAccepted() {
    compileSqrlProject(testDir);
    configureSpecificOrigins(testDir, PRODUCTION_ORIGIN, STAGING_ORIGIN, LOCAL_ORIGIN);
    startGraphQLServer(testDir);

    assertPreflightAllowed(STAGING_ORIGIN, "POST", "Content-Type, Authorization");
  }

  @Test
  @SneakyThrows
  void given_specificOriginsConfig_when_preflightFromDisallowedOrigin_then_preflightRejected() {
    compileSqrlProject(testDir);
    configureSpecificOrigins(testDir, PRODUCTION_ORIGIN);
    startGraphQLServer(testDir);

    var request = new HttpOptions(getGraphQLEndpoint());
    request.setHeader("Origin", ARBITRARY_ORIGIN);
    request.setHeader("Access-Control-Request-Method", "POST");
    request.setHeader("Access-Control-Request-Headers", "Content-Type");

    var response = sharedHttpClient.execute(request);

    assertThat(response.getStatusLine().getStatusCode())
        .as("Preflight from an origin not in the allowed list must be rejected with 403")
        .isEqualTo(403);
  }

  @Test
  @SneakyThrows
  void given_specificOriginsConfig_when_simpleRequestFromDisallowedOrigin_then_noCorsHeader() {
    compileSqrlProject(testDir);
    configureSpecificOrigins(testDir, PRODUCTION_ORIGIN);
    startGraphQLServer(testDir);

    var request = new HttpPost(getGraphQLEndpoint());
    request.setHeader("Origin", ARBITRARY_ORIGIN);
    request.setEntity(new StringEntity(GRAPHQL_QUERY, ContentType.APPLICATION_JSON));

    var response = sharedHttpClient.execute(request);

    // The server still processes the request, but the browser would block it because
    // Access-Control-Allow-Origin is absent or does not match the requesting origin.
    var allowOrigin = response.getFirstHeader("Access-Control-Allow-Origin");
    if (allowOrigin != null) {
      assertThat(allowOrigin.getValue())
          .as("If Allow-Origin is present it must NOT match the disallowed origin")
          .isNotEqualTo(ARBITRARY_ORIGIN);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Helpers
  // ─────────────────────────────────────────────────────────────────────────

  @SneakyThrows
  private void assertPreflightAllowed(String origin, String requestMethod, String requestHeaders) {
    var request = new HttpOptions(getGraphQLEndpoint());
    request.setHeader("Origin", origin);
    request.setHeader("Access-Control-Request-Method", requestMethod);
    request.setHeader("Access-Control-Request-Headers", requestHeaders);

    HttpResponse response = sharedHttpClient.execute(request);
    log.debug(
        "Preflight {} origin={} method={} headers={} → status={}",
        getGraphQLEndpoint(),
        origin,
        requestMethod,
        requestHeaders,
        response.getStatusLine().getStatusCode());

    assertSoftly(
        softly -> {
          softly
              .assertThat(response.getStatusLine().getStatusCode())
              .as(
                  "Preflight OPTIONS from origin='%s' requesting method='%s' headers='%s'"
                      + " should be accepted (200 or 204)",
                  origin, requestMethod, requestHeaders)
              .isIn(200, 204);
          softly
              .assertThat(response.getFirstHeader("Access-Control-Allow-Origin"))
              .as("Access-Control-Allow-Origin must be present in preflight response")
              .isNotNull();
          softly
              .assertThat(response.getFirstHeader("Access-Control-Allow-Methods"))
              .as("Access-Control-Allow-Methods must be present in preflight response")
              .isNotNull();
        });
  }

  /** Replaces {@code corsHandlerOptions.allowedOrigin} with a specific list of origins. */
  @SneakyThrows
  private void configureSpecificOrigins(Path testDir, String... origins) {
    var vertxConfigPath = testDir.resolve("build/deploy/plan/vertx-config.json");
    var configContent = Files.readString(vertxConfigPath);
    var configNode = (ObjectNode) objectMapper.readTree(configContent);

    var corsOptions = objectMapper.createObjectNode();
    corsOptions.putNull("allowedOrigin");
    var originsArray = objectMapper.createArrayNode();
    for (var origin : origins) {
      originsArray.add(origin);
    }
    corsOptions.set("allowedOrigins", originsArray);
    corsOptions.put("allowCredentials", false);
    corsOptions.put("maxAgeSeconds", -1);
    corsOptions.put("allowPrivateNetwork", false);
    corsOptions.set(
        "allowedMethods", objectMapper.createArrayNode().add("POST").add("GET").add("OPTIONS"));
    corsOptions.set(
        "allowedHeaders", objectMapper.createArrayNode().add("Content-Type").add("Authorization"));
    corsOptions.set("exposedHeaders", objectMapper.createArrayNode());

    configNode.set("corsHandlerOptions", corsOptions);
    Files.writeString(
        vertxConfigPath,
        objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(configNode));

    log.info("Configured specific allowed origins: {}", (Object) origins);
  }
}
