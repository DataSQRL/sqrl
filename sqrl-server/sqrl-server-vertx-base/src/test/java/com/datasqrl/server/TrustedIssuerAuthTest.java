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
package com.datasqrl.server;

import static com.datasqrl.server.auth.TestJwtKeys.claims;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.server.auth.AuthProviderFactory;
import com.datasqrl.server.auth.TestJwtKeys;
import com.datasqrl.server.config.ServerConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.StringSchema;
import graphql.GraphQL;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TrustedIssuerAuthTest {

  private static final String ISSUER_A = "https://login.example.com/";
  private static final String ISSUER_B = "https://tenant.auth.example.com/";
  private static final String ISSUER_C = "https://attacker.example.com/";
  private static final String AUDIENCE_X = "https://app.example.com";
  private static final String AUDIENCE_Y = "https://api.example.com/";

  private Vertx vertx;
  private HttpServer httpServer;
  private TestJwtKeys keyA;
  private TestJwtKeys keyB;

  @BeforeEach
  void setup() {
    vertx = Vertx.vertx();
    keyA = TestJwtKeys.hmac(vertx, "secret-of-issuer-a-at-least-256-bits-long");
    keyB = TestJwtKeys.hmac(vertx, "secret-of-issuer-b-at-least-256-bits-long");
  }

  @AfterEach
  void teardown() {
    await(vertx.close());
  }

  @ParameterizedTest(name = "iss={0} aud={1} signedBy={2} -> {3}")
  @CsvSource({
    ISSUER_A + "," + AUDIENCE_X + ",A,200",
    ISSUER_B + "," + AUDIENCE_Y + ",B,200",
    ISSUER_A + "," + AUDIENCE_Y + ",A,401",
    ISSUER_B + "," + AUDIENCE_X + ",B,401",
    ISSUER_B + "," + AUDIENCE_Y + ",A,401",
    ISSUER_A + "," + AUDIENCE_X + ",B,401",
    ISSUER_C + "," + AUDIENCE_X + ",A,401",
    "," + AUDIENCE_X + ",A,401",
    ISSUER_A + ",,A,401"
  })
  void givenTwoTrustedIssuers_whenTokenPresented_thenIssuerAudienceAndKeyMustAllMatchOneIssuer(
      String issuer, String audience, String signedBy, int expectedStatus) {
    startServer(
        auth(
            Map.of("algorithms", List.of("HS256")),
            issuer(ISSUER_A, AUDIENCE_X, keyA),
            issuer(ISSUER_B, AUDIENCE_Y, keyB)));
    var key = "A".equals(signedBy) ? keyA : keyB;

    assertThat(queryStatus(key.token(claims(issuer, audience)))).isEqualTo(expectedStatus);
  }

  @Test
  void givenOctKeyOfSecret_whenTokenSignedWithSameRawSecret_thenAccepted() {
    var secret = "mySuperSecretSignerStringThatIsLongEnough";
    Map<String, Object> legacyJwtAuth =
        Map.of(
            "pubSecKeys",
            List.of(
                Map.of(
                    "algorithm",
                    "HS256",
                    "buffer",
                    Base64.getEncoder().encodeToString(secret.getBytes(UTF_8)))));
    var legacySigner = JWTAuth.create(vertx, new JWTAuthOptions(new JsonObject(legacyJwtAuth)));
    var entry = issuer(ISSUER_A, AUDIENCE_X, TestJwtKeys.hmac(vertx, secret));
    startServer(auth(Map.of("algorithms", List.of("HS256")), entry));

    var token =
        legacySigner.generateToken(
            new JsonObject(claims(ISSUER_A, AUDIENCE_X)), new JWTOptions().setAlgorithm("HS256"));

    assertThat(queryStatus(token)).isEqualTo(200);
  }

  @Test
  void givenDefaultAlgorithms_whenHs256TokenSignedWithTrustedSecret_thenRejected() {
    var rsa = TestJwtKeys.rsa(vertx);
    var entry = issuer(ISSUER_A, AUDIENCE_X, rsa);
    entry.put("keys", List.of(rsa.publicJwk().getMap(), keyA.publicJwk().getMap()));
    startServer(auth(Map.of(), entry));

    assertThat(queryStatus(rsa.token(claims(ISSUER_A, AUDIENCE_X)))).isEqualTo(200);
    assertThat(queryStatus(keyA.token(claims(ISSUER_A, AUDIENCE_X)))).isEqualTo(401);
  }

  @Test
  void givenIssuerAlgorithmsOverride_whenTokensPresented_thenEachIssuerUsesItsOwnAlgorithms() {
    var rsa = TestJwtKeys.rsa(vertx);
    var hmacIssuer = issuer(ISSUER_B, AUDIENCE_Y, keyB);
    hmacIssuer.put("algorithms", List.of("HS256"));
    startServer(auth(Map.of(), issuer(ISSUER_A, AUDIENCE_X, rsa), hmacIssuer));

    assertThat(queryStatus(rsa.token(claims(ISSUER_A, AUDIENCE_X)))).isEqualTo(200);
    assertThat(queryStatus(keyB.token(claims(ISSUER_B, AUDIENCE_Y)))).isEqualTo(200);
  }

  @ParameterizedTest(name = "auth.leeway={0} issuer.leeway={1} expiredSecondsAgo={2} -> {3}")
  @CsvSource({
    ",,30,200",
    ",,120,401",
    "10,,30,401",
    "180,,120,200",
    "10,180,120,200",
    "180,10,30,401"
  })
  void givenLeewaySettings_whenExpiredTokenPresented_thenIssuerThenAuthThenDefaultLeewayApplies(
      Integer authLeeway, Integer issuerLeeway, int expiredSecondsAgo, int expectedStatus) {
    var defaults = new HashMap<String, Object>();
    defaults.put("algorithms", List.of("HS256"));
    if (authLeeway != null) {
      defaults.put("leeway", authLeeway);
    }
    var entry = issuer(ISSUER_A, AUDIENCE_X, keyA);
    if (issuerLeeway != null) {
      entry.put("leeway", issuerLeeway);
    }
    startServer(auth(defaults, entry));

    var token =
        keyA.token(claims(ISSUER_A, AUDIENCE_X, Instant.now().minusSeconds(expiredSecondsAgo)));

    assertThat(queryStatus(token)).isEqualTo(expectedStatus);
  }

  @SafeVarargs
  private static Map<String, Object> auth(
      Map<String, Object> defaults, Map<String, Object>... issuers) {
    var auth = new HashMap<>(defaults);
    auth.put("issuers", List.of(issuers));
    return auth;
  }

  private static Map<String, Object> issuer(String issuer, String audience, TestJwtKeys key) {
    var entry = new HashMap<String, Object>();
    entry.put("issuer", issuer);
    entry.put("audiences", List.of(audience));
    entry.put("keys", List.of(key.publicJwk().getMap()));
    return entry;
  }

  private void startServer(Map<String, Object> auth) {
    var config = new ServerConfig();
    config.setAuth(auth);
    List<AuthenticationProvider> authProviders =
        await(AuthProviderFactory.createAuthProviders(vertx, config.validated().getAuthConfig()));
    var model =
        RootGraphQLModel.builder()
            .schema(StringSchema.builder().schema("type Query { mock: String }").build())
            .build();
    var router = Router.router(vertx);
    router.route().handler(BodyHandler.create());

    await(
        vertx.deployVerticle(new TestGraphQLServerVerticle(router, config, model, authProviders)));
    httpServer = await(vertx.createHttpServer().requestHandler(router).listen(0));
  }

  private int queryStatus(String token) {
    var response =
        await(
            WebClient.create(vertx)
                .post(httpServer.actualPort(), "localhost", "/v1/graphql")
                .putHeader("Authorization", "Bearer " + token)
                .sendJson(new JsonObject().put("query", "query { mock }")));
    return response.statusCode();
  }

  @SneakyThrows
  private static <T> T await(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  private static class TestGraphQLServerVerticle extends GraphQLServerVerticle {

    TestGraphQLServerVerticle(
        Router router,
        ServerConfig config,
        RootGraphQLModel model,
        List<AuthenticationProvider> authProviders) {
      super(router, config, "v1", model, authProviders, Optional.empty());
    }

    @Override
    protected GraphQL createGraphQLEngine(SubscriptionConfigurationImpl subscriptionConfig) {
      var schema =
          new SchemaGenerator()
              .makeExecutableSchema(
                  new SchemaParser().parse("type Query { mock: String }"),
                  RuntimeWiring.newRuntimeWiring().build());
      return GraphQL.newGraphQL(schema).build();
    }
  }
}
