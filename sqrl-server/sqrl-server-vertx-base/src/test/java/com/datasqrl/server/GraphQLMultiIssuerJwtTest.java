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

import static org.assertj.core.api.Assertions.assertThat;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GraphQLMultiIssuerJwtTest {

  private static final String SECRET = "dGVzdA==";
  private static final String ISSUER_A = "https://login.example.com/";
  private static final String ISSUER_B = "https://tenant.auth.example.com/";
  private static final String ISSUER_C = "https://attacker.example.com/";
  private static final String AUDIENCE = "my-api";

  private Vertx vertx;
  private HttpServer httpServer;
  private JWTAuth signer;

  @BeforeEach
  void setup() {
    vertx = Vertx.vertx();
    signer =
        JWTAuth.create(
            vertx, new JWTAuthOptions(new JsonObject().put("pubSecKeys", List.of(hs256Key()))));
  }

  @AfterEach
  void teardown() {
    await(vertx.close());
  }

  @Test
  void givenIssuersConfig_whenTokenFromFirstIssuer_thenAccepted() {
    startServer(jwtOptions(Map.of("issuers", List.of(ISSUER_A, ISSUER_B))));

    assertThat(queryStatus(tokenFrom(ISSUER_A))).isEqualTo(200);
  }

  @Test
  void givenIssuersConfig_whenTokenFromSecondIssuer_thenAccepted() {
    startServer(jwtOptions(Map.of("issuers", List.of(ISSUER_A, ISSUER_B))));

    assertThat(queryStatus(tokenFrom(ISSUER_B))).isEqualTo(200);
  }

  @Test
  void givenIssuersConfig_whenTokenFromUnlistedIssuer_thenRejected() {
    startServer(jwtOptions(Map.of("issuers", List.of(ISSUER_A, ISSUER_B))));

    assertThat(queryStatus(tokenFrom(ISSUER_C))).isEqualTo(401);
  }

  @Test
  void givenIssuersConfig_whenTokenWithoutIssuer_thenRejected() {
    startServer(jwtOptions(Map.of("issuers", List.of(ISSUER_A, ISSUER_B))));

    assertThat(queryStatus(tokenFrom(null))).isEqualTo(401);
  }

  @Test
  void givenIssuersConfig_whenTokenForOtherAudience_thenRejected() {
    startServer(jwtOptions(Map.of("issuers", List.of(ISSUER_A, ISSUER_B))));

    var token =
        signer.generateToken(
            new JsonObject(),
            new JWTOptions().setIssuer(ISSUER_A).setAudience(List.of("other-api")));

    assertThat(queryStatus(token)).isEqualTo(401);
  }

  @Test
  void givenSingleIssuerConfig_whenTokensFromConfiguredAndOtherIssuer_thenOnlyConfiguredAccepted() {
    startServer(jwtOptions(Map.of("issuer", ISSUER_A)));

    assertThat(queryStatus(tokenFrom(ISSUER_A))).isEqualTo(200);
    assertThat(queryStatus(tokenFrom(ISSUER_B))).isEqualTo(401);
    assertThat(queryStatus(tokenFrom(null))).isEqualTo(401);
  }

  @Test
  void givenNoIssuerConfig_whenTokensWithAnyOrNoIssuer_thenAccepted() {
    startServer(jwtOptions(Map.of()));

    assertThat(queryStatus(tokenFrom(ISSUER_C))).isEqualTo(200);
    assertThat(queryStatus(tokenFrom(null))).isEqualTo(200);
  }

  private static Map<String, Object> hs256Key() {
    return Map.of("algorithm", "HS256", "buffer", SECRET);
  }

  private static Map<String, Object> jwtOptions(Map<String, Object> issuerOptions) {
    var jwtOptions = new HashMap<>(issuerOptions);
    jwtOptions.put("audience", List.of(AUDIENCE));
    return Map.of("pubSecKeys", List.of(hs256Key()), "jwtOptions", jwtOptions);
  }

  private String tokenFrom(String issuer) {
    var options = new JWTOptions().setAudience(List.of(AUDIENCE)).setExpiresInSeconds(60);
    if (issuer != null) {
      options.setIssuer(issuer);
    }
    return signer.generateToken(new JsonObject(), options);
  }

  private void startServer(Map<String, Object> jwtAuth) {
    var config = new ServerConfig();
    config.setJwtAuth(jwtAuth);
    var authProviders =
        config.getJwtAuthOptions().stream()
            .map(options -> (AuthenticationProvider) JWTAuth.create(vertx, options))
            .toList();
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
