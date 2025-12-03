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
package com.datasqrl.graphql;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.datasqrl.graphql.config.CorsHandlerOptions;
import com.datasqrl.graphql.config.KafkaConfig;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.config.ServletConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(VertxExtension.class)
@Testcontainers
class GraphQLJwtHandlerIT {
  private static final int SERVER_PORT = 8889; // Use different port to avoid conflicts

  @Container
  private final KafkaContainer kafkaContainer =
      new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.9.1"));

  @Container
  private final PostgreSQLContainer postgresContainer =
      new PostgreSQLContainer(
              DockerImageName.parse("ankane/pgvector:v0.5.0").asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("datasqrl")
          .withUsername("foo")
          .withPassword("secret");

  Vertx vertx;
  GraphQLServerVerticle graphQLServerVerticle;
  ServerConfig serverConfig;
  RootGraphqlModel model;

  @SneakyThrows
  @BeforeEach
  void setup(VertxTestContext testContext) {
    try (var admin =
        AdminClient.create(
            Map.of(BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic("mytopic", Optional.empty(), Optional.empty())));
    }
    vertx = Vertx.vertx();

    // Create the model with GraphQL schema and Kafka subscription
    model =
        RootGraphqlModel.builder()
            .schema(
                StringSchema.builder()
                    .schema(
                        "type Query { "
                            + "  mock: String "
                            + "}"
                            + "type Subscription { "
                            + "  mock: MySub "
                            + "}"
                            + "type MySub { val: String }")
                    .build())
            .subscription(
                KafkaSubscriptionCoords.builder()
                    .topic("mytopic")
                    .fieldName("mock")
                    .equalityConditions(Map.of())
                    .build())
            .build();

    // Create server config with JWT auth and Kafka settings
    serverConfig = new ServerConfig();
    serverConfig.setJwtAuth(
        new JWTAuthOptions()
            .addPubSecKey(new PubSecKeyOptions().setAlgorithm("HS256").setBuffer("dGVzdA==")));
    serverConfig.setPoolOptions(new PoolOptions());
    serverConfig.setServletConfig(new ServletConfig());
    serverConfig.setCorsHandlerOptions(new CorsHandlerOptions());
    serverConfig.setKafkaSubscriptionConfig(
        new KafkaConfig.KafkaSubscriptionConfig(
            Map.of(
                BOOTSTRAP_SERVERS_CONFIG,
                kafkaContainer.getBootstrapServers(),
                KEY_DESERIALIZER_CLASS_CONFIG,
                "com.datasqrl.graphql.kafka.JsonDeserializer",
                VALUE_DESERIALIZER_CLASS_CONFIG,
                "com.datasqrl.graphql.kafka.JsonDeserializer")));

    // Configure PostgreSQL connection
    PgConnectOptions pgOptions = new PgConnectOptions();
    pgOptions.setDatabase(postgresContainer.getDatabaseName());
    pgOptions.setHost(postgresContainer.getHost());
    pgOptions.setPort(postgresContainer.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
    pgOptions.setUser(postgresContainer.getUsername());
    pgOptions.setPassword(postgresContainer.getPassword());
    serverConfig.setPgConnectOptions(pgOptions);
    HttpServerOptions httpServerOptions =
        new HttpServerOptions().setPort(SERVER_PORT).setHost("0.0.0.0");
    serverConfig.setHttpServerOptions(httpServerOptions);

    // Create a minimal HTTP server with GraphQL verticle (following McpApiValidationIT pattern)
    var httpServer = vertx.createHttpServer(httpServerOptions);
    var router = io.vertx.ext.web.Router.router(vertx);

    // Add basic handlers
    router.route().handler(io.vertx.ext.web.handler.BodyHandler.create());
    router.route().handler(io.vertx.ext.web.handler.LoggerHandler.create());

    // Add CORS handler
    var corsHandler =
        io.vertx.ext.web.handler.CorsHandler.create()
            .addOrigin("*")
            .allowedMethod(io.vertx.core.http.HttpMethod.GET)
            .allowedMethod(io.vertx.core.http.HttpMethod.POST)
            .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
            .allowedHeader("Access-Control-Request-Method")
            .allowedHeader("Access-Control-Allow-Credentials")
            .allowedHeader("Access-Control-Allow-Origin")
            .allowedHeader("Access-Control-Allow-Headers")
            .allowedHeader("Content-Type")
            .allowedHeader("Authorization");
    router.route().handler(corsHandler);

    // Create the GraphQL server verticle directly (similar to McpBridgeVerticle)
    var authProviders =
        List.of(
            (io.vertx.ext.auth.authentication.AuthenticationProvider)
                io.vertx.ext.auth.jwt.JWTAuth.create(vertx, serverConfig.getJwtAuth()));
    graphQLServerVerticle =
        new GraphQLServerVerticle(
            router, serverConfig, "v1", model, authProviders, Optional.empty());

    vertx
        .deployVerticle(graphQLServerVerticle)
        .compose(
            deploymentId -> {
              httpServer.requestHandler(router);
              return httpServer.listen();
            })
        .onSuccess(
            server -> {
              System.out.println("GraphQL server started on port " + SERVER_PORT);
              testContext.completeNow();
            })
        .onFailure(testContext::failNow);
  }

  @SneakyThrows
  @AfterEach
  void teardown(VertxTestContext testContext) {
    if (vertx != null) {
      vertx
          .close()
          .onComplete(
              ar -> {
                testContext.completeNow();
              });
    } else {
      testContext.completeNow();
    }
  }

  @Test
  void jwtAuthentication(VertxTestContext testContext) {
    var provider = JWTAuth.create(vertx, this.serverConfig.getJwtAuth());

    // Generate token
    var token = provider.generateToken(new JsonObject(), new JWTOptions().setExpiresInSeconds(60));

    sendQuery(
        token,
        ar -> {
          if (ar.succeeded()) {
            if (ar.result().statusCode() != 200) {
              testContext.failNow("Status code not 200: " + ar.result().statusCode());
            } else {
              testContext.completeNow();
            }
          } else {
            testContext.failNow(ar.cause());
          }
        });
  }

  @Test
  void invalidJWTAuthentication(VertxTestContext testContext) {
    sendQuery(
        "Badtoken",
        ar -> {
          if (ar.succeeded()) {
            if (ar.result().statusCode() != 401) {
              testContext.failNow("Status code not 401: " + ar.result().statusCode());
            } else {
              testContext.completeNow();
            }
          } else {
            testContext.failNow(ar.cause());
          }
        });
  }

  @Test
  void websocketBadAuth(VertxTestContext testContext) {
    var options =
        new WebSocketConnectOptions()
            .setPort(SERVER_PORT)
            .setHost("localhost")
            .setURI("/v1/graphql")
            .putHeader("Authorization", "Bearer badToken");

    var client = vertx.createWebSocketClient();

    client
        .connect(options)
        .onSuccess(
            ws -> {
              // Should not succeed
              testContext.failNow("Should fail");
            })
        .onFailure(
            err -> {
              // Failure is expected
              testContext.completeNow();
            });
  }

  @Disabled
  @Test
  void websocket(VertxTestContext testContext) {
    //    var provider = JWTAuth.create(vertx, this.serverConfig.getAuthOptions());
    //    var token = provider.generateToken(new JsonObject(),
    //        new JWTOptions().setExpiresInSeconds(60));
    //
    //    var options = new WebSocketConnectOptions()
    //        .setPort(SERVER_PORT)
    //        .setHost("localhost")
    //        .setURI("/graphql") // Your actual WebSocket endpoint URI
    //        .addHeader("Authorization", "Bearer " + token); // Send the JWT as part of the initial
    // request headers
    //
    //    var initMessage = "{\"type\":\"connection_init\",\"payload\":{}}";  // connection
    // initialization message
    //
    //    var graphqlSubscription =
    // "{\"type\":\"subscribe\",\"id\":\"1\",\"payload\":{\"query\":\"subscription { mock { val }
    // }\"}}";
    //    // Connect using the WebSocket client
    //    vertx.createHttpClient().webSocket(options, wsResult -> {
    //      if (wsResult.succeeded()) {
    //        var ws = wsResult.result();
    //        // Send a GraphQL query as a text message
    //        ws.writeTextMessage(initMessage);
    //        ws.handler(message -> {
    //          if (message.toString().contains("next")) {
    //            if
    // (message.toString().equals("{\"id\":\"1\",\"type\":\"next\",\"payload\":{\"data\":{\"mock\":{\"val\":\"x\"}}}}")) {
    //              testContext.completeNow();
    //            } else {
    //              testContext.failNow("Unexpected message:" + message);
    //            }
    //          } else if (message.toString().contains("connection_ack")) {
    //            ws.writeTextMessage(graphqlSubscription);
    //
    //            var props = new Properties();
    //            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    //            props.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
    //            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    // StringSerializer.class.getName());
    //            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    // StringSerializer.class.getName());
    //
    //            try {
    //              Thread.sleep(1000);
    //            } catch (InterruptedException e) {
    //              throw new RuntimeException(e);
    //            }
    //            KafkaProducer producer =  KafkaProducer.create(Vertx.vertx(), props);
    //            var jsonMessage = new JsonObject().put("val", "x");
    //            producer.send(new KafkaProducerRecordImpl("mytopic",
    // jsonMessage.toString()),(Handler<AsyncResult<RecordMetadata>>)(metadata)->{
    //              System.out.println(metadata.result().getTopic());
    //              System.out.println(metadata.result().getTimestamp());
    //            });
    //          }
    //        });
    //      } else {
    //        testContext.failNow(wsResult.cause()); // Fail the test if the WebSocket connection
    // could not be established
    //      }
    //    });
  }

  private void sendQuery(String token, Handler<AsyncResult<HttpResponse<Buffer>>> callback) {
    var query = new JsonObject().put("query", "query { mock }");
    var webClient = WebClient.create(vertx);
    webClient
        .post(SERVER_PORT, "localhost", "/v1/graphql")
        .putHeader("Authorization", "Bearer " + token)
        .putHeader("Content-Type", "application/json")
        .sendJson(query)
        .onComplete(callback);
  }
}
