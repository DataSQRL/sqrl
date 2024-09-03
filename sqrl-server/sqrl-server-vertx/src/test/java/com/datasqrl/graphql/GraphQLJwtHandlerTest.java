package com.datasqrl.graphql;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.config.CorsHandlerOptions;
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
import io.vertx.core.http.WebSocket;
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
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import io.vertx.pgclient.impl.PgPoolOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class GraphQLJwtHandlerTest {
  EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

  Vertx vertx;
  GraphQLServer server;
  ServerConfig serverConfig;

  @SneakyThrows
  @BeforeEach
  public void setup(VertxTestContext testContext) {
    CLUSTER.start();
    try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic("mytopic", Optional.empty(), Optional.empty())));
    }
    vertx = Vertx.vertx();
    RootGraphqlModel root = RootGraphqlModel.builder()
        .schema(StringSchema.builder()
            .schema("type Query { "
                + "  mock: String "
                + "}"
                + "type Subscription { "
                + "  mock: MySub "
                + "}"
                + "type MySub { val: String }")
            .build())
        .subscription(KafkaSubscriptionCoords.builder()
            .topic("mytopic")
            .fieldName("mock")
            .filters(Map.of())
            .build())
        .build();

    serverConfig = new ServerConfig();
    serverConfig.setAuthOptions(new JWTAuthOptions()
        .addPubSecKey(new PubSecKeyOptions()
            .setAlgorithm("HS256")
            .setBuffer("dGVzdA==")));
    serverConfig.setPoolOptions(new PgPoolOptions());
    serverConfig.setServletConfig(new ServletConfig());
    serverConfig.setCorsHandlerOptions(new CorsHandlerOptions());
    HttpServerOptions httpServerOptions = new HttpServerOptions().setPort(8888).setHost("localhost");
    serverConfig.setHttpServerOptions(httpServerOptions);
    server = new GraphQLServer(root, serverConfig, NameCanonicalizer.SYSTEM, Optional.empty()) {
      @Override
      public String getEnvironmentVariable(String envVar) {
        return CLUSTER.bootstrapServers();
      }

      Map<String, String> getSourceConfig() {
        Map<String, String> conf = new HashMap<>();
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        conf.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
        conf.put(KEY_DESERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonDeserializer");
        conf.put(VALUE_DESERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonDeserializer");
        conf.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        return conf;
      }
    };
    vertx.deployVerticle(server)
        .onSuccess((c)->testContext.completeNow())
        .onFailure((c)->fail("Could not start")).toCompletionStage().toCompletableFuture().get();
  }

  @SneakyThrows
  @AfterEach
  public void teardown(VertxTestContext testContext) {
    vertx.close()
        .onSuccess((c)->testContext.completeNow());
  }

  @Test
  public void testJWTAuthentication(VertxTestContext testContext) {
    JWTAuth provider = JWTAuth.create(vertx, this.serverConfig.getAuthOptions());

    // Generate token
    String token = provider.generateToken(new JsonObject(), new JWTOptions().setExpiresInSeconds(60));

    sendQuery(token, ar -> {
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
  public void testInvalidJWTAuthentication(VertxTestContext testContext) {
    sendQuery("Badtoken", ar -> {
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
  public void testWebsocketBadAuth(VertxTestContext testContext) {
    WebSocketConnectOptions options = new WebSocketConnectOptions()
        .setPort(serverConfig.getHttpServerOptions().getPort())
        .setHost("localhost")
        .setURI("/graphql")
        .addHeader("Authorization", "Bearer badToken");

    vertx.createHttpClient().webSocket(options, wsResult -> {
      if (wsResult.succeeded()) {
        testContext.failNow("Should fail");
      } else {
        testContext.completeNow();
      }
    });
  }

  @Test
  public void testWebsocket(VertxTestContext testContext) {
    JWTAuth provider = JWTAuth.create(vertx, this.serverConfig.getAuthOptions());
    String token = provider.generateToken(new JsonObject(),
        new JWTOptions().setExpiresInSeconds(60));

    WebSocketConnectOptions options = new WebSocketConnectOptions()
        .setPort(serverConfig.getHttpServerOptions().getPort())
        .setHost("localhost")
        .setURI("/graphql") // Your actual WebSocket endpoint URI
        .addHeader("Authorization", "Bearer " + token); // Send the JWT as part of the initial request headers

    String initMessage = "{\"type\":\"connection_init\",\"payload\":{}}";  // connection initialization message

    String graphqlSubscription = "{\"type\":\"subscribe\",\"id\":\"1\",\"payload\":{\"query\":\"subscription { mock { val } }\"}}";
    // Connect using the WebSocket client
    vertx.createHttpClient().webSocket(options, wsResult -> {
      if (wsResult.succeeded()) {
        WebSocket ws = wsResult.result();
        // Send a GraphQL query as a text message
        ws.writeTextMessage(initMessage);
        ws.handler(message -> {
          if (message.toString().contains("next")) {
            if (message.toString().equals("{\"id\":\"1\",\"type\":\"next\",\"payload\":{\"data\":{\"mock\":{\"val\":\"x\"}}}}")) {
              testContext.completeNow();
            } else {
              testContext.failNow("Unexpected message:" + message);
            }
          } else if (message.toString().contains("connection_ack")) {
            ws.writeTextMessage(graphqlSubscription);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
            props.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            KafkaProducer producer =  KafkaProducer.create(Vertx.vertx(), props);
            JsonObject jsonMessage = new JsonObject().put("val", "x");
            producer.send(new KafkaProducerRecordImpl("mytopic", jsonMessage.toString()),(Handler<AsyncResult<RecordMetadata>>)(metadata)->{
//              System.out.println(metadata.result().getTopic());
//              System.out.println(metadata.result().getTimestamp());
            });

          }
        });
      } else {
        testContext.failNow(wsResult.cause()); // Fail the test if the WebSocket connection could not be established
      }
    });
  }

  private void sendQuery(String token, Handler<AsyncResult<HttpResponse<Buffer>>> callback) {
    JsonObject query = new JsonObject()
        .put("query", "query { mock }");
    WebClient webClient = WebClient.create(vertx);
    webClient.post(serverConfig.getHttpServerOptions().getPort(), "localhost", "/graphql")
        .putHeader("Authorization", "Bearer " + token)
        .putHeader("Content-Type", "application/json")
        .sendJsonObject(query, callback);
  }
}