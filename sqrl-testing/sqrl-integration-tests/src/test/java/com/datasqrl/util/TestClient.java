package com.datasqrl.util;

import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocket;
import io.vertx.core.impl.future.PromiseImpl;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class TestClient {

  public static final Consumer<HttpResponse<JsonObject>> NO_HANDLER = (h)->{};
  public static final Consumer<HttpResponse<JsonObject>> FAIL_HANDLER = (h)->{
    if (h.statusCode() >= 300) {
      fail(h.statusMessage());
    }};
  public static final Consumer<JsonObject> NO_WS_HANDLER = (h)->{};
  private final Vertx vertx;

  public TestClient(Vertx vertx) {
    this.vertx = vertx;
  }

  public void query(String query, JsonObject input,
                    Consumer<HttpResponse<JsonObject>> callback) {
    WebClient client = WebClient.create(vertx);
    JsonObject graphqlQuery = new JsonObject().put("query", query)
        .put("variables", input);

    client.post(8888, "localhost", "/graphql").putHeader("Content-Type", "application/json")
        .as(BodyCodec.jsonObject()).sendJsonObject(graphqlQuery, ar -> {
          if (ar.succeeded()) {
            logResponse(ar.result());
            if (ar.result().statusCode() != 200 || ar.result().body().toString().contains("errors")) {
              fail(ar.result().body().toString());
            }
            callback.accept(ar.result());
          } else {
            logFailure(ar.cause(), ar.result());
            fail();
          }
        });
  }

  public void executePersistedQuery(String querySha, JsonObject input,
      Consumer<HttpResponse<JsonObject>> callback) {
    WebClient client = WebClient.create(vertx);

    // Create JSON object for the persisted query
    JsonObject graphqlQuery = new JsonObject()
        .put("extensions", new JsonObject()
            .put("persistedQuery", new JsonObject()
                .put("version", 1) // version of persisted query protocol
                .put("sha256Hash", querySha)))
        .put("variables", input);

    System.out.println(graphqlQuery.encode());

    // Send the request
    client.post(8888, "localhost", "/graphql")
        .putHeader("Content-Type", "application/json")
        .as(BodyCodec.jsonObject())
        .sendJsonObject(graphqlQuery, ar -> {
          if (ar.succeeded()) {
            // Log the response and invoke the callback
            logResponse(ar.result());
            if (ar.result().statusCode() != 200 || ar.result().body().toString().contains("errors")) {
              fail(ar.result().body().toString());
            }
            callback.accept(ar.result());
          } else {
            // Handle failure
            logFailure(ar.cause(), ar.result());
            fail(ar.cause());
          }
        });
  }

  private void logResponse(HttpResponse<JsonObject> response) {
    System.out.println("Received response with status code: " + response.statusCode());
    System.out.println("Response Body: " + response.body().toString());
    if (response.statusCode() != 200 || response.body().toString().contains("errors")) {
      System.err.println("GraphQL Error: " + response.body().toString());
    }
  }

  private void logFailure(Throwable cause, HttpResponse<JsonObject> response) {
    if (response != null) {
      // Attempt to log the raw response body for debugging
      try {
        String rawResponse = response.body().toString();
        System.err.println("Raw response body: " + rawResponse);
      } catch (Exception e) {
        System.err.println("Error parsing response body: " + e.getMessage());
      }
    }
    System.err.println("Request failed: " + cause.getMessage());
  }
  public PromiseImpl listen(String query, Consumer<JsonObject> successHandler) {
    PromiseImpl p = new PromiseImpl();
    vertx.createHttpClient().webSocket(8888, "localhost", "/graphql-ws", websocketRes -> {
      if (websocketRes.succeeded()) {
        WebSocket websocket = websocketRes.result();

        JsonObject connectionInitPayload = new JsonObject();
        JsonObject payload = new JsonObject();
        payload.put("query", query);

        // Connection initialization
        connectionInitPayload.put("type", "connection_init");
        connectionInitPayload.put("payload", new JsonObject());
        websocket.writeTextMessage(connectionInitPayload.encode());

        // Start GraphQL subscription
        JsonObject startMessage = new JsonObject();
        startMessage.put("id", "1");
        startMessage.put("type", "start");
        startMessage.put("payload", payload);
        websocket.writeTextMessage(startMessage.encode());

        // Handle incoming messages
        websocket.handler(message -> {
          JsonObject jsonMessage = new JsonObject(message.toString());
          String messageType = jsonMessage.getString("type");
          switch (messageType) {
            case "connection_ack":
              log.info("Connection acknowledged");
              break;
            case "data":
              JsonObject result = jsonMessage.getJsonObject("payload").getJsonObject("data");
              log.info("Data: " + result);
              successHandler.accept(result);
              break;
            case "complete":
              log.info("Server indicated subscription complete");
              break;
            default:
              log.info("Received message: " + message);
          }
        });
        p.complete();
      } else {
        log.info("Failed to connect " + websocketRes.cause());
        fail();
        p.fail("Failed");
      }
    });
    return p;
  }
}
