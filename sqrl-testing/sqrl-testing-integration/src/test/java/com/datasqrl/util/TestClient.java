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
// package com.datasqrl.util;
//
// import io.vertx.core.Vertx;
// import io.vertx.core.http.WebSocket;
// import io.vertx.core.impl.future.PromiseImpl;
// import io.vertx.core.json.JsonObject;
// import io.vertx.ext.web.client.HttpResponse;
// import io.vertx.ext.web.client.WebClient;
// import io.vertx.ext.web.codec.BodyCodec;
// import lombok.extern.slf4j.Slf4j;
//
// import java.util.function.Consumer;
//
// import static org.junit.jupiter.api.Assertions.fail;
//
// @Slf4j
// public class TestClient {
//
//  public static final Consumer<HttpResponse<JsonObject>> NO_HANDLER = (h)->{};
//  public static final Consumer<HttpResponse<JsonObject>> FAIL_HANDLER = (h)->{
//    if (h.statusCode() >= 300) {
//      fail(h.statusMessage());
//    }};
//  public static final Consumer<JsonObject> NO_WS_HANDLER = (h)->{};
//  private final Vertx vertx;
//
//  public TestClient(Vertx vertx) {
//    this.vertx = vertx;
//  }
//
//  public void query(String query, JsonObject input,
//                    Consumer<HttpResponse<JsonObject>> callback) {
//    WebClient client = WebClient.create(vertx);
//    JsonObject graphqlQuery = new JsonObject().put("query", query)
//        .put("variables", input);
//
//    client.post(8888, "localhost", "/graphql").putHeader("Content-Type", "application/json")
//        .as(BodyCodec.jsonObject()).sendJsonObject(graphqlQuery, ar -> {
//          if (ar.succeeded()) {
//            log.info("Received response with status code" + ar.result().statusCode());
//            log.info("Response Body: " + ar.result().body());
//            if (ar.result().statusCode() != 200 ||
// ar.result().body().toString().contains("errors")) {
//              fail(ar.result().body().toString());
//            }
//            callback.accept(ar.result());
//          } else {
//            log.info(ar.result().bodyAsString());
//            log.info("Something went wrong " + ar.cause().getMessage());
//            fail();
//          }
//        });
//  }
//
//  public PromiseImpl listen(String query, Consumer<JsonObject> successHandler) {
//    PromiseImpl p = new PromiseImpl();
//    vertx.createHttpClient().webSocket(8888, "localhost", "/graphql-ws", websocketRes -> {
//      if (websocketRes.succeeded()) {
//        WebSocket websocket = websocketRes.result();
//
//        JsonObject connectionInitPayload = new JsonObject();
//        JsonObject payload = new JsonObject();
//        payload.put("query", query);
//
//        // Connection initialization
//        connectionInitPayload.put("type", "connection_init");
//        connectionInitPayload.put("payload", new JsonObject());
//        websocket.writeTextMessage(connectionInitPayload.encode());
//
//        // Start GraphQL subscription
//        JsonObject startMessage = new JsonObject();
//        startMessage.put("id", "1");
//        startMessage.put("type", "start");
//        startMessage.put("payload", payload);
//        websocket.writeTextMessage(startMessage.encode());
//
//        // Handle incoming messages
//        websocket.handler(message -> {
//          JsonObject jsonMessage = new JsonObject(message.toString());
//          String messageType = jsonMessage.getString("type");
//          switch (messageType) {
//            case "connection_ack":
//              log.info("Connection acknowledged");
//              break;
//            case "data":
//              JsonObject result = jsonMessage.getJsonObject("payload").getJsonObject("data");
//              log.info("Data: " + result);
//              successHandler.accept(result);
//              break;
//            case "complete":
//              log.info("Server indicated subscription complete");
//              break;
//            default:
//              log.info("Received message: " + message);
//          }
//        });
//        p.complete();
//      } else {
//        log.info("Failed to connect " + websocketRes.cause());
//        fail(websocketRes.cause());
//        p.fail("Failed");
//      }
//    });
//    return p;
//  }
// }
