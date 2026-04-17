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
package com.datasqrl.graphql.kafka;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.vertx.core.Vertx;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.healthchecks.HealthCheckHandler;
import io.vertx.junit5.VertxExtension;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class KafkaHealthTrackerTest {

  private Vertx vertx;

  @BeforeEach
  void setUp() {
    vertx = Vertx.vertx();
  }

  @AfterEach
  void tearDown() {
    vertx.close();
  }

  @Test
  void givenNoTopicRegistered_whenProbeFires_thenTrackerStaysHealthy() {
    var config = Map.of(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
    try (var tracker = new KafkaHealthTracker(vertx, config, 200L, 500L)) {
      await()
          .during(Duration.ofSeconds(1))
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(
              () -> {
                assertThat(tracker.isHealthy()).isTrue();
                assertThat(tracker.lastError()).isNull();
              });
    }
  }

  @Test
  void givenUnreachableBroker_whenTopicRegistered_thenTrackerReportsUnhealthy() {
    var config = Map.of(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
    try (var tracker = new KafkaHealthTracker(vertx, config, 200L, 500L)) {
      tracker.registerTopic("probe-topic");
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> {
                assertThat(tracker.isHealthy()).isFalse();
                assertThat(tracker.lastError()).isNotBlank();
              });
    }
  }

  @Test
  void givenUnhealthyTracker_whenHealthEndpointHit_thenReturns503() throws Exception {
    var config = Map.of(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
    try (var tracker = new KafkaHealthTracker(vertx, config, 200L, 500L)) {
      tracker.registerTopic("probe-topic");
      await().atMost(Duration.ofSeconds(10)).until(() -> !tracker.isHealthy());

      var router = Router.router(vertx);
      var handler = HealthCheckHandler.create(vertx);
      handler.register(
          "kafka", promise -> promise.complete(tracker.isHealthy() ? Status.OK() : Status.KO()));
      router.get("/health*").handler(handler);

      var server =
          vertx
              .createHttpServer()
              .requestHandler(router)
              .listen(0)
              .toCompletionStage()
              .toCompletableFuture()
              .get(5, TimeUnit.SECONDS);

      var client = WebClient.create(vertx);
      var responseFuture = new CompletableFuture<Integer>();
      client
          .get(server.actualPort(), "127.0.0.1", "/health")
          .send()
          .onSuccess(resp -> responseFuture.complete(resp.statusCode()))
          .onFailure(responseFuture::completeExceptionally);

      var statusCode = responseFuture.get(5, TimeUnit.SECONDS);
      assertThat(statusCode).isEqualTo(503);

      client.close();
      server.close();
    }
  }
}
