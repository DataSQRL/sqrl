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

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import java.util.Map;
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
  void givenUnreachableBroker_whenProbeFires_thenTrackerReportsUnhealthy() throws Exception {
    var config = Map.of(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
    try (var tracker = new KafkaHealthTracker(vertx, config, 200L, 500L)) {
      var deadline = System.currentTimeMillis() + 10_000L;
      while (tracker.isHealthy() && System.currentTimeMillis() < deadline) {
        Thread.sleep(100);
      }
      assertThat(tracker.isHealthy()).isFalse();
      assertThat(tracker.lastError()).isNotBlank();
    }
  }
}
