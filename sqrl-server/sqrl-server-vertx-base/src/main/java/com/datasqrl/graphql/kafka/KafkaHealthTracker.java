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

import static org.apache.kafka.clients.admin.AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG;

import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Self-monitoring Kafka health probe used by the {@code /health} endpoint. Periodically calls
 * {@link KafkaAdminClient#listTopics()} on a dedicated admin client with short timeouts; the latest
 * probe outcome is cached so the health endpoint never blocks. Self-heals: once the broker becomes
 * reachable again, the next probe flips the cached state back to healthy without depending on user
 * traffic.
 */
@Slf4j
public class KafkaHealthTracker implements AutoCloseable {

  public static final long DEFAULT_PROBE_INTERVAL_MS = 10_000L;
  public static final long DEFAULT_PROBE_TIMEOUT_MS = 5_000L;

  private final KafkaAdminClient adminClient;
  private final long timerId;
  private final Vertx vertx;
  private final AtomicReference<HealthState> state =
      new AtomicReference<>(new HealthState(true, "initial"));

  public KafkaHealthTracker(Vertx vertx, Map<String, String> baseKafkaConfig) {
    this(vertx, baseKafkaConfig, DEFAULT_PROBE_INTERVAL_MS, DEFAULT_PROBE_TIMEOUT_MS);
  }

  public KafkaHealthTracker(
      Vertx vertx, Map<String, String> baseKafkaConfig, long probeIntervalMs, long probeTimeoutMs) {
    this.vertx = vertx;
    var probeConfig = new HashMap<>(baseKafkaConfig);
    probeConfig.put(REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(probeTimeoutMs));
    probeConfig.put(DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(probeTimeoutMs));
    this.adminClient = KafkaAdminClient.create(vertx, probeConfig);
    probe();
    this.timerId = vertx.setPeriodic(probeIntervalMs, id -> probe());
  }

  private void probe() {
    adminClient
        .listTopics()
        .onSuccess(topics -> state.set(new HealthState(true, "ok")))
        .onFailure(
            err -> {
              log.warn("Kafka health probe failed: {}", err.getMessage());
              state.set(new HealthState(false, err.getMessage()));
            });
  }

  public boolean isHealthy() {
    return state.get().healthy();
  }

  public String lastError() {
    var s = state.get();
    return s.healthy() ? null : s.detail();
  }

  @Override
  public void close() {
    vertx.cancelTimer(timerId);
    adminClient.close();
  }

  private record HealthState(boolean healthy, String detail) {}
}
