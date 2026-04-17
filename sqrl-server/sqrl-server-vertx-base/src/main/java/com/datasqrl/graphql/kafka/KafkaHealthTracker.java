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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Self-monitoring Kafka health probe used by the {@code /health} endpoint. Periodically calls
 * {@link KafkaAdminClient#describeTopics} on a dedicated admin client with short timeouts; the
 * latest probe outcome is cached so the health endpoint never blocks. Self-heals: once the broker
 * becomes reachable again, the next probe flips the cached state back to healthy without depending
 * on user traffic.
 *
 * <p>The probe target is the first topic handed to {@link #registerTopic(String)} (typically by
 * {@code MutationConfigurationImpl} when it wires up a sink). Until a topic is registered the
 * tracker reports healthy, so {@code /health} does not flap during startup.
 */
@Slf4j
public class KafkaHealthTracker implements AutoCloseable {

  public static final long DEFAULT_PROBE_INTERVAL_MS = 10_000L;
  public static final long DEFAULT_PROBE_TIMEOUT_MS = 5_000L;

  private final AtomicReference<String> probeTopic = new AtomicReference<>();
  private final AtomicReference<HealthState> state =
      new AtomicReference<>(new HealthState(true, "no topics registered yet"));

  private final Vertx vertx;
  private final long probeIntervalMs;
  private final KafkaAdminClient adminClient;

  private Long timerId;

  public KafkaHealthTracker(Vertx vertx, Map<String, String> baseKafkaConfig) {
    this(vertx, baseKafkaConfig, DEFAULT_PROBE_INTERVAL_MS, DEFAULT_PROBE_TIMEOUT_MS);
  }

  public KafkaHealthTracker(
      Vertx vertx, Map<String, String> baseKafkaConfig, long probeIntervalMs, long probeTimeoutMs) {
    this.vertx = vertx;
    this.probeIntervalMs = probeIntervalMs;

    var probeConfig = new HashMap<>(baseKafkaConfig);
    probeConfig.put(REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(probeTimeoutMs));
    probeConfig.put(DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(probeTimeoutMs));
    adminClient = KafkaAdminClient.create(vertx, probeConfig);
  }

  /**
   * Registers a topic to probe. First call wins; subsequent calls are ignored. Triggers an
   * immediate probe so the cached health state reflects the new target without waiting for the next
   * periodic tick.
   */
  public void registerTopic(String topic) {
    if (probeTopic.compareAndSet(null, topic)) {
      vertx.runOnContext(v -> probe());
      timerId = vertx.setPeriodic(probeIntervalMs, id -> probe());
    }
  }

  private void probe() {
    var topic = probeTopic.get();
    if (topic == null) {
      return;
    }

    adminClient
        .describeTopics(List.of(topic))
        .onSuccess(desc -> state.set(new HealthState(true, "ok")))
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
    if (timerId != null) {
      vertx.cancelTimer(timerId);
    }
    adminClient.close();
  }

  private record HealthState(boolean healthy, String detail) {}
}
