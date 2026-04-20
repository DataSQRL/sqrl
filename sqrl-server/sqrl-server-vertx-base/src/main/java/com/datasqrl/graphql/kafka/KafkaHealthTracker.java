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
 * Self-monitoring Kafka health probe for the {@code /health} endpoint. Periodically calls {@link
 * KafkaAdminClient#describeTopics} on a dedicated admin client with short timeouts and caches the
 * result, so the endpoint never blocks and the tracker self-heals once the broker recovers.
 *
 * <p>Topic and Kafka client config come from {@link Context} at construction. A probe fires
 * immediately then on a timer; the tracker reports healthy until that first probe returns, so
 * {@code /health} doesn't flap on startup. If admin-client construction itself fails, the tracker
 * starts unhealthy with the failure reason and no probes are scheduled.
 */
@Slf4j
public class KafkaHealthTracker implements AutoCloseable {

  public static final long DEFAULT_PROBE_INTERVAL_MS = 10_000L;
  public static final long DEFAULT_PROBE_TIMEOUT_MS = 5_000L;

  private final AtomicReference<HealthState> state =
      new AtomicReference<>(new HealthState(true, "not initialized yet"));

  private final Vertx vertx;
  private final KafkaAdminClient adminClient;
  private final String topic;
  private final Long timerId;

  public KafkaHealthTracker(Vertx vertx, Context ctx) {
    this(vertx, ctx, DEFAULT_PROBE_INTERVAL_MS, DEFAULT_PROBE_TIMEOUT_MS);
  }

  public KafkaHealthTracker(Vertx vertx, Context ctx, long probeIntervalMs, long probeTimeoutMs) {
    this.vertx = vertx;
    this.topic = ctx.topic();

    adminClient = initAdminClient(vertx, ctx, probeTimeoutMs);

    if (adminClient != null) {
      probe();
      timerId = vertx.setPeriodic(probeIntervalMs, id -> probe());
    } else {
      timerId = null;
    }
  }

  public boolean isHealthy() {
    return state.get().healthy();
  }

  public String lastError() {
    var s = state.get();
    return s.healthy() ? null : s.detail();
  }

  private KafkaAdminClient initAdminClient(Vertx vertx, Context ctx, long probeTimeoutMs) {
    try {
      var probeConfig = new HashMap<>(ctx.kafkaConfig());
      probeConfig.put(REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(probeTimeoutMs));
      probeConfig.put(DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(probeTimeoutMs));

      return KafkaAdminClient.create(vertx, probeConfig);

    } catch (Exception e) {
      log.warn("Failed to create Kafka admin client: {}", e.getMessage());
      state.set(new HealthState(false, e.getMessage()));
    }

    return null;
  }

  private void probe() {
    adminClient
        .describeTopics(List.of(topic))
        .onSuccess(desc -> state.set(new HealthState(true, "ok")))
        .onFailure(
            err -> {
              log.warn("Kafka health probe failed: {}", err.getMessage());
              state.set(new HealthState(false, err.getMessage()));
            });
  }

  @Override
  public void close() {
    vertx.cancelTimer(timerId);
    adminClient.close();
  }

  public record Context(String topic, Map<String, String> kafkaConfig) {}

  private record HealthState(boolean healthy, String detail) {}
}
