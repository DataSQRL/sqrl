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

import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks recent Kafka send failures so the /health endpoint can report unhealthy when the broker
 * becomes unreachable. Marks unhealthy only when both the failure threshold is reached and there
 * has been no successful send within the configured window — this absorbs broker rolling restarts
 * while still tripping on sustained outages (e.g. DNS resolution failures).
 */
public class KafkaHealthTracker {

  public static final int DEFAULT_FAILURE_THRESHOLD = 10;
  public static final long DEFAULT_WINDOW_MS = 30_000L;

  private final int failureThreshold;
  private final long windowMs;
  private final Clock clock;
  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
  private final AtomicLong lastSuccessTime;

  public KafkaHealthTracker() {
    this(DEFAULT_FAILURE_THRESHOLD, DEFAULT_WINDOW_MS, Clock.systemUTC());
  }

  public KafkaHealthTracker(int failureThreshold, long windowMs, Clock clock) {
    this.failureThreshold = failureThreshold;
    this.windowMs = windowMs;
    this.clock = clock;
    this.lastSuccessTime = new AtomicLong(clock.millis());
  }

  public void recordFailure() {
    consecutiveFailures.incrementAndGet();
  }

  public void recordSuccess() {
    consecutiveFailures.set(0);
    lastSuccessTime.set(clock.millis());
  }

  public boolean isHealthy() {
    if (consecutiveFailures.get() < failureThreshold) {
      return true;
    }
    return clock.millis() - lastSuccessTime.get() < windowMs;
  }

  public int getConsecutiveFailures() {
    return consecutiveFailures.get();
  }
}
