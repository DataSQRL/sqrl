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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class KafkaHealthTrackerTest {

  private static class MutableClock extends Clock {
    private Instant now;

    MutableClock(Instant now) {
      this.now = now;
    }

    void advanceMillis(long ms) {
      now = now.plusMillis(ms);
    }

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }

  @Test
  void givenNoActivity_whenIsHealthy_thenReturnsTrue() {
    var tracker = new KafkaHealthTracker(3, 10_000, new MutableClock(Instant.EPOCH));

    assertThat(tracker.isHealthy()).isTrue();
  }

  @Test
  void givenFailuresBelowThreshold_whenIsHealthy_thenReturnsTrue() {
    var tracker = new KafkaHealthTracker(3, 10_000, new MutableClock(Instant.EPOCH));

    tracker.recordFailure();
    tracker.recordFailure();

    assertThat(tracker.isHealthy()).isTrue();
  }

  @Test
  void givenFailuresAtThresholdWithinWindow_whenIsHealthy_thenReturnsTrue() {
    var clock = new MutableClock(Instant.EPOCH);
    var tracker = new KafkaHealthTracker(3, 10_000, clock);
    tracker.recordSuccess();

    clock.advanceMillis(1_000);
    tracker.recordFailure();
    tracker.recordFailure();
    tracker.recordFailure();

    assertThat(tracker.isHealthy()).isTrue();
  }

  @Test
  void givenFailuresAtThresholdBeyondWindow_whenIsHealthy_thenReturnsFalse() {
    var clock = new MutableClock(Instant.EPOCH);
    var tracker = new KafkaHealthTracker(3, 10_000, clock);
    tracker.recordSuccess();

    clock.advanceMillis(15_000);
    tracker.recordFailure();
    tracker.recordFailure();
    tracker.recordFailure();

    assertThat(tracker.isHealthy()).isFalse();
    assertThat(tracker.getConsecutiveFailures()).isEqualTo(3);
  }

  @Test
  void givenSuccessAfterFailures_whenIsHealthy_thenReturnsTrue() {
    var clock = new MutableClock(Instant.EPOCH);
    var tracker = new KafkaHealthTracker(3, 10_000, clock);

    clock.advanceMillis(15_000);
    tracker.recordFailure();
    tracker.recordFailure();
    tracker.recordFailure();
    tracker.recordSuccess();

    assertThat(tracker.isHealthy()).isTrue();
    assertThat(tracker.getConsecutiveFailures()).isZero();
  }
}
