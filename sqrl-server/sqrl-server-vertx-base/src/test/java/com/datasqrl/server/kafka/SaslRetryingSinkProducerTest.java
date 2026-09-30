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
package com.datasqrl.server.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.server.io.SinkProducer;
import com.datasqrl.server.io.SinkProducer.Record;
import com.datasqrl.server.io.SinkResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SaslRetryingSinkProducerTest {

  private static final SinkResult SUCCESS =
      new SinkResult(Instant.ofEpochMilli(1_700_000_000_000L));

  private Vertx vertx;
  private AtomicInteger attempts;

  @BeforeEach
  void setUp() {
    vertx = Vertx.vertx();
    attempts = new AtomicInteger();
  }

  @AfterEach
  void tearDown() {
    vertx.close().await();
  }

  @Test
  void givenSaslAuthFailure_whenSend_thenRetriesOnceAndSucceeds() throws Exception {
    var sink = retrying(new SaslAuthenticationException("sts unreachable"));

    assertThat(sink.send(record()).await(5, TimeUnit.SECONDS)).isEqualTo(SUCCESS);
    assertThat(attempts).hasValue(2);
  }

  @Test
  void givenSaslAuthFailureOnRetry_whenSend_thenFailsAfterTwoAttempts() {
    var sink =
        retrying(
            new SaslAuthenticationException("sts unreachable"),
            new SaslAuthenticationException("sts still unreachable"));

    assertThatThrownBy(() -> sink.send(record()).await(5, TimeUnit.SECONDS))
        .isInstanceOf(SaslAuthenticationException.class)
        .hasMessage("sts still unreachable");
    assertThat(attempts).hasValue(2);
  }

  @Test
  void givenNonSaslFailure_whenSend_thenFailsWithoutRetry() {
    var sink = retrying(new IllegalStateException("boom"));

    assertThatThrownBy(() -> sink.send(record()).await(5, TimeUnit.SECONDS))
        .isInstanceOf(IllegalStateException.class);
    assertThat(attempts).hasValue(1);
  }

  private SinkProducer retrying(Throwable... failures) {
    SinkProducer delegate =
        r -> {
          var attempt = attempts.getAndIncrement();
          return attempt < failures.length
              ? Future.failedFuture(failures[attempt])
              : Future.succeededFuture(SUCCESS);
        };
    return new SaslRetryingSinkProducer(vertx, delegate, Duration.ofMillis(1));
  }

  private static Record record() {
    return new Record(Map.of("id", 1), Map.of("id", 1, "a", "x"));
  }
}
