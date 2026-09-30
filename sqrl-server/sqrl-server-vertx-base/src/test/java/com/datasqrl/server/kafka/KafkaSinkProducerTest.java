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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.server.io.SinkProducer.Record;
import com.datasqrl.server.io.SinkResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.RecordMetadata;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaSinkProducerTest {

  private static final long TIMESTAMP = 1_700_000_000_000L;

  private Vertx vertx;
  private KafkaProducer<String, String> producer;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    vertx = Vertx.vertx();
    producer = mock(KafkaProducer.class);
  }

  @AfterEach
  void tearDown() {
    vertx.close().await();
  }

  @Test
  void givenSaslAuthFailure_whenSend_thenRetriesOnceAndSucceeds() throws Exception {
    when(producer.send(any()))
        .thenReturn(Future.failedFuture(new SaslAuthenticationException("sts unreachable")))
        .thenReturn(Future.succeededFuture(metadata()));

    var result = send(true);

    assertThat(result.sourceTime()).isEqualTo(Instant.ofEpochMilli(TIMESTAMP));
    verify(producer, times(2)).send(any());
  }

  @Test
  void givenNonAuthFailure_whenSend_thenFailsWithoutRetry() {
    when(producer.send(any())).thenReturn(Future.failedFuture(new IllegalStateException("boom")));

    var future = new KafkaSinkProducer<>(vertx, "topic", producer, true).send(record());

    assertThat(future.failed()).isTrue();
    assertThat(future.cause()).isInstanceOf(IllegalStateException.class);
    verify(producer, times(1)).send(any());
  }

  @Test
  void givenRetryDisabled_whenSaslAuthFailure_thenFailsWithoutRetry() {
    when(producer.send(any()))
        .thenReturn(Future.failedFuture(new SaslAuthenticationException("sts unreachable")));

    var future = new KafkaSinkProducer<>(vertx, "topic", producer, false).send(record());

    assertThat(future.cause()).isInstanceOf(SaslAuthenticationException.class);
    verify(producer, times(1)).send(any());
  }

  private SinkResult send(boolean retry) throws Exception {
    return new KafkaSinkProducer<>(vertx, "topic", producer, retry)
        .send(record())
        .toCompletionStage()
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  private static Record record() {
    return new Record(Map.of("id", 1), Map.of("id", 1, "a", "x"));
  }

  private static RecordMetadata metadata() {
    return new RecordMetadata(0, 0, TIMESTAMP, "topic");
  }
}
