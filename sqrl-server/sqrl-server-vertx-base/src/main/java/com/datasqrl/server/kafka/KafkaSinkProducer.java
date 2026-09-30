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

import com.datasqrl.server.io.SinkProducer;
import com.datasqrl.server.io.SinkResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.errors.SaslAuthenticationException;

@Slf4j
@AllArgsConstructor
public class KafkaSinkProducer<OUT> implements SinkProducer {

  static final long AUTH_RETRY_DELAY_MS = 1000;

  private final Vertx vertx;
  private final String topic;
  private final KafkaProducer<String, OUT> kafkaProducer;
  private final boolean retryOnAuthFailure;

  @Override
  public Future<SinkResult> send(Record record) {
    final KafkaProducerRecord producerRecord;

    try {
      producerRecord = KafkaProducerRecord.create(topic, record.key(), record.value());
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
    // TODO: generate UUID server side
    Future<RecordMetadata> sent = kafkaProducer.send(producerRecord);
    if (retryOnAuthFailure) {
      sent =
          sent.recover(
              e -> isAuthFailure(e) ? retryAfterDelay(producerRecord, e) : Future.failedFuture(e));
    }
    return sent.map(result -> new SinkResult(Instant.ofEpochMilli(result.getTimestamp())));
  }

  private static boolean isAuthFailure(Throwable e) {
    return ExceptionUtils.indexOfType(e, SaslAuthenticationException.class) >= 0;
  }

  private Future<RecordMetadata> retryAfterDelay(KafkaProducerRecord producerRecord, Throwable e) {
    log.warn("Kafka authentication failed on topic {}, retrying send once", topic, e);
    return vertx
        .timer(AUTH_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
        .compose(v -> kafkaProducer.send(producerRecord));
  }
}
