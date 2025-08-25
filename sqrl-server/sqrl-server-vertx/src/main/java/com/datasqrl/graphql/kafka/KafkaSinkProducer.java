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

import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.io.SinkResult;
import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import java.time.Instant;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class KafkaSinkProducer<OUT> implements SinkProducer {

  private final String topic;
  private final KafkaProducer<String, OUT> kafkaProducer;

  @Override
  public Future<SinkResult> send(Map entry) {
    final KafkaProducerRecord producerRecord;

    try {
      producerRecord = KafkaProducerRecord.create(topic, entry);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
    // TODO: generate UUID server side
    return kafkaProducer
        .send(producerRecord)
        .map(
            result ->
                new SinkResult(Instant.ofEpochMilli(((RecordMetadata) result).getTimestamp())));
  }
}
