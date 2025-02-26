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
