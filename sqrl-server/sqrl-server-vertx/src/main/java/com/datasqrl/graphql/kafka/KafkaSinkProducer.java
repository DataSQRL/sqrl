package com.datasqrl.graphql.kafka;

import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.io.SinkResult;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class KafkaSinkProducer<OUT> implements SinkProducer {

  private final String topic;
  private final KafkaProducer<String, OUT> kafkaProducer;

  @Override
  public Future<SinkResult> send(Map entry) {
    final KafkaProducerRecord producerRecord;
//    entry.put("_source_time", ZonedDateTime.now(ZoneId.systemDefault()));
    UUID uuid = UUID.randomUUID();
    entry.put("_uuid", uuid);

    try {
      producerRecord = KafkaProducerRecord.create(topic, entry);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
    //TODO: generate UUID server side
    return kafkaProducer.send(producerRecord).map(result ->
        new SinkResult(Instant.ofEpochMilli(((RecordMetadata)result).getTimestamp()), Optional.of(uuid)));
  }
}
