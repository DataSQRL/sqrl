package com.datasqrl.graphql.kafka;

import com.datasqrl.graphql.server.SinkEmitter;
import com.datasqrl.graphql.server.SinkResult;
import com.datasqrl.graphql.server.SinkRecord;
import io.vertx.core.Promise;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class KafkaSinkEmitter extends SinkEmitter {
  private final KafkaProducer<String, String> kafkaProducer;
  private final String topic;

  @Override
  public CompletableFuture<SinkResult> send(SinkRecord data, Promise<Object> fut,
      Map<String, Object> entry) {
    KafkaSinkRecord record = (KafkaSinkRecord) data;

    KafkaProducerRecord<String, String> producerRecord =
        KafkaProducerRecord.create(topic, record.getObject());

    kafkaProducer.send(producerRecord)
        .onSuccess(f->{
          ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(f.getTimestamp()), ZoneId.systemDefault());
          entry.put("_source_time", dateTime.toLocalDateTime());
          fut.complete(entry);
        })
        .onFailure(f->fut.fail(f));

    return null;
  }
}
