package com.datasqrl.graphql.kafka;

import com.datasqrl.graphql.server.SinkEmitter;
import com.datasqrl.graphql.server.SinkResult;
import com.datasqrl.graphql.server.SinkRecord;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@AllArgsConstructor
public class KafkaSinkEmitter extends SinkEmitter {
  private final KafkaProducer<String, String> kafkaProducer;

  @Override
  public CompletableFuture<SinkResult> send(SinkRecord data) {
    KafkaSinkRecord record = (KafkaSinkRecord) data;

    Future<RecordMetadata> metadataFuture =
        kafkaProducer.send(new ProducerRecord<>(record.getTopic(), record.getObject()));

    return CompletableFuture.supplyAsync(()->{
      try {
        System.out.println("event emitted..." + record.getTopic());
        RecordMetadata recordMetadata = metadataFuture.get();
        return new KafkaSinkResult(recordMetadata);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
