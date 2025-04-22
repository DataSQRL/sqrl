package com.datasqrl.graphql.kafka;

import java.util.function.Consumer;

import com.datasqrl.graphql.io.SinkConsumer;

import io.vertx.kafka.client.consumer.KafkaConsumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class KafkaSinkConsumer<IN> implements SinkConsumer {

  final KafkaConsumer<String, IN> consumer;

  @Override
  public void listen(Consumer<Object> listener, Consumer<Throwable> errorHandler, Consumer<Void> endOfStream) {
    consumer.handler(kafkaRecord -> {
        try {
          Object result = kafkaRecord.value();
          listener.accept(result);
        } catch (Exception e) {
          errorHandler.accept(e);
        }
    }).exceptionHandler(errorHandler::accept)
        .endHandler(endOfStream::accept);
  }
}
