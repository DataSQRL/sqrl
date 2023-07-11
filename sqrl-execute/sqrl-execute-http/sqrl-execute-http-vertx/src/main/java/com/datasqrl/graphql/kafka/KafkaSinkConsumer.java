package com.datasqrl.graphql.kafka;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.kafka.KafkaSinkProducer.KafkaConfig;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.formats.FormatFactory.Parser;
import com.datasqrl.io.formats.TextLineFormat;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;

@Slf4j
@AllArgsConstructor
public class KafkaSinkConsumer<IN> implements SinkConsumer {

  final KafkaConsumer<String, IN> consumer;
  final FormatFactory.Parser<IN> deserializer;

  @Override
  public void listen(Consumer<Map<String, Object>> listener, Consumer<Throwable> errorHandler, Consumer<Void> endOfStream) {
    consumer.handler(kafkaRecord -> {
      Parser.Result result = deserializer.parse(kafkaRecord.value());
      if (result.isSuccess()) {
        listener.accept(result.getRecord());
      } else if (result.isError()) {
        errorHandler.accept(new IllegalArgumentException(result.getErrorMsg()));
        log.error("error! " + result.getErrorMsg());
      }
      //TODO: error handling
    }).exceptionHandler(errorHandler::accept)
        .endHandler(endOfStream::accept);
  }

  public static KafkaSinkConsumer<?> createFromConfig(Vertx vertx, SqrlConfig config) {
    KafkaConfig kafkaConfig = KafkaSinkProducer.getKafkaConfig(config);
    Map<String,String> settings = new HashMap<>(kafkaConfig.settings);
    String uid = UUID.randomUUID().toString();
    settings.put(ConsumerConfig.GROUP_ID_CONFIG, uid);
    settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    if (kafkaConfig.formatFactory instanceof TextLineFormat) {
      settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
      KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx,  settings);
      consumer.subscribe(kafkaConfig.topic)
          .onSuccess(v ->
              log.info("Subscribed to topic: {}", kafkaConfig.topic)
          ).onFailure(cause ->
              log.error("Could not subscribe to topic {}. Error {}", kafkaConfig.topic,
                  cause.getMessage())
          );
      TextLineFormat.Parser deserializer = ((TextLineFormat) kafkaConfig.formatFactory)
          .getParser(kafkaConfig.formatConfig);
      return new KafkaSinkConsumer<>(consumer, deserializer);
    } else {
      throw new NotYetImplementedException("only supporting text-line formats");
    }
  }

}
