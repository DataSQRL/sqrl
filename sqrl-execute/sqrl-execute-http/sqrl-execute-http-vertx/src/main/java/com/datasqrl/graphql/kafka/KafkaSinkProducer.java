package com.datasqrl.graphql.kafka;

import static com.datasqrl.io.DataSystemImplementationFactory.SYSTEM_NAME_KEY;
import static com.datasqrl.io.tables.TableConfig.CONNECTOR_KEY;
import static com.datasqrl.io.tables.TableConfig.FORMAT_KEY;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.io.SinkResult;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.tables.BaseTableConfig;
import com.google.common.base.Preconditions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.checkerframework.checker.units.qual.K;

@AllArgsConstructor
public class KafkaSinkProducer<OUT> implements SinkProducer {

  private final String topic;
  private final KafkaProducer<String, OUT> kafkaProducer;
  private final FormatFactory.Writer<OUT> serializer;

  @Override
  public Future<SinkResult> send(Map<String, Object> entry) {
    final KafkaProducerRecord<String, OUT> producerRecord;
    try {
      //TODO: set partition key
      producerRecord = KafkaProducerRecord.create(topic, serializer.write(entry));
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
    //TODO: generate UUID server side
    return kafkaProducer.send(producerRecord).map(result ->
        new SinkResult(Instant.ofEpochMilli(result.getTimestamp()), Optional.empty()));
  }

  public static KafkaSinkProducer<?> createFromConfig(Vertx vertx, SqrlConfig config) {
    KafkaConfig kafkaConfig = getKafkaConfig(config);
    Map<String,String> settings = new HashMap<>(kafkaConfig.settings);
    settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    if (kafkaConfig.formatFactory instanceof TextLineFormat) {
      settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      KafkaProducer<String, String> producer =
          KafkaProducer.create(vertx, settings);
      TextLineFormat.Writer serializer = ((TextLineFormat) kafkaConfig.formatFactory)
          .getWriter(kafkaConfig.formatConfig);
      return new KafkaSinkProducer<>(kafkaConfig.topic, producer, serializer);
    } else {
      throw new NotYetImplementedException("only supporting text-line formats");
    }
  }

  @Value
  static class KafkaConfig {
    public Map<String, String> settings;
    public String topic;
    public FormatFactory formatFactory;
    public SqrlConfig formatConfig;
  }

  static KafkaConfig getKafkaConfig(SqrlConfig config) {
    SqrlConfig connectorConfig = config.getSubConfig(CONNECTOR_KEY);
    Preconditions.checkArgument(connectorConfig.asString(SYSTEM_NAME_KEY).get().equalsIgnoreCase("kafka"),"expected kafka");
    SqrlConfig formatConfig = config.getSubConfig(FORMAT_KEY);
    FormatFactory formatFactory = FormatFactory.fromConfig(formatConfig);
    String topic = config.asString(BaseTableConfig.IDENTIFIER_KEY).get();

    return new KafkaConfig(connectorConfig.toStringMap(), topic, formatFactory, formatConfig);
  }

}
