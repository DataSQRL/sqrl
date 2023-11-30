package com.datasqrl.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.util.data.Sensors;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class AvroKafkaIntegrationTest extends AbstractGraphqlTest {

  CompletableFuture<ExecutionResult> fut;

  @BeforeEach
  void setUp() {
    fut = execute(Sensors.INSTANCE_MUTATION);
    events = new ArrayList<>();
  }

  @SneakyThrows
  @Test
  @Disabled
  public void singleSubscriptionMutationTest() {
    Thread.sleep(5000);

    CountDownLatch countDownLatch = null;//subscribeToAlert(alert);

    Thread.sleep(1000);

//    executeMutation(addReading, nontriggerAlertJson);//test subscription filtering
//    executeMutation(addReading, triggerAlertJson);

    countDownLatch.await(120, TimeUnit.SECONDS);
    fut.cancel(true);
    assertEquals(countDownLatch.getCount(), 0);

    validateEvents();
  }

  private void writeOrders() {
//    ObjectMapper MAPPER = new ObjectMapper();
//    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
//    Properties props = new Properties();
//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
//    KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
//
//    try (BufferedReader br = new BufferedReader(new FileReader("<YourJsonFile>"))) {
//      String line;
//      while ((line = br.readLine()) != null) {
//        JsonNode jsonNode = MAPPER.readTree(line);
//        GenericRecord avroRecord = new GenericData.Record(schema);
//        // Populate the Avro record from JsonNode
//        // Replace with actual field population
//        avroRecord.put("id", jsonNode.get("id").asLong());
//        // ...
//
//        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(KAFKA_TOPIC, avroRecord);
//        producer.send(record);
//      }
//    }
//
//    producer.close();
  }

}
