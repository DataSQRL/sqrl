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
package com.datasqrl;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

// @ExtendWith(MiniClusterExtension.class)
class FlinkKafkaIntegrationIT {

  private static KafkaContainer kafkaContainer;

  @SuppressWarnings("resource")
  @BeforeAll
  static void setup() {
    kafkaContainer =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.2"))
            .withEnv(
                "KAFKA_MESSAGE_MAX_BYTES",
                "50000000") // Increase broker's max message size to 50 MB
            .withEnv("KAFKA_MAX_REQUEST_SIZE", "50000000")
            .withEnv("KAFKA_MAX_PARTITION_FETCH_BYTES", "50000000");
    kafkaContainer.start();
  }

  @AfterAll
  static void teardown() {
    kafkaContainer.stop();
  }

  @Test
  void flinkKafkaLargeMessage() throws Exception {

    // Get Kafka bootstrap servers from the test container
    var kafkaBootstrapServers = kafkaContainer.getBootstrapServers();

    // Now, read from Kafka directly using KafkaConsumer
    var consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Increase the max fetch bytes to handle large messages
    consumerProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "50000000");
    consumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "50000000");

    AdminClient.create(consumerProps)
        .createTopics(List.of(new NewTopic("test-topic", 1, (short) 1)));

    // Set up Flink execution environments
    var env =
        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(Configuration.fromMap(Map.of()));
    var tableEnv = StreamTableEnvironment.create(env);

    // Define the DataGen source table with a large message field
    var dataGenSourceDDL =
        """
                CREATE TABLE source_table (
                  id INT,
                  large_message STRING
                ) WITH (
                  'connector' = 'datagen',
                  'number-of-rows' = '1',
                  'fields.id.kind' = 'sequence',
                  'fields.id.start' = '1',
                  'fields.id.end' = '1',
                  'fields.large_message.length' = '2048576'
                )""";

    // Define the Kafka sink table with appropriate properties
    var kafkaSinkDDL =
        """
                CREATE TABLE sink_table (
                  id INT,
                  large_message STRING
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'test-topic',
                  'properties.bootstrap.servers' = '%s',
                  'key.format' = 'json',
                  'key.fields' = 'id',
                  'value.format' = 'json',
                  'properties.max.request.size' = '50000000'
                )"""
            .formatted(kafkaBootstrapServers);

    // Execute the DDL statements to create the tables
    tableEnv.executeSql(dataGenSourceDDL);
    tableEnv.executeSql(kafkaSinkDDL);

    // Insert data from the DataGen source to the Kafka sink
    var result =
        tableEnv.executeSql("INSERT INTO sink_table SELECT id, large_message FROM source_table");

    // Wait for the job to finish
    result.await(10, TimeUnit.SECONDS);
    var resultKind = result.getResultKind();
    assertThat(resultKind).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT);

    result.print();

    var consumer = new KafkaConsumer<String, String>(consumerProps);
    consumer.subscribe(Collections.singletonList("test-topic"));

    // Poll for messages
    List<ConsumerRecord<String, String>> recordsList = new ArrayList<>();
    var messageReceived = false;
    var timeout = System.currentTimeMillis() + 10000; // 10 seconds timeout

    while (!messageReceived && System.currentTimeMillis() < timeout) {
      var records = consumer.poll(Duration.ofMillis(1000));
      for (ConsumerRecord<String, String> record : records) {
        recordsList.add(record);
        messageReceived = true;
      }
    }
    consumer.close();

    // Verify that the message was written
    assertThat(recordsList).hasSize(1);
    var record = recordsList.get(0);

    var key = record.key();
    var value = record.value();

    // Parse the key and value from JSON strings to objects if necessary
    // Assuming the key and value are JSON strings
    // For simplicity, let's just check the lengths
    assertThat(key).isNotNull();
    assertThat(value).isNotNull();

    // Optionally, parse the JSON and check the contents
    // For this example, we'll check the lengths
    // Since the 'large_message' field is 1 MB, the value should be at least 1 MB in size
    assertThat(value).hasSizeGreaterThanOrEqualTo(1048576);

    System.out.println("Message successfully written and read from Kafka.");
  }
}
