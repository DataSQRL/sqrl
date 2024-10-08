package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

//@ExtendWith(MiniClusterExtension.class)
public class FlinkKafkaIntegrationTest {

    private static KafkaContainer kafkaContainer;

    @BeforeClass
    public static void setup() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
                .withEnv("KAFKA_MESSAGE_MAX_BYTES", "50000000") // Increase broker's max message size to 50 MB
                .withEnv("KAFKA_MAX_REQUEST_SIZE", "50000000")
                .withEnv("KAFKA_MAX_PARTITION_FETCH_BYTES", "50000000")
        ;
        kafkaContainer.start();
    }

    @AfterClass
    public static void teardown() {
        kafkaContainer.stop();
    }

    @Test
    public void testFlinkKafkaLargeMessage() throws Exception {

        // Get Kafka bootstrap servers from the test container
        String kafkaBootstrapServers = kafkaContainer.getBootstrapServers();

        // Now, read from Kafka directly using KafkaConsumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Increase the max fetch bytes to handle large messages
        consumerProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "50000000");
        consumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "50000000");

        AdminClient.create(consumerProps).createTopics(List.of(new NewTopic("test-topic", 1, (short)1)));

        // Set up Flink execution environments
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
            Configuration.fromMap(Map.of()));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Define the DataGen source table with a large message field
        String dataGenSourceDDL = "CREATE TABLE source_table (\n" +
                "  id INT,\n" +
                "  large_message STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'number-of-rows' = '1',\n" +
                "  'fields.id.kind' = 'sequence',\n" +
                "  'fields.id.start' = '1',\n" +
                "  'fields.id.end' = '1',\n" +
                "  'fields.large_message.length' = '2048576'\n" + // Generate a 2 MB string
                ")";

        // Define the Kafka sink table with appropriate properties
        String kafkaSinkDDL = String.format(
                "CREATE TABLE sink_table (\n" +
                "  id INT,\n" +
                "  large_message STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'test-topic',\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  'key.format' = 'json',\n" +
                "  'key.fields' = 'id',\n" +
                "  'value.format' = 'json',\n" +
                "  'properties.max.request.size' = '50000000'\n" + // Increase producer's max request size
                ")", kafkaBootstrapServers);

        // Execute the DDL statements to create the tables
        tableEnv.executeSql(dataGenSourceDDL);
        tableEnv.executeSql(kafkaSinkDDL);

        // Insert data from the DataGen source to the Kafka sink
        TableResult result = tableEnv.executeSql("INSERT INTO sink_table SELECT id, large_message FROM source_table");

        // Wait for the job to finish
        result.await(10, TimeUnit.SECONDS);
        ResultKind resultKind = result.getResultKind();
        assertEquals(ResultKind.SUCCESS_WITH_CONTENT, resultKind);

        result.print();


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("test-topic"));

        // Poll for messages
        List<ConsumerRecord<String, String>> recordsList = new ArrayList<>();
        boolean messageReceived = false;
        long timeout = System.currentTimeMillis() + 10000; // 10 seconds timeout

        while (!messageReceived && System.currentTimeMillis() < timeout) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                recordsList.add(record);
                messageReceived = true;
            }
        }
        consumer.close();

        // Verify that the message was written
        Assertions.assertEquals(1, recordsList.size());
        ConsumerRecord<String, String> record = recordsList.get(0);

        String key = record.key();
        String value = record.value();

        // Parse the key and value from JSON strings to objects if necessary
        // Assuming the key and value are JSON strings
        // For simplicity, let's just check the lengths
        assertTrue(key != null);
        assertTrue(value != null);

        // Optionally, parse the JSON and check the contents
        // For this example, we'll check the lengths
        // Since the 'large_message' field is 1 MB, the value should be at least 1 MB in size
        assertTrue( value.length() >= 1048576);

        System.out.println("Message successfully written and read from Kafka.");
    }
}
