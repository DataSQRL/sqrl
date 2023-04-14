/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;


import com.datasqrl.AbstractEngineIT;
import com.datasqrl.util.FileStreamUtil;
import com.datasqrl.io.formats.JsonLineFormat.Configuration;
import com.datasqrl.io.impl.kafka.KafkaDataSystemConfig;
import com.google.common.base.Strings;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

@Slf4j
public class KafkaBaseTest extends AbstractEngineIT {

  public static final int NUM_BROKERS = 1;

  public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

  @BeforeAll
  public static void startCluster() throws IOException {
    CLUSTER.start();
  }

  @AfterAll
  public static void closeCluster() {
    CLUSTER.stop();
  }

  String[] bootstrapServers;

  public void createTopics(String[] topics) throws Exception {
    bootstrapServers = CLUSTER.bootstrapServers().split(";");
    CLUSTER.createTopics(topics);
  }

  @AfterEach
  public void after() throws Exception {
    CLUSTER.deleteAllTopicsAndWait(0L);
  }

  public Properties getAdminProps() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    return props;
  }

  public Properties getProducerProps() {
    Properties props = getAdminProps();
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return props;
  }

  public Properties getConsumerProps(String groupId) {
    Properties props = getAdminProps();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    return props;
  }

  @SneakyThrows
  public <V> int writeToTopic(String topic, Stream<V> messages) {
    try (Producer<Void, V> producer = new KafkaProducer<>(getProducerProps())) {
      AtomicInteger counter = new AtomicInteger(0);
      messages.forEach(msg -> {
        ProducerRecord<Void, V> record = new ProducerRecord<>(topic, null, msg);
        try {
          RecordMetadata metadata = producer.send(record).get(1, TimeUnit.SECONDS);
          counter.incrementAndGet();
          log.info("Send record at: " + Instant.ofEpochMilli(metadata.timestamp()));
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });
      return counter.get();
    }
  }

  public int writeTextFilesToTopic(String topic, Path... paths) {
    return writeToTopic(topic, FileStreamUtil.filesByline(paths));
  }

  protected DataSystemDiscoveryConfig getDiscoveryConfig(String topicPrefix) {
    KafkaDataSystemConfig.Discovery.DiscoveryBuilder builder = KafkaDataSystemConfig.Discovery.builder();
    builder.servers(Arrays.asList(bootstrapServers));
    if (!Strings.isNullOrEmpty(topicPrefix)) builder.topicPrefix(topicPrefix);
    return builder.build();
  }

  protected DataSystemConfig.DataSystemConfigBuilder getSystemConfigBuilder(String name,
      boolean withTopicPrefix, boolean withFormat) {
    DataSystemConfig.DataSystemConfigBuilder builder = DataSystemConfig.builder();
    builder.datadiscovery(getDiscoveryConfig(withTopicPrefix?name+".":null));
    if (withFormat) builder.format(new Configuration());
    builder.type(ExternalDataType.source);
    builder.name(name);
    return builder;
  }





//    @Disabled("fix after Flink monitoring idling is solved")
//    @Test
//    @SneakyThrows
//    public void testDatasetMonitoringWithPrefix() {
//String[] topics = {"bookclub.book", "book.json"};

//        writeTextFilesToTopic(topics[0], "key", BookClub.BOOK_FILES);
//
//        String dsName = "bookclub";
//        DataSourceUpdate dsUpdate = DataSourceUpdate.builder()
//                .name(dsName).discoverTables(true)
//                .config(
//                    DataSystemConfig.builder().format(new JsonLineFormat.Configuration()).build())
//                .source(
//                    KafkaDataSystem.builder().servers(Arrays.asList(bootstrapServers)).topicPrefix(dsName+".").build())
//                .build();
//
//        testBookSourceTable(dsName,dsUpdate);
//    }
//
//    @Disabled("fix after Flink monitoring idling is solved")
//    @Test
//    @SneakyThrows
//    public void testDatasetMonitoringWithExtension() {
//        writeTextFilesToTopic(topics[1], "key", BookClub.BOOK_FILES);
//
//        String dsName = "test";
//        DataSourceUpdate dsUpdate = DataSourceUpdate.builder()
//                .name(dsName).discoverTables(true)
//                .source(KafkaDataSystem.builder().servers(Arrays.asList(bootstrapServers)).build())
//                .build();
//
//        testBookSourceTable(dsName,dsUpdate);
//    }
//
//    public void testBookSourceTable(String dsName, DataSourceUpdate dsUpdate) {
//        initialize(IntegrationTestSettings.getFlinkWithDB());
//
//        ErrorCollector errors = ErrorCollector.root();
//        sourceRegistry.addOrUpdateSource(dsUpdate, errors);
//        assertFalse(errors.isFatal(), errors.toString());
//
//        //Needs some time to wait for the flink pipeline to compile data
//
//        SourceDataset ds = sourceRegistry.getDataset(dsName);
//        assertEquals(1, ds.getTables().size());
//        TableSource book = ds.getTable("book");
//        assertNotNull(book);
//        SourceTableStatistics stats = book.getStatistics();
//        assertNotNull(stats);
//        assertEquals(4,stats.getCount());
//    }


}
