///*
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
//package com.datasqrl.io;
//
//
//import com.datasqrl.AbstractEngineIT;
//import com.datasqrl.io.formats.FormatFactory;
//import com.datasqrl.io.formats.JsonLineFormat;
//import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
//import com.datasqrl.io.tables.TableConfig;
//import com.datasqrl.util.FileStreamUtil;
//import com.google.common.collect.ImmutableList;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.nio.file.Path;
//import java.time.Duration;
//import java.time.Instant;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Stream;
//import lombok.SneakyThrows;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericDatumWriter;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.io.DatumWriter;
//import org.apache.avro.io.Encoder;
//import org.apache.avro.io.EncoderFactory;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.Producer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.apache.kafka.common.serialization.ByteArraySerializer;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//
//@Slf4j
//public class KafkaBaseTest extends AbstractEngineIT {
//
//  public static final int NUM_BROKERS = 1;
//
//  public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
//
//  String bootstrapServers;
//
//  @SneakyThrows
//  @BeforeAll
//  public static void startCluster() {
//    CLUSTER.start();
//    System.setProperty("kafka.bootstrap", CLUSTER.bootstrapServers());
//  }
//
//  @SneakyThrows
//  @BeforeEach
//  public void clear() {
//    CLUSTER.deleteAllTopicsAndWait(-1L);
//  }
//
////
//  @AfterAll
//  public static void stopCluster() throws IOException {
//    CLUSTER.stop();
//  }
//
//  public void createTopics(String[] topics) throws Exception {
//    bootstrapServers = CLUSTER.bootstrapServers();
//    CLUSTER.createTopics(topics);
//  }
//
//  @AfterEach
//  public void after() throws Exception {
//    CLUSTER.deleteAllTopicsAndWait(-1L);
//  }
//
//  public static Properties getAdminProps() {
//    Properties props = new Properties();
//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//    return props;
//  }
//
//  public enum ValueType {
//    TEXT, BYTE;
//  }
//
//  public Properties getProducerProps(ValueType valueType) {
//    Properties props = getAdminProps();
//    props.put(ProducerConfig.RETRIES_CONFIG, 0);
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//    switch (valueType) {
//      case TEXT:
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        break;
//      case BYTE:
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
//        break;
//      default:
//        throw new UnsupportedOperationException();
//    }
//
//    return props;
//  }
//
//  public static Properties getConsumerProps(String groupId) {
//    Properties props = getAdminProps();
//    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
//    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
//    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//    return props;
//  }
//
//  @SneakyThrows
//  public <V> int writeToTopic(String topic, Stream<V> messages, ValueType valueType) {
//    try (Producer<Void, V> producer = new KafkaProducer<>(getProducerProps(valueType))) {
//      AtomicInteger counter = new AtomicInteger(0);
//      messages.forEach(msg -> {
//        ProducerRecord<Void, V> record = new ProducerRecord<>(topic, null, msg);
//        try {
//          RecordMetadata metadata = producer.send(record).get(1, TimeUnit.SECONDS);
//          counter.incrementAndGet();
//          log.info("Send record at: " + Instant.ofEpochMilli(metadata.timestamp()));
//        } catch (Exception e) {
//          e.printStackTrace();
//          throw new RuntimeException(e);
//        }
//      });
//      return counter.get();
//    }
//  }
//
//  public int writeTextFilesToTopic(String topic, Path... paths) {
//    return writeToTopic(topic, FileStreamUtil.filesByline(paths), ValueType.TEXT);
//  }
//
//  protected TableConfig getSystemConfigBuilder(String name,
//      boolean withTopicPrefix) {
//    TableConfig.Builder tblBuilder = KafkaDataSystemFactory.getKafkaConfig(name, bootstrapServers, withTopicPrefix?name+".":null);
//    tblBuilder.getFormatConfig().setProperty(FormatFactory.FORMAT_NAME_KEY, JsonLineFormat.NAME);
//    return tblBuilder.build();
//  }
//
//
//  public static byte[] serializeAvro(GenericRecord record, Schema schema) {
//    byte[] bytes = null;
//    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//    Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
//    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
//
//    try {
//      writer.write(record, encoder);
//      encoder.flush();
//      bytes = outputStream.toByteArray();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//
//    return bytes;
//  }
//
//  public List<ConsumerRecord<String, String>> getAllInTopic(String topicName) {
//    List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
//    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProps("grouppid"))) {
//      consumer.subscribe(ImmutableList.of(topicName));
//
//      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
//      for (ConsumerRecord<String, String> record : records) {
//        allRecords.add(record);
//      }
//    }
//    return allRecords;
//  }
//
//
////    @Disabled("fix after Flink monitoring idling is solved")
////    @Test
////    @SneakyThrows
////    public void testDatasetMonitoringWithPrefix() {
////String[] topics = {"bookclub.book", "book.json"};
//
////        writeTextFilesToTopic(topics[0], "key", BookClub.BOOK_FILES);
////
////        String dsName = "bookclub";
////        DataSourceUpdate dsUpdate = DataSourceUpdate.builder()
////                .name(dsName).discoverTables(true)
////                .config(
////                    DataSystemConfig.builder().format(new JsonLineFormat.Configuration()).build())
////                .source(
////                    KafkaDataSystem.builder().servers(Arrays.asList(bootstrapServers)).topicPrefix(dsName+".").build())
////                .build();
////
////        testBookSourceTable(dsName,dsUpdate);
////    }
////
////    @Disabled("fix after Flink monitoring idling is solved")
////    @Test
////    @SneakyThrows
////    public void testDatasetMonitoringWithExtension() {
////        writeTextFilesToTopic(topics[1], "key", BookClub.BOOK_FILES);
////
////        String dsName = "test";
////        DataSourceUpdate dsUpdate = DataSourceUpdate.builder()
////                .name(dsName).discoverTables(true)
////                .source(KafkaDataSystem.builder().servers(Arrays.asList(bootstrapServers)).build())
////                .build();
////
////        testBookSourceTable(dsName,dsUpdate);
////    }
////
////    public void testBookSourceTable(String dsName, DataSourceUpdate dsUpdate) {
////        initialize(IntegrationTestSettings.getFlinkWithDB());
////
////        ErrorCollector errors = ErrorCollector.root();
////        sourceRegistry.addOrUpdateSource(dsUpdate, errors);
////        assertFalse(errors.isFatal(), errors.toString());
////
////        //Needs some time to wait for the flink pipeline to compile data
////
////        SourceDataset ds = sourceRegistry.getDataset(dsName);
////        assertEquals(1, ds.getTables().size());
////        TableSource book = ds.getTable("book");
////        assertNotNull(book);
////        SourceTableStatistics stats = book.getStatistics();
////        assertNotNull(stats);
////        assertEquals(4,stats.getCount());
////    }
//
//
//}
