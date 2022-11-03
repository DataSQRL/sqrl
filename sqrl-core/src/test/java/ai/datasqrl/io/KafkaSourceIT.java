package ai.datasqrl.io;


import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.formats.JsonLineFormat;
import ai.datasqrl.io.impl.kafka.KafkaSource;
import ai.datasqrl.io.sources.DataSourceConfig;
import ai.datasqrl.io.sources.DataSourceUpdate;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.physical.stream.inmemory.io.FileStreamUtil;
import ai.datasqrl.util.data.BookClub;
import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
//TODO: flink integration currently hangs on monitoring
public class KafkaSourceIT extends AbstractSQRLIT {

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
    String[] topics = {"bookclub.book","book.json"};

    @BeforeEach
    public void before() throws Exception {
        bootstrapServers = CLUSTER.bootstrapServers().split(";");
        CLUSTER.createTopics(topics);
    }

    @AfterEach
    public void after() throws Exception {
        CLUSTER.deleteAllTopicsAndWait(0L);
    }

    public Properties getAdminProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", CLUSTER.bootstrapServers());
        return props;
    }

    public Properties getProducerProps() {
        Properties props = getAdminProps();
        props.put("retries", 0);
        //props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public Properties getConsumerProps(String groupId) {
        Properties props = getAdminProps();
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,100);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,10000);
        props.put("group.id",groupId);
        return props;
    }



    public static<K,V> Stream<Pair<K,V>> addDefaultKey(K key, Stream<V> values) {
        return values.map(v -> Pair.of(key,v));
    }

    @SneakyThrows
    private<K,V> void writeToTopic(String topic, Stream<Pair<K,V>> messages) {
        try (Producer<K,V> producer = new KafkaProducer<K,V>(getProducerProps())) {
            messages.forEach(msg -> {
                ProducerRecord<K, V> record = new ProducerRecord<>(topic, msg.getKey(), msg.getValue());
                try {
                    RecordMetadata metadata = producer.send(record).get(1, TimeUnit.SECONDS);
                    log.warn("Send record at: " + Instant.ofEpochMilli(metadata.timestamp()));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void writeTextFilesToTopic(String topic, String key, Path... paths) {
        writeToTopic(topics[0], addDefaultKey(key, FileStreamUtil.filesByline(paths)));
    }

    @Test
    @SneakyThrows
    public void testKafka() {
        Admin admin = Admin.create(getAdminProps());
        String clusterId = admin.describeCluster().clusterId().get();
        assertNotNull(clusterId);
        log.warn("Cluster id: " + clusterId);
        assertEquals(Set.of(topics), admin.listTopics().names().get());

        int numRecords = 0;
        try (KafkaConsumer<String,String> consumer = new KafkaConsumer<>(getConsumerProps("test1"))) {
            consumer.subscribe(ImmutableList.of(topics[0]));
            writeTextFilesToTopic(topics[0], "key", BookClub.BOOK_FILES);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                log.warn("Received record: " + record.value());
                numRecords++;
            }
        }
        assertEquals(4,numRecords);
    }

    @Disabled("fix after Flink monitoring idling is solved")
    @Test
    @SneakyThrows
    public void testDatasetMonitoringWithPrefix() {
        writeTextFilesToTopic(topics[0], "key", BookClub.BOOK_FILES);

        String dsName = "bookclub";
        DataSourceUpdate dsUpdate = DataSourceUpdate.builder()
                .name(dsName).discoverTables(true)
                .config(
                    DataSourceConfig.builder().format(new JsonLineFormat.Configuration()).build())
                .source(
                    KafkaSource.builder().servers(Arrays.asList(bootstrapServers)).topicPrefix(dsName+".").build())
                .build();

        testBookSourceTable(dsName,dsUpdate);
    }

    @Disabled("fix after Flink monitoring idling is solved")
    @Test
    @SneakyThrows
    public void testDatasetMonitoringWithExtension() {
        writeTextFilesToTopic(topics[1], "key", BookClub.BOOK_FILES);

        String dsName = "test";
        DataSourceUpdate dsUpdate = DataSourceUpdate.builder()
                .name(dsName).discoverTables(true)
                .source(KafkaSource.builder().servers(Arrays.asList(bootstrapServers)).build())
                .build();

        testBookSourceTable(dsName,dsUpdate);
    }

    public void testBookSourceTable(String dsName, DataSourceUpdate dsUpdate) {
        initialize(IntegrationTestSettings.getFlinkWithDB());

        ErrorCollector errors = ErrorCollector.root();
        sourceRegistry.addOrUpdateSource(dsUpdate, errors);
        assertFalse(errors.isFatal(), errors.toString());

        //Needs some time to wait for the flink pipeline to compile data

        SourceDataset ds = sourceRegistry.getDataset(dsName);
        assertEquals(1, ds.getTables().size());
        SourceTable book = ds.getTable("book");
        assertNotNull(book);
        SourceTableStatistics stats = book.getStatistics();
        assertNotNull(stats);
        assertEquals(4,stats.getCount());
    }


}
