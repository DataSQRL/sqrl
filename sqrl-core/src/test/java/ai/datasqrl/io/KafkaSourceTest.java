package ai.datasqrl.io;


import ai.datasqrl.TestUtil;
import ai.datasqrl.api.ConfigurationTest;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.formats.JsonLineFormat;
import ai.datasqrl.io.impl.kafka.KafkaSourceImplementation;
import ai.datasqrl.io.sources.DataSourceConfiguration;
import ai.datasqrl.io.sources.DataSourceUpdate;
import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.server.Environment;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class KafkaSourceTest {

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
    Environment env = null;

    @BeforeEach
    public void before() throws Exception {
        FileUtils.cleanDirectory(ConfigurationTest.dbPath.toFile());
        bootstrapServers = CLUSTER.bootstrapServers().split(";");
        CLUSTER.createTopics(topics);
    }

    @AfterEach
    public void after() throws Exception {
        if (env!=null) env.close();
        env = null;
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
            writeToTopic(topics[0], addDefaultKey("key", TestUtil.files2StringByLine(ConfigurationTest.BOOK_FILES)));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                log.warn("Received record: " + record.value());
                numRecords++;
            }
        }
        assertEquals(4,numRecords);
    }

    @Test
    @SneakyThrows
    public void testDatasetMonitoringWithPrefix() {
        writeToTopic(topics[0], addDefaultKey("key",TestUtil.files2StringByLine(ConfigurationTest.BOOK_FILES)));

        String dsName = "bookclub";
        DataSourceUpdate dsUpdate = DataSourceUpdate.builder()
                .name(dsName).discoverTables(true)
                .config(
                    DataSourceConfiguration.builder().format(new JsonLineFormat.Configuration()).build())
                .source(
                    KafkaSourceImplementation.builder().servers(Arrays.asList(bootstrapServers)).topicPrefix(dsName+".").build())
                .build();

        testBookSourceTable(dsName,dsUpdate);
    }

    @Test
    @SneakyThrows
    public void testDatasetMonitoringWithExtension() {
        writeToTopic(topics[1], addDefaultKey("key",TestUtil.files2StringByLine(ConfigurationTest.BOOK_FILES)));

        String dsName = "test";
        DataSourceUpdate dsUpdate = DataSourceUpdate.builder()
                .name(dsName).discoverTables(true)
                .source(KafkaSourceImplementation.builder().servers(Arrays.asList(bootstrapServers)).build())
                .build();

        testBookSourceTable(dsName,dsUpdate);
    }

    public void testBookSourceTable(String dsName, DataSourceUpdate dsUpdate) {
        SqrlSettings settings = ConfigurationTest.getDefaultSettings(true);
        env = Environment.create(settings);

        DatasetRegistry registry = env.getDatasetRegistry();

        ErrorCollector errors = ErrorCollector.root();
        registry.addOrUpdateSource(dsUpdate, errors);
        if (errors.hasErrors()) System.out.println(errors);
        assertFalse(errors.isFatal());

        //Needs some time to wait for the flink pipeline to compile data

        SourceDataset ds = registry.getDataset(dsName);
        assertEquals(1, ds.getTables().size());
        SourceTable book = ds.getTable("book");
        assertNotNull(book);
        SourceTableStatistics stats = book.getStatistics();
        assertNotNull(stats);
        assertEquals(4,stats.getCount());
    }


}
