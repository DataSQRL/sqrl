package ai.dataeng.sqml.io;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.TestUtil;
import ai.dataeng.sqml.api.ConfigurationTest;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.io.formats.JsonLineFormat;
import ai.dataeng.sqml.io.impl.file.DirectorySourceImplementation;
import ai.dataeng.sqml.io.impl.kafka.KafkaSourceImplementation;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.DataSourceUpdate;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.stats.SourceTableStatistics;
import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

    Properties kafkaProps;
    String[] bootstrapServers;
    String[] topics = {"bookclub.book"};
    Environment env = null;

    @BeforeEach
    public void before() throws Exception {
        bootstrapServers = CLUSTER.bootstrapServers().split(";");
        kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", CLUSTER.bootstrapServers());
//        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", 0);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer");

        CLUSTER.createTopics(topics);
    }

    public Properties getConsumerProps(String groupId) {
        Properties props = new Properties(kafkaProps);
        props.put("group.id",groupId);
        return props;
    }


    @AfterEach
    public void after() throws Exception {
        if (env!=null) env.close();
        env = null;
        CLUSTER.deleteAllTopicsAndWait(0L);
    }

    public static<K,V> Stream<Pair<K,V>> addDefaultKey(K key, Stream<V> values) {
        return values.map(v -> Pair.of(key,v));
    }

    @SneakyThrows
    private<K,V> void writeToTopic(String topic, Stream<Pair<K,V>> messages) {
        Producer<K,V> producer;
        producer = new KafkaProducer<K,V>(kafkaProps);
        messages.forEach(msg -> {
            ProducerRecord<K,V> record = new ProducerRecord<>(topic,msg.getKey(),msg.getValue());
            try {
                RecordMetadata metadata = producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testKafka() {
        writeToTopic(topics[0], addDefaultKey("key",TestUtil.files2StringByLine(ConfigurationTest.BOOK_FILES)));
        int numRecords = 0;
        try (KafkaConsumer<String,String> consumer = new KafkaConsumer<>(getConsumerProps("test1"))) {
            consumer.subscribe(ImmutableList.of(topics[0]));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
                numRecords++;
            }
        }
        assertEquals(4,numRecords);
    }

    //@Test
    @SneakyThrows
    public void testDatasetMonitoring() {
        writeToTopic(topics[0], addDefaultKey("key",TestUtil.files2StringByLine(ConfigurationTest.BOOK_FILES)));


        SqrlSettings settings = ConfigurationTest.getDefaultSettings(true);
        env = Environment.create(settings);

        DatasetRegistry registry = env.getDatasetRegistry();
        String dsName = "bookclub";
        DataSourceUpdate dsUpdate = DataSourceUpdate.builder()
                .name(dsName).discoverTables(true)
                .config(DataSourceConfiguration.builder().format(new JsonLineFormat.Configuration()).build())
                .source(KafkaSourceImplementation.builder().servers(Arrays.asList(bootstrapServers)).topicPrefix(dsName).build())
                .build();

        ErrorCollector errors = ErrorCollector.root();
        registry.addOrUpdateSource(dsUpdate, errors);
        if (errors.isFatal()) System.out.println(errors);
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
