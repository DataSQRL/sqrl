package com.datasqrl.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.io.KafkaBaseTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.data.Retail.RetailScriptNames;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

public class AvroKafkaIntegrationTest extends AbstractGraphqlTest {

  CompletableFuture<ExecutionResult> fut;

  String orderCounts = "subscription OrderCount {\n"
      + "  OrderCount {\n"
      + "    timeSec\n"
      + "    volume\n"
      + "    number\n"
      + "  }\n"
      + "}";

  Schema avroSchema;

  private static final String TOPIC = "orders";

  @BeforeEach
  void setUp() throws Exception {
    CLUSTER.createTopic(TOPIC);
    avroSchema = new Schema.Parser().parse(Retail.INSTANCE.getRootPackageDirectory()
        .resolve("ecommerce-avro/orders.avsc").toFile());
    System.setProperty("datasqrl.kafka_servers", CLUSTER.bootstrapServers());
    TestScript script = Retail.INSTANCE.getScript(RetailScriptNames.AVRO_KAFKA);
    Path compile = compiler.compile(
        Retail.INSTANCE.getRootPackageDirectory(), packageOverride, script.getScriptPath(),
        script.getGraphQLSchemas().get(0).getSchemaPath());

    fut = executeSql(compile);
    events = new ArrayList<>();
  }

  @SneakyThrows
  @Test
  public void singleSubscriptionMutationTest() {
    Thread.sleep(5000);

    CountDownLatch countDownLatch = subscribeToAlert(orderCounts);

    Thread.sleep(1000);

    int numOrders = 10;
    List<GenericRecord> orders = new ArrayList<>(numOrders);
    for (int i = 0; i < 10; i++) {
      orders.add(createRandomOrder());
    }

    writeToTopic(TOPIC,orders.stream().map(o -> KafkaBaseTest.serializeAvro(o, avroSchema)), ValueType.BYTE);

    Thread.sleep(2000);
    writeToTopic(TOPIC,orders.stream().map(o -> KafkaBaseTest.serializeAvro(o, avroSchema)), ValueType.BYTE);

    countDownLatch.await(120, TimeUnit.SECONDS);

    fut.cancel(true);
    assertEquals(countDownLatch.getCount(), 0);

    assertEquals(1, events.size());
    System.out.println("Event Payload:" + events.get(0));
  }

  private static final Random RANDOM = new Random();

  private GenericRecord createRandomOrder() {
    GenericRecord order = new GenericData.Record(avroSchema);
    order.put("id", RANDOM.nextLong());
    order.put("customerid", RANDOM.nextLong());
    order.put("time", Instant.now().toString());

    List<GenericRecord> entries = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      GenericRecord entry = new GenericData.Record(avroSchema.getField("entries").schema().getElementType());
      entry.put("productid", RANDOM.nextInt());
      entry.put("quantity", RANDOM.nextInt(10) + 1);
      entry.put("unit_price", RANDOM.nextDouble() * 100);
      entry.put("discount", RANDOM.nextDouble());
      entries.add(entry);
    }
    order.put("entries", entries);

    return order;
  }

}
