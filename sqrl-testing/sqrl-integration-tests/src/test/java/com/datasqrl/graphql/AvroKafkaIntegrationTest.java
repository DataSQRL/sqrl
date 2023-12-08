package com.datasqrl.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.graphql.inference.CipherUtil;
import com.datasqrl.io.KafkaBaseTest;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.data.Retail.RetailScriptNames;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
  FileResourceResolver fileResourceResolver;

  @BeforeEach
  void setUp() throws Exception {
    CLUSTER.createTopic(TOPIC);
    avroSchema = new Schema.Parser().parse(Retail.INSTANCE.getRootPackageDirectory()
        .resolve("ecommerce-avro/orders.avsc").toFile());
    System.setProperty("datasqrl.kafka_servers", CLUSTER.bootstrapServers());
    TestScript script = Retail.INSTANCE.getScript(RetailScriptNames.AVRO_KAFKA);
    fileResourceResolver = new FileResourceResolver(
        Retail.INSTANCE.getScript(RetailScriptNames.AVRO_KAFKA).getRootPackageDirectory());
    fut = execute(Retail.INSTANCE.getRootPackageDirectory(), script.getScriptPath(),
        script.getGraphQLSchemas().get(0).getSchemaPath());
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

    //Wait for a response
    String query = Files.readString(Path.of(fileResourceResolver.resolveFile(
        NamePath.of("c360-kafka-graphql", "schema-queries", "GetOrderCount.graphql")).get()));
    CountDownLatch queryLatch = new CountDownLatch(1);
    executePreparsedQueryUntilTrue(CipherUtil.sha256(query),
        new JsonObject().put("limit", 3), (resp)-> queryLatch.countDown(),
        (resp) -> {
          if (resp.statusCode() != 200) {
            fail(String.format("Non 200 response. %s", resp.body().encode()));
          }
          JsonArray jsonArray = resp.body().getJsonObject("data").getJsonArray("OrderCount");
          return !jsonArray.isEmpty();
        },
        20);

    queryLatch.await(120, TimeUnit.SECONDS);

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
