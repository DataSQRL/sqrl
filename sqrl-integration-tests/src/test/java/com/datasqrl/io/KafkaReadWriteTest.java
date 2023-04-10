package com.datasqrl.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.datasqrl.util.data.Retail;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

public class KafkaReadWriteTest extends KafkaBaseTest {

  @Test
  @SneakyThrows
  public void testKafkaReadWrite() {
    String topic = "orders";
    createTopics(new String[]{topic});
    Admin admin = Admin.create(getAdminProps());
    String clusterId = admin.describeCluster().clusterId().get();
    assertNotNull(clusterId);
    assertEquals(Set.of(topic), admin.listTopics().names().get());

    int recordsRead = 0;
    int recordsWritten = writeTextFilesToTopic(topic, Retail.INSTANCE.getDataDirectory().resolve("orders.json"));
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProps("test1"))) {
      consumer.subscribe(ImmutableList.of(topic));


      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
      for (ConsumerRecord<String, String> record : records) {
        recordsRead++;
      }
    }
    assertEquals(recordsWritten, recordsRead);
  }


}