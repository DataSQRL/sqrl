package com.datasqrl.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.IntegrationTestSettings.LogEngine;
import com.datasqrl.IntegrationTestSettings.StreamEngine;
import com.datasqrl.io.KafkaBaseTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.text.StringSubstitutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MiniClusterExtension.class)
class FlexibleJsonTest extends KafkaBaseTest {

  @SneakyThrows
  @Test
  public void testFlexibleJson() {
    initialize(IntegrationTestSettings.builder()
        .stream(StreamEngine.FLINK)
        .log(LogEngine.KAFKA)
        .build(), null, Optional.empty());

    CLUSTER.createTopic("orders");

    String[] flinkSql = Resources.toString(Resources.getResource("flexible-json/c360.sql"),
        StandardCharsets.UTF_8).split("\n\n");

    Configuration sEnvConfig = Configuration.fromMap(Map.of());
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment(sEnvConfig);

    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(Configuration.fromMap(Map.of())).build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
    StringSubstitutor substitutor = new StringSubstitutor((Map) System.getProperties());

    TableResult tableResult = null;
    for (String sql : flinkSql) {
      String replacedSql = substitutor.replace(sql);
      tableResult = tEnv.executeSql(replacedSql);
    }

    tableResult.print();

    List<ConsumerRecord<String, String>> orders = getAllInTopic("orders");

    assertEquals(20, orders.size());
    JsonNode jsonNode = new ObjectMapper().readTree(orders.get(0).value())
        .get("json");

    System.out.println(jsonNode);

    assertTrue(jsonNode instanceof ObjectNode);
    assertEquals("{\"int\":1,\"string\":\"str\",\"array\":[0,1,2],\"nested\":{\"key\":\"value\"}}", jsonNode.toString());
  }
}