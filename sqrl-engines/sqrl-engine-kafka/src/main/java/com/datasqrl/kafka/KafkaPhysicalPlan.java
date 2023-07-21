package com.datasqrl.kafka;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.serializer.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Value;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Value
public class KafkaPhysicalPlan implements EnginePhysicalPlan {
  SqrlConfig config;

  List<NewTopic> topics;

  @Override
  public void writeTo(Path deployDir, String stageName, Deserializer serializer)
      throws IOException {
    //Write out topic creation shell script
    StringBuilder b = new StringBuilder();
    b.append("#!/bin/bash");
    for (NewTopic topic : topics) {
      b.append("\n");
      b.append(String.format(
          "/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server %s " +
              "--topic %s --partitions %s --replication-factor %s",
          this.config.getSubConfig("connector").asString("bootstrap.servers").get(),
          topic.getName(),
              Math.max(topic.getNumPartitions(), 1),
              Math.max(topic.getReplicationFactor(), 1)
          ));
    }
    b.append("\nexit 0;");
    Files.writeString(deployDir.resolve("create-topics.sh"), b.toString());

    ObjectMapper mapper = new Deserializer().getJsonMapper();
    mapper.writeValue(deployDir.resolve("kafka-plan.json").toFile(), this.topics);
  }
}
