package com.datasqrl.kafka;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.serializer.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.kafka.clients.admin.NewTopic;

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
          topic.name(),
          topic.numPartitions() == -1 ? 1 : topic.numPartitions(),
          topic.replicationFactor() == -1 ? 1 : topic.replicationFactor()
          ));
    }
    b.append("\nexit 0;");
    Files.writeString(deployDir.resolve("create-topics.sh"), b.toString());

    ObjectMapper mapper = new Deserializer().getJsonMapper();
    mapper.writeValue(deployDir.resolve("kafka-plan.json").toFile(), this.topics);
  }
}
