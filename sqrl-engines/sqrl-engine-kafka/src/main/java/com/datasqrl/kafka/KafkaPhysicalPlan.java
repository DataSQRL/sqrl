package com.datasqrl.kafka;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.serializer.Deserializer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.SneakyThrows;
import lombok.Value;

@Value
public class KafkaPhysicalPlan implements EnginePhysicalPlan {

  List<KafkaTopic> topics;

  @Override
  public void writeTo(Path deployDir, String stageName, Deserializer serializer)
      throws IOException {
    //Write out topic creation shell script
    StringBuilder b = new StringBuilder();
    b.append("#!/bin/bash");
    for (KafkaTopic topic : topics) {
      b.append("\n");
      b.append(String.format(
          "/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 " +
              "--topic %s --partitions 1 --replication-factor 1", topic.getTopicName()));
    }
    b.append("\nexit 0;");
    Files.writeString(deployDir.resolve("create-topics.sh"), b.toString());
  }

  @SneakyThrows
  private void createTopicSh(Path deployDir, List<MutationCoords> mutations,
      List<SubscriptionCoords> subscriptions) {

  }

}
