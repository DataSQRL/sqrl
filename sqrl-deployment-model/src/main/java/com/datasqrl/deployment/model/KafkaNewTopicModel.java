/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.deployment.model;

import java.util.List;
import java.util.Map;

/**
 * A Kafka topic definition in a deployment file.
 *
 * @param tableName the internal table id the planner assigned, e.g. {@code Orders_3}
 * @param logicalName the SQRL table name substituted for {@code ${sqrl:table-name}} in the topic
 *     template
 */
public record KafkaNewTopicModel(
    String topicName,
    String tableName,
    String logicalName,
    String format,
    int numPartitions,
    short replicationFactor,
    Type type,
    List<String> messageKeys,
    String messageSchema,
    Map<String, String> config) {

  public KafkaNewTopicModel(
      String topicName,
      String tableName,
      String logicalName,
      int numPartitions,
      short replicationFactor) {
    this(
        topicName,
        tableName,
        logicalName,
        null,
        numPartitions,
        replicationFactor,
        Type.SUBSCRIPTION,
        List.of(),
        "",
        Map.of());
  }

  public KafkaNewTopicModel(String topicName, String tableName, String logicalName) {
    this(topicName, tableName, logicalName, 1, (short) 1);
  }

  public enum Type {
    MUTATION,
    SUBSCRIPTION
  }
}
