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
package com.datasqrl.graphql.kafka;

import com.datasqrl.graphql.config.ServerConfigProperties;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoordsVisitor;
import com.datasqrl.graphql.server.SubscriptionConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.schema.DataFetcher;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

/**
 * Spring-based subscription configuration that uses Reactor Kafka for subscriptions. Replaces the
 * Vert.x-based SubscriptionConfigurationImpl.
 */
@Slf4j
@RequiredArgsConstructor
public class SpringSubscriptionConfiguration implements SubscriptionConfiguration<DataFetcher<?>> {

  private final ServerConfigProperties config;
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public SubscriptionCoordsVisitor<DataFetcher<?>, Context> createSubscriptionFetcherVisitor() {
    return (coords, context) -> {
      var kafkaConfig = config.getKafkaSubscription();
      if (kafkaConfig == null) {
        throw new RuntimeException("Kafka subscription configuration is not set");
      }

      return env -> {
        var topic = coords.getTopic();
        var receiver = createKafkaReceiver(kafkaConfig, topic);

        return receiver
            .receive()
            .map(
                record -> {
                  try {
                    var value = record.value();
                    record.receiverOffset().acknowledge();
                    return objectMapper.readValue(
                        value, new TypeReference<Map<String, Object>>() {});
                  } catch (Exception e) {
                    log.error("Error deserializing Kafka message", e);
                    return Map.<String, Object>of("error", e.getMessage());
                  }
                })
            .filter(data -> matchesEqualityConditions(data, coords, env.getArguments()));
      };
    };
  }

  private KafkaReceiver<String, String> createKafkaReceiver(
      ServerConfigProperties.KafkaSubscriptionConfig kafkaConfig, String topic) {

    var props = new HashMap<String, Object>();
    kafkaConfig.getProperties().forEach(props::put);

    // Ensure unique consumer group for each subscription
    props.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        props.getOrDefault(ConsumerConfig.GROUP_ID_CONFIG, "sqrl-subscription")
            + "-"
            + UUID.randomUUID());

    var receiverOptions =
        ReceiverOptions.<String, String>create(props).subscription(Collections.singleton(topic));

    log.info("Creating Kafka subscription receiver for topic: {}", topic);

    return KafkaReceiver.create(receiverOptions);
  }

  private boolean matchesEqualityConditions(
      Map<String, Object> data, KafkaSubscriptionCoords coords, Map<String, Object> arguments) {

    var conditions = coords.getEqualityConditions();
    if (conditions == null || conditions.isEmpty()) {
      return true;
    }

    for (var entry : conditions.entrySet()) {
      var fieldName = entry.getKey();
      var fieldValue = data.get(fieldName);

      var paramHandler = entry.getValue();
      var expectedValue = resolveParameterValue(paramHandler, arguments, data);

      if (expectedValue != null && !expectedValue.equals(fieldValue)) {
        return false;
      }
    }

    return true;
  }

  private Object resolveParameterValue(
      Object paramHandler, Map<String, Object> arguments, Map<String, Object> data) {
    // This is a simplified implementation
    // The full implementation would need to handle various parameter types
    if (paramHandler instanceof String paramName) {
      return arguments.get(paramName);
    }
    return null;
  }
}
