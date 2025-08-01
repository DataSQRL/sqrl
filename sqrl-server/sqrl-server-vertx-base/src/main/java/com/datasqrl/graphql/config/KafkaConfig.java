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
package com.datasqrl.graphql.config;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
public abstract class KafkaConfig {

  protected Map<String, String> config = new HashMap<>();

  public KafkaConfig(JsonObject json) {
    for (Map.Entry<String, Object> entry : json) {
      config.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
    }

    checkNotNull(
        config.get(BOOTSTRAP_SERVERS_CONFIG),
        "The '%s' config must be provided".formatted(BOOTSTRAP_SERVERS_CONFIG));
  }

  @JsonAnyGetter
  public Map<String, String> getConfig() {
    return config;
  }

  @JsonAnySetter
  public void setConfig(String key, String value) {
    config.put(key, value);
  }

  @Getter
  @Setter
  @NoArgsConstructor
  public static class KafkaSubscriptionConfig extends KafkaConfig {

    public KafkaSubscriptionConfig(JsonObject json) {
      super(json);
    }

    public KafkaSubscriptionConfig(Map<String, String> config) {
      super(config);
    }

    public Map<String, String> asMap() {
      var finalConfig = new HashMap<>(config);
      finalConfig.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());

      return Collections.unmodifiableMap(finalConfig);
    }
  }

  @Getter
  @Setter
  @NoArgsConstructor
  public static class KafkaMutationConfig extends KafkaConfig {

    public KafkaMutationConfig(JsonObject json) {
      super(json);
    }

    public KafkaMutationConfig(Map<String, String> config) {
      super(config);
    }

    public Map<String, String> asMap(boolean transactional) {
      var finalConfig = new HashMap<>(config);

      if (transactional) {
        finalConfig.put(TRANSACTIONAL_ID_CONFIG, "sqrl-mutation-" + UUID.randomUUID());
        finalConfig.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
      }

      return Collections.unmodifiableMap(finalConfig);
    }
  }
}
