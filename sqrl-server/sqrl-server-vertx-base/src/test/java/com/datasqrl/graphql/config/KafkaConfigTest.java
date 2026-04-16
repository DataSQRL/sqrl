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

import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.graphql.config.KafkaConfig.KafkaMutationConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaConfigTest {

  @Test
  void givenNoUserOverrides_whenAsMap_thenAppliesFailFastTimeoutDefaults() {
    var config = new HashMap<String, String>();
    config.put("bootstrap.servers", "localhost:9092");
    var mutationConfig = new KafkaMutationConfig(config);

    Map<String, String> result = mutationConfig.asMap(false);

    assertThat(result)
        .containsEntry(MAX_BLOCK_MS_CONFIG, KafkaMutationConfig.DEFAULT_MAX_BLOCK_MS)
        .containsEntry(REQUEST_TIMEOUT_MS_CONFIG, KafkaMutationConfig.DEFAULT_REQUEST_TIMEOUT_MS)
        .containsEntry(DELIVERY_TIMEOUT_MS_CONFIG, KafkaMutationConfig.DEFAULT_DELIVERY_TIMEOUT_MS);
  }

  @Test
  void givenUserOverrides_whenAsMap_thenPreservesUserValues() {
    var config = new HashMap<String, String>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put(MAX_BLOCK_MS_CONFIG, "1000");
    config.put(REQUEST_TIMEOUT_MS_CONFIG, "2000");
    config.put(DELIVERY_TIMEOUT_MS_CONFIG, "3000");
    var mutationConfig = new KafkaMutationConfig(config);

    Map<String, String> result = mutationConfig.asMap(false);

    assertThat(result)
        .containsEntry(MAX_BLOCK_MS_CONFIG, "1000")
        .containsEntry(REQUEST_TIMEOUT_MS_CONFIG, "2000")
        .containsEntry(DELIVERY_TIMEOUT_MS_CONFIG, "3000");
  }
}
