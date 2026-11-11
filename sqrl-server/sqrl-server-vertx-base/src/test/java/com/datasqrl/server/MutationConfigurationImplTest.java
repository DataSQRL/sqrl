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
package com.datasqrl.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.server.MutationConfigurationImpl.ComputeInputColumns;
import com.datasqrl.server.io.SinkProducer;
import com.datasqrl.server.kafka.JsonSerializer;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingEnvironmentImpl;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MutationConfigurationImplTest {

  private static final JsonSerializer KEY_SERIALIZER = new JsonSerializer();

  @Test
  void givenComputedKeyColumn_whenGettingRecords_thenKeyContainsComputedValue() {
    var env = mutationOf(new HashMap<>(Map.of("sensorid", 1)));
    Map<String, ComputeInputColumns> computedColumns = Map.of("tenantId", e -> "tenant-1");

    var records =
        MutationConfigurationImpl.getRecords(env, List.of("sensorid", "tenantId"), computedColumns);

    assertThat(serializedKeys(records))
        .containsExactly("{\"sensorid\":1,\"tenantId\":\"tenant-1\"}");
  }

  @Test
  void givenOmittedAndNullKeyColumn_whenGettingRecords_thenBothKeysContainNull() {
    var explicitNull = new HashMap<String, Object>();
    explicitNull.put("zone", null);
    var env = mutationOf(List.of(new HashMap<>(), explicitNull));

    var records = MutationConfigurationImpl.getRecords(env, List.of("zone"), Map.of());

    assertThat(serializedKeys(records)).containsExactly("{\"zone\":null}", "{\"zone\":null}");
  }

  @Test
  void givenNoKeyColumns_whenGettingRecords_thenRecordHasNoKey() {
    var env = mutationOf(new HashMap<>(Map.of("sensorid", 1)));

    var records = MutationConfigurationImpl.getRecords(env, List.of(), Map.of());

    assertThat(records).singleElement().extracting(SinkProducer.Record::key).isNull();
  }

  private static DataFetchingEnvironment mutationOf(Object input) {
    return DataFetchingEnvironmentImpl.newDataFetchingEnvironment()
        .arguments(Map.of("event", input))
        .build();
  }

  private static List<String> serializedKeys(List<SinkProducer.Record> records) {
    return records.stream()
        .map(rec -> new String(KEY_SERIALIZER.serialize("topic", rec.key()), UTF_8))
        .toList();
  }
}
