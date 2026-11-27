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
package com.datasqrl.planner.tables;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.io.tables.TableType;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class FlinkConnectorConfigWrapperTest {

  @Test
  void givenKafkaConnector_whenGetStorage_thenTopicWithTheNameOfTheTopicOption() {
    var config = config(Map.of("connector", "Upsert-Kafka", "topic", "customers"));

    assertThat(config.getTableType()).isEqualTo(TableType.VERSIONED_STATE);
    assertThat(config.getStorageType()).contains(StorageType.TOPIC);
    assertThat(config.getStorageName()).contains("customers");
  }

  @Test
  void givenOptionThatNamesTheStorageOfAnotherConnector_whenGetStorageName_thenItIsNotUsed() {
    var jdbc = config(Map.of("connector", "jdbc", "table-name", "orders", "path", "/data/orders"));
    var files = config(Map.of("connector", "filesystem", "topic", "orders"));

    assertThat(jdbc.getStorageType()).contains(StorageType.TABLE);
    assertThat(jdbc.getStorageName()).contains("orders");
    assertThat(files.getStorageType()).contains(StorageType.FILE);
    assertThat(files.getStorageName()).isEmpty();
  }

  @Test
  void givenConnectorThatIsNotKnown_whenGetStorage_thenEmptyAndStreamTable() {
    var config = config(Map.of("connector", "pulsar", "topic", "customers"));

    assertThat(config.getTableType()).isEqualTo(TableType.STREAM);
    assertThat(config.getStorageType()).isEmpty();
    assertThat(config.getStorageName()).isEmpty();
  }

  @Test
  void givenNoConnector_whenGetStorage_thenEmptyAndRelation() {
    var config = config(Map.of());

    assertThat(config.getTableType()).isEqualTo(TableType.RELATION);
    assertThat(config.getStorageType()).isEmpty();
    assertThat(config.getStorageName()).isEmpty();
  }

  private static FlinkConnectorConfigWrapper config(Map<String, String> options) {
    return new FlinkConnectorConfigWrapper(options, Optional.empty());
  }
}
