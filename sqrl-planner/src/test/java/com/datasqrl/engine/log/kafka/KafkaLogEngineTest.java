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
package com.datasqrl.engine.log.kafka;

import static com.datasqrl.flinkrunner.connector.kafka.KeyFieldsOptions.SCAN_KEY_FIELDS_READ_FROM_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.server.MutationInsertType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaLogEngineTest {

  private static final String TIMESTAMP_FORMAT = "flexible-json.timestamp-format.standard";
  private static final String KEY_FIELDS_READ_FROM_VALUE_KEY =
      SCAN_KEY_FIELDS_READ_FROM_VALUE.key();

  private final ConnectorConf mutationConnectorConf = mock(ConnectorConf.class);

  private ErrorCollector errors;
  private KafkaLogEngine engine;
  private FlinkTableBuilder tableBuilder;

  @BeforeEach
  void setUp() {
    var engineConfig = mock(PackageJson.EngineConfig.class);
    when(engineConfig.getPropertyOptional(KafkaLogEngine.DEFAULT_TTL_KEY))
        .thenReturn(Optional.empty());
    when(engineConfig.getProperty("use-source-watermark")).thenReturn("false");
    when(engineConfig.getProperty("use-transaction-source-watermark")).thenReturn("false");
    when(engineConfig.getProperty("watermark")).thenReturn("0 ms");
    when(engineConfig.getProperty("transaction-watermark")).thenReturn("0 ms");
    when(engineConfig.getProperty("num-partitions")).thenReturn("3");
    when(engineConfig.getProperty("replication-factor")).thenReturn("1");

    var engines = mock(PackageJson.EnginesConfig.class);
    when(engines.getEngineConfigOrEmpty(KafkaLogEngineFactory.ENGINE_NAME))
        .thenReturn(engineConfig);
    var packageJson = mock(PackageJson.class);
    when(packageJson.getEngines()).thenReturn(engines);

    var streamConnectorConf = mock(ConnectorConf.class);
    when(streamConnectorConf.toMap()).thenReturn(Map.of("format", "flexible-json"));
    var connectorFactory = mock(ConnectorFactoryFactory.class);
    when(connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME))
        .thenReturn(streamConnectorConf);
    when(connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME + "-mutation"))
        .thenReturn(mutationConnectorConf);

    errors = ErrorCollector.root();
    engine = new KafkaLogEngine(packageJson, connectorFactory, errors);
  }

  @Test
  void givenKafkaSafeConnector_whenCreateKeyedMutation_thenReadsKeyFieldsFromValue() {
    var options = createKeyedMutation("kafka-safe", Map.of());

    assertThat(options)
        .containsEntry("key.fields", "sensorid")
        .containsEntry(KEY_FIELDS_READ_FROM_VALUE_KEY, "true");
    assertThat(warnings()).isEmpty();
  }

  @Test
  void givenKafkaConnector_whenCreateKeyedMutation_thenWarnsThatRecordsWithoutKeyAreDropped() {
    var options = createKeyedMutation("kafka", Map.of());

    assertThat(options)
        .containsEntry("key.fields", "sensorid")
        .doesNotContainKey(KEY_FIELDS_READ_FROM_VALUE_KEY);
    assertThat(warnings())
        .extracting(ErrorMessage::getMessage)
        .singleElement()
        .asString()
        .contains("Reading", "'kafka' connector", "drops records without a message key");
  }

  @Test
  void givenReadingFromValueDisabledInWith_whenCreateKeyedMutation_thenKeepsTheConfiguredValue() {
    var options =
        createKeyedMutation("kafka-safe", Map.of(KEY_FIELDS_READ_FROM_VALUE_KEY, "false"));

    assertThat(options).containsEntry(KEY_FIELDS_READ_FROM_VALUE_KEY, "false");
  }

  @Test
  void givenFormatOptionInWith_whenCreateKeyedMutation_thenAppliesItToKeyAndValueFormat() {
    var options = createKeyedMutation("kafka-safe", Map.of(TIMESTAMP_FORMAT, "SQL"));

    assertThat(options)
        .containsEntry("key." + TIMESTAMP_FORMAT, "SQL")
        .containsEntry("value." + TIMESTAMP_FORMAT, "SQL")
        .doesNotContainKey(TIMESTAMP_FORMAT);
  }

  @Test
  void givenValueFormatOptionInWith_whenCreateKeyedMutation_thenKeepsItOverTheDefault() {
    var options = createKeyedMutation("kafka-safe", Map.of("value." + TIMESTAMP_FORMAT, "SQL"));

    assertThat(options)
        .containsEntry("key." + TIMESTAMP_FORMAT, "ISO-8601")
        .containsEntry("value." + TIMESTAMP_FORMAT, "SQL");
  }

  @Test
  void givenNumPartitionsInWith_whenCreateMutation_thenOverridesTopicPartitionsAndDropsTheOption() {
    var table = createMutationTable("kafka-safe", Map.of("topic.num-partitions", "12"));

    assertThat(table.numPartitions()).isEqualTo(12);
    assertThat(tableBuilder.getConnectorOptions()).doesNotContainKey("topic.num-partitions");
  }

  @Test
  void givenNoNumPartitionsInWith_whenCreateMutation_thenUsesTheEngineDefault() {
    var table = createMutationTable("kafka-safe", Map.of());

    assertThat(table.numPartitions()).isEqualTo(3);
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-2", "many", ""})
  void givenInvalidNumPartitionsInWith_whenCreateMutation_thenFails(String value) {
    assertThatThrownBy(
            () -> createMutationTable("kafka-safe", Map.of("topic.num-partitions", value)))
        .hasMessageContaining(
            "Invalid 'topic.num-partitions' option on Kafka table 'Reading': expected a positive"
                + " integer, but got '%s'",
            value);
  }

  private Map<String, String> createKeyedMutation(
      String connector, Map<String, String> withOptions) {
    createMutationTable(connector, withOptions);
    return tableBuilder.getConnectorOptions();
  }

  private KafkaLogEngine.Table createMutationTable(
      String connector, Map<String, String> withOptions) {
    when(mutationConnectorConf.toMapWithSubstitution(any(ConnectorConf.Context.class)))
        .thenReturn(
            new HashMap<>(
                Map.of(
                    "connector",
                    connector,
                    "format",
                    "flexible-json",
                    TIMESTAMP_FORMAT,
                    "ISO-8601",
                    "topic",
                    "Reading")));

    tableBuilder = new FlinkTableBuilder().setName("Reading");
    tableBuilder.setColumnList(SqlNodeList.EMPTY);
    tableBuilder.setPartition(List.of("sensorid"));
    tableBuilder.setConnectorOptions(withOptions);

    return (KafkaLogEngine.Table)
        engine.createMutation(
            null, "Reading", tableBuilder, null, MutationInsertType.SINGLE, Optional.empty());
  }

  private List<ErrorMessage> warnings() {
    return errors.getErrors().stream().filter(ErrorMessage::isWarning).toList();
  }
}
