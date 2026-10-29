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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.TestRunnerConfiguration;
import com.datasqrl.config.TestScriptConfig;
import com.datasqrl.deployment.model.KafkaIncludedTopicModel;
import com.datasqrl.deployment.model.KafkaNewTopicModel;
import com.datasqrl.engine.log.MutationEngine.MutationSettings;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.server.MutationInsertType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaLogEngineTest {

  private static final String TIMESTAMP_FORMAT = "flexible-json.timestamp-format.standard";
  private static final String KEY_FIELDS_READ_FROM_VALUE_KEY =
      SCAN_KEY_FIELDS_READ_FROM_VALUE.key();

  private final ConnectorConf mutationConnectorConf = mock(ConnectorConf.class);
  private final PackageJson packageJson = mock(PackageJson.class);
  private final ConnectorFactoryFactory connectorFactory = mock(ConnectorFactoryFactory.class);

  private ErrorCollector errors;
  private KafkaLogEngine engine;

  @BeforeEach
  void setUp() {
    var engineConfig = mock(PackageJson.EngineConfig.class);
    when(engineConfig.getPropertyOptional(KafkaLogEngine.DEFAULT_TTL_KEY))
        .thenReturn(Optional.empty());
    when(engineConfig.getProperty("use-source-watermark")).thenReturn("false");
    when(engineConfig.getProperty("use-transaction-source-watermark")).thenReturn("false");
    when(engineConfig.getProperty("watermark")).thenReturn("0 ms");
    when(engineConfig.getProperty("transaction-watermark")).thenReturn("0 ms");
    when(engineConfig.getProperty("num-partitions")).thenReturn("1");
    when(engineConfig.getProperty("replication-factor")).thenReturn("1");

    var engines = mock(PackageJson.EnginesConfig.class);
    when(engines.getEngineConfigOrEmpty(KafkaLogEngineFactory.ENGINE_NAME))
        .thenReturn(engineConfig);
    when(packageJson.getEngines()).thenReturn(engines);
    var testRunnerConfig = mock(TestRunnerConfiguration.class);
    when(testRunnerConfig.getCreateTopics()).thenReturn(List.of());
    when(packageJson.getTestConfig()).thenReturn(testRunnerConfig);

    var streamConnectorConf = mock(ConnectorConf.class);
    when(streamConnectorConf.toMap()).thenReturn(Map.of("format", "flexible-json"));
    when(connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME))
        .thenReturn(streamConnectorConf);
    when(connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME + "-mutation"))
        .thenReturn(mutationConnectorConf);

    errors = ErrorCollector.root();
    engine = newEngine(Map.of());
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
  void givenTableOfStandaloneInclude_whenCreateMutation_thenTableKeepsItsStandaloneInclude() {
    var table = createMutation("kafka-safe", readingTable(Map.of()), Optional.of("core"));

    assertThat(table.standaloneInclude()).contains("core");
  }

  @Test
  void givenOwnAndStandaloneIncludeMutations_whenPlanned_thenOnlyTheOwnTopicHasSettings() {
    var plan =
        plan(
            engine,
            mutation("Feedback", Optional.empty()),
            mutation("Order", Optional.of("shared")));

    assertThat(plan.topics())
        .singleElement()
        .satisfies(
            topic -> {
              assertThat(topic.topic().topicName()).isEqualTo("Feedback");
              assertThat(topic.topic().numPartitions()).isOne();
              assertThat(topic.topic().type()).isEqualTo(KafkaNewTopicModel.Type.MUTATION);
            });
    assertThat(plan.includedTopics())
        .containsExactly(new KafkaIncludedTopicModel("Order", "Order_1", "Order", "shared"));
  }

  @Test
  void givenStandaloneIncludeWithoutUsedTables_whenPlanned_thenNoticesThatItHasNoIncludedTopics() {
    var engine = newEngine(Map.of("shared", true, "unused", true, "catalog", false));

    plan(engine, mutation("Order", Optional.of("shared")));

    assertThat(errors.getErrors())
        .filteredOn(ErrorMessage::isNotice)
        .extracting(ErrorMessage::getMessage)
        .singleElement()
        .asString()
        .contains("'unused'", "\"standalone\"", "includedTopics");
  }

  private KafkaLogEngine newEngine(Map<String, Boolean> standaloneByInclude) {
    when(packageJson.getScriptConfig())
        .thenReturn(TestScriptConfig.withIncludes(standaloneByInclude));
    return new KafkaLogEngine(packageJson, connectorFactory, errors);
  }

  private static KafkaPhysicalPlan plan(KafkaLogEngine engine, KafkaLogEngine.Table... mutations) {
    var stagePlan = MaterializationStagePlan.builder().mutations(List.of(mutations)).build();
    return (KafkaPhysicalPlan) engine.plan(stagePlan);
  }

  private static KafkaLogEngine.Table mutation(String name, Optional<String> standaloneInclude) {
    return new KafkaLogEngine.Table(
        name, name + "_1", name, "flexible-json", List.of(), null, Map.of(), standaloneInclude);
  }

  private Map<String, String> createKeyedMutation(
      String connector, Map<String, String> withOptions) {
    var tableBuilder = readingTable(withOptions);
    createMutation(connector, tableBuilder, Optional.empty());
    return tableBuilder.getConnectorOptions();
  }

  private FlinkTableBuilder readingTable(Map<String, String> withOptions) {
    var tableBuilder = new FlinkTableBuilder().setName("Reading");
    tableBuilder.setColumnList(SqlNodeList.EMPTY);
    tableBuilder.setPartition(List.of("sensorid"));
    tableBuilder.setConnectorOptions(withOptions);
    return tableBuilder;
  }

  private KafkaLogEngine.Table createMutation(
      String connector, FlinkTableBuilder tableBuilder, Optional<String> standaloneInclude) {
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

    return (KafkaLogEngine.Table)
        engine.createMutation(
            null,
            "Reading",
            tableBuilder,
            null,
            new MutationSettings(MutationInsertType.SINGLE, Optional.empty(), standaloneInclude));
  }

  private List<ErrorMessage> warnings() {
    return errors.getErrors().stream().filter(ErrorMessage::isWarning).toList();
  }
}
