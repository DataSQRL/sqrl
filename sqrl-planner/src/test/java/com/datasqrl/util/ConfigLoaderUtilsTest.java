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
package com.datasqrl.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJsonImpl;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigTest;
import com.datasqrl.engine.database.relational.JdbcStatement;
import com.datasqrl.engine.log.kafka.NewTopic;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigLoaderUtilsTest {

  private ErrorCollector errors;
  private SqrlConfig config;

  @TempDir private Path tempDir;

  @BeforeEach
  void setup() {
    errors = ErrorCollector.root();
    config = SqrlConfig.createCurrentVersion();
  }

  @Test
  void givenJsonConfigFile_whenLoadViaCommons_thenParsesCorrectly() {
    var res =
        ConfigLoaderUtils.loadResolvedConfigFromFile(
            errors, Path.of("src/test/resources/config/config1.json"), null);
    var config1 = getSqrlConfig(res);
    testConfig1(config1);
    testSubConf(config1.getSubConfig("sub-conf"));
  }

  @Test
  void givenLoadedConfig_whenWriteToFile_thenLoadsIdentically() {
    var res =
        ConfigLoaderUtils.loadResolvedConfigFromFile(
            errors, Path.of("src/test/resources/config/config1.json"), null);
    var loadedConfig = getSqrlConfig(res);
    var tempFile2 = createTempFile();
    loadedConfig.toFile(tempFile2);

    res = ConfigLoaderUtils.loadResolvedConfigFromFile(errors, tempFile2, null);
    var reloadedConfig = getSqrlConfig(res);
    testConfig1(reloadedConfig);
    testSubConf(reloadedConfig.getSubConfig("sub-conf"));
  }

  @Test
  void givenNewConfig_whenSetPropertiesAndObjects_thenPersistsCorrectly() {
    var newConf = SqrlConfig.createCurrentVersion();
    newConf.setProperty("test", true);
    var tc = new SqrlConfigTest.TestClass(9, "boat", List.of("x", "y", "z"));
    newConf.getSubConfig("clazz").setProperties(tc);
    assertThat(newConf.asBool("test").get()).isTrue();
    assertThat(newConf.getSubConfig("clazz").allAs(SqrlConfigTest.TestClass.class).get().field3)
        .isEqualTo(tc.field3);
    var tempFile2 = createTempFile();
    newConf.toFile(tempFile2, true);
    var config2 =
        ((PackageJsonImpl) ConfigLoaderUtils.loadResolvedConfigFromFile(errors, tempFile2, null))
            .getSqrlConfig();
    assertThat(config2.asBool("test").get()).isTrue();
    var tc2 = config2.getSubConfig("clazz").allAs(SqrlConfigTest.TestClass.class).get();
    assertThat(tc2.field1).isEqualTo(tc.field1);
    assertThat(tc2.field2).isEqualTo(tc.field2);
    assertThat(tc2.field3).isEqualTo(tc.field3);
  }

  @Test
  @SneakyThrows
  void givenConfigWithData_whenToFile_thenWritesAndLoadsCorrectly() {
    config.setProperty("key1", "value1");
    config.setProperty("key2", 42);
    config.getSubConfig("nested").setProperty("key", "nestedValue");

    var tempFile = createTempFile();
    config.toFile(tempFile);

    assertThat(tempFile).exists();

    SqrlConfig loadedConfig =
        ((PackageJsonImpl) ConfigLoaderUtils.loadResolvedConfigFromFile(errors, tempFile, null))
            .getSqrlConfig();
    assertThat(loadedConfig.asString("key1").get()).isEqualTo("value1");
    assertThat(loadedConfig.asInt("key2").get()).isEqualTo(42);
    assertThat(loadedConfig.getSubConfig("nested").asString("key").get()).isEqualTo("nestedValue");
  }

  @Test
  void givenNoPaths_loadDefaultConfig_thenReturnsDefaults() {
    var underTest = ConfigLoaderUtils.loadDefaultConfig(errors);

    assertThat(underTest).isNotNull();
    assertThat(underTest.getVersion()).isEqualTo(1);
    assertThat(underTest.getEnabledEngines()).contains("vertx", "postgres", "kafka", "flink");
    assertThat(underTest.getTestConfig()).isNotNull();
    assertThat(underTest.getEngines().getEngineConfig("flink")).isPresent();
    assertThat(underTest.getScriptConfig().getGraphql()).isEmpty();
    assertThat(underTest.getScriptConfig().getMainScript()).isEmpty();
  }

  @Test
  void givenNoPaths_whenLoadingUnresolvedConfig_thenReturnsDefaults() {
    var underTest = ConfigLoaderUtils.loadUnresolvedConfig(errors, List.of());

    assertThat(underTest).isNotNull();
    assertThat(underTest.getVersion()).isEqualTo(1);
    assertThat(underTest.getEnabledEngines()).contains("vertx", "postgres", "kafka", "flink");
    assertThat(underTest.getTestConfig()).isNotNull();
    assertThat(underTest.getEngines().getEngineConfig("flink")).isPresent();
    assertThat(underTest.getScriptConfig().getGraphql()).isEmpty();
    assertThat(underTest.getScriptConfig().getMainScript()).isEmpty();
  }

  @Test
  void givenSinglePath_whenLoadingUnresolvedConfig_thenOverridesDefaults() {
    var underTest =
        ConfigLoaderUtils.loadUnresolvedConfig(
            errors, List.of(Path.of("src/test/resources/config/test-package.json")));

    assertThat(underTest).isNotNull();
    assertThat(underTest.getVersion()).isEqualTo(1);

    assertThat(underTest.getEnabledEngines()).contains("test");

    assertThat(underTest.getTestConfig()).isNotNull();
    assertThat(underTest.getEngines().getEngineConfig("flink")).isPresent();
    assertThat(underTest.getScriptConfig().getGraphql()).isEmpty();
    assertThat(underTest.getScriptConfig().getMainScript()).isEmpty();
  }

  @Test
  void loadResolvedConfig_shouldThrow_whenDeployDirDoesNotExist() {
    Path invalidDir = tempDir.resolve("nonexistent");

    assertThatThrownBy(() -> ConfigLoaderUtils.loadResolvedConfig(null, invalidDir))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nonexistent' does not exist");
  }

  @Test
  void loadFlinkConfig_shouldLoadSuccessfully_whenConfigExists() throws IOException {
    // Arrange
    Path planDir = tempDir.resolve("plan");
    planDir.toFile().mkdirs();

    Path flinkConfig = planDir.resolve("flink-config.yaml");
    String yamlContent = "jobmanager.rpc.address: localhost";

    Files.writeString(flinkConfig, yamlContent);

    // Act
    Configuration conf = ConfigLoaderUtils.loadFlinkConfig(planDir);

    // Assert
    assertThat(conf).isNotNull();
    assertThat(conf.getString("jobmanager.rpc.address", null)).isEqualTo("localhost");
  }

  @Test
  void loadFlinkConfig_shouldThrow_whenPlanDirDoesNotExist() {
    Path invalidDir = tempDir.resolve("doesNotExist");

    assertThatThrownBy(() -> ConfigLoaderUtils.loadFlinkConfig(invalidDir))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("plan dir does not exist");
  }

  @Test
  void loadFlinkConfig_shouldThrow_whenFlinkConfigYamlMissing() {
    Path planDir = tempDir.resolve("planWithoutYaml");
    planDir.toFile().mkdirs();

    assertThatThrownBy(() -> ConfigLoaderUtils.loadFlinkConfig(planDir))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'flink-config.yaml' does not found");
  }

  private SqrlConfig getSqrlConfig(PackageJson packageJson) {
    return ((PackageJsonImpl) packageJson).getSqrlConfig();
  }

  private void testConfig1(SqrlConfig config) {
    assertThat(config.asInt("key2").get()).isEqualTo(5);
    assertThat(config.asLong("key2").get()).isEqualTo(5L);
    assertThat(config.asString("key1").get()).isEqualTo("value1");
    assertThat(config.asBool("key3").get()).isTrue();
    assertThat(config.asList("list", String.class).get()).isEqualTo(List.of("a", "b", "c"));
    var map = config.asMap("map", SqrlConfigTest.TestClass.class).get();
    assertThat(map).hasSize(3);
    assertThat(map.get("e2").field1).isEqualTo(7);
    assertThat(map.get("e3").field2).isEqualTo("flip");
    assertThat(map.get("e1").field3).isEqualTo(List.of("a", "b", "c"));
    assertThat(config.getVersion()).isEqualTo(1);

    var x1 = config.as("x1", SqrlConfigTest.ConstraintClass.class).get();
    assertThat(x1.integer).isEqualTo(2);
    assertThat(x1.flag).isFalse();
    assertThat(x1.string).isEqualTo("hello world");

    var x2 = config.as("x2", SqrlConfigTest.ConstraintClass.class).get();
    assertThat(x2.integer).isEqualTo(33);

    assertThatThrownBy(() -> config.as("xf1", SqrlConfigTest.ConstraintClass.class).get())
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("is not valid");

    assertThatThrownBy(() -> config.as("xf2", SqrlConfigTest.ConstraintClass.class).get())
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("Could not find key");

    var nested = config.as("nested", SqrlConfigTest.NestedClass.class).get();
    assertThat(nested.counter).isEqualTo(5);
    assertThat(nested.obj.integer).isEqualTo(33);
    assertThat(nested.obj.flag).isTrue();
  }

  private void testSubConf(SqrlConfig config) {
    assertThat(config.asString("delimited.config.option").get()).isEqualTo("that");
    assertThat(config.asInt("one").get()).isEqualTo(1);
    assertThat(config.asString("token").get()).isEqualTo("piff");
  }

  @Test
  void givenNonExistentPlanDir_whenLoadKafkaPhysicalPlan_thenThrowsIllegalArgumentException() {
    // Given
    Path nonExistentDir = tempDir.resolve("nonexistent");

    // When & Then
    assertThatThrownBy(() -> ConfigLoaderUtils.loadKafkaPhysicalPlan(nonExistentDir))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("plan dir does not exist");
  }

  @Test
  void givenPlanDirWithoutKafkaJson_whenLoadKafkaPhysicalPlan_thenReturnsEmpty()
      throws IOException {
    // Given
    Path planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);

    // When
    var result = ConfigLoaderUtils.loadKafkaPhysicalPlan(planDir);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void givenPlanDirWithValidKafkaJson_whenLoadKafkaPhysicalPlan_thenReturnsPopulatedPlan()
      throws IOException {
    // Given
    Path planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);

    String kafkaJsonContent =
        """
        {
          "topics": [
            {
              "topicName": "orders",
              "numPartitions": 3,
              "replicationFactor": 1
            },
            {
              "topicName": "customers",
              "numPartitions": 1,
              "replicationFactor": 1
            }
          ],
          "testRunnerTopics": [
            {
              "topicName": "test-topic-1",
              "numPartitions": 1,
              "replicationFactor": 1
            },
            {
              "topicName": "test-topic-2",
              "numPartitions": 1,
              "replicationFactor": 1
            }
          ]
        }
        """;

    Path kafkaJsonFile = planDir.resolve("kafka.json");
    Files.writeString(kafkaJsonFile, kafkaJsonContent);

    // When
    var result = ConfigLoaderUtils.loadKafkaPhysicalPlan(planDir);

    // Then
    assertThat(result).isPresent();
    var kafkaPlan = result.get();
    assertThat(kafkaPlan.topics()).hasSize(2);
    assertThat(kafkaPlan.topics().get(0).getTopicName()).isEqualTo("orders");
    assertThat(kafkaPlan.topics().get(0).getNumPartitions()).isEqualTo(3);
    assertThat(kafkaPlan.topics().get(1).getTopicName()).isEqualTo("customers");
    assertThat(kafkaPlan.topics().get(1).getNumPartitions()).isEqualTo(1);

    assertThat(kafkaPlan.testRunnerTopics().stream().map(NewTopic::getTopicName))
        .containsExactlyInAnyOrder("test-topic-1", "test-topic-2");
    assertThat(kafkaPlan.isEmpty()).isFalse();
  }

  @Test
  void
      givenPlanDirWithMalformedKafkaJson_whenLoadKafkaPhysicalPlan_thenThrowsIllegalStateException()
          throws IOException {
    // Given
    Path planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);

    String malformedKafkaJson = "{ invalid json }";
    Path kafkaJsonFile = planDir.resolve("kafka.json");
    Files.writeString(kafkaJsonFile, malformedKafkaJson);

    // When & Then
    assertThatThrownBy(() -> ConfigLoaderUtils.loadKafkaPhysicalPlan(planDir))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to load");
  }

  @Test
  void givenNonExistentPlanDir_whenLoadPostgresPhysicalPlan_thenThrowsIllegalArgumentException() {
    // Given
    Path nonExistentDir = tempDir.resolve("nonexistent");

    // When & Then
    assertThatThrownBy(() -> ConfigLoaderUtils.loadPostgresPhysicalPlan(nonExistentDir))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("plan dir does not exist");
  }

  @Test
  void givenPlanDirWithoutPostgresJson_whenLoadPostgresPhysicalPlan_thenReturnsEmpty()
      throws IOException {
    // Given
    Path planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);

    // When
    var result = ConfigLoaderUtils.loadPostgresPhysicalPlan(planDir);

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void givenPlanDirWithValidPostgresJson_whenLoadPostgresPhysicalPlan_thenReturnsJdbcPlan()
      throws IOException {
    // Given
    Path planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);

    String postgresJsonContent =
        """
        {
          "statements": [
            {
              "name": "create_users_table",
              "type": "TABLE",
              "sql": "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))"
            },
            {
              "name": "create_orders_table",
              "type": "TABLE",
              "sql": "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2))"
            },
            {
              "name": "create_index",
              "type": "INDEX",
              "sql": "CREATE INDEX idx_user_id ON orders(user_id)"
            }
          ]
        }
        """;

    Path postgresJsonFile = planDir.resolve("postgres.json");
    Files.writeString(postgresJsonFile, postgresJsonContent);

    // When
    var result = ConfigLoaderUtils.loadPostgresPhysicalPlan(planDir);

    // Then
    assertThat(result).isPresent();
    var jdbcPlan = result.get();
    assertThat(jdbcPlan.statements()).hasSize(3);

    var statements = jdbcPlan.statements();
    assertThat(statements.get(0).getName()).isEqualTo("create_users_table");
    assertThat(statements.get(0).getType()).isEqualTo(JdbcStatement.Type.TABLE);
    assertThat(statements.get(0).getSql()).contains("CREATE TABLE users");

    assertThat(statements.get(1).getName()).isEqualTo("create_orders_table");
    assertThat(statements.get(1).getType()).isEqualTo(JdbcStatement.Type.TABLE);
    assertThat(statements.get(1).getSql()).contains("CREATE TABLE orders");

    assertThat(statements.get(2).getName()).isEqualTo("create_index");
    assertThat(statements.get(2).getType()).isEqualTo(JdbcStatement.Type.INDEX);
    assertThat(statements.get(2).getSql()).contains("CREATE INDEX");
  }

  @Test
  void
      givenPlanDirWithMalformedPostgresJson_whenLoadPostgresPhysicalPlan_thenThrowsIllegalStateException()
          throws IOException {
    // Given
    Path planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);

    String malformedPostgresJson = "{ invalid json }";
    Path postgresJsonFile = planDir.resolve("postgres.json");
    Files.writeString(postgresJsonFile, malformedPostgresJson);

    // When & Then
    assertThatThrownBy(() -> ConfigLoaderUtils.loadPostgresPhysicalPlan(planDir))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to load");
  }

  @Test
  void givenPlanDirWithEmptyPostgresStatements_whenLoadPostgresPhysicalPlan_thenReturnsEmptyPlan()
      throws IOException {
    // Given
    Path planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);

    String emptyPostgresJson =
        """
        {
          "statements": []
        }
        """;

    Path postgresJsonFile = planDir.resolve("postgres.json");
    Files.writeString(postgresJsonFile, emptyPostgresJson);

    // When
    var result = ConfigLoaderUtils.loadPostgresPhysicalPlan(planDir);

    // Then
    assertThat(result).isPresent();
    assertThat(result.get().statements()).isEmpty();
  }

  @SneakyThrows
  private Path createTempFile() {
    return Files.createTempFile("configuration", ".json");
  }
}
