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
package com.datasqrl.container.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class ExternalRedpandaContainerIT extends SqrlContainerTestBase {

  private static final String REDPANDA_IMAGE = "redpandadata/redpanda:latest";
  private static final String REDPANDA_CONTAINER_NAME = "redpanda";
  private static final int REDPANDA_PORT = 9093;

  private GenericContainer<?> redpandaContainer;

  @Override
  protected String getTestCaseName() {
    return "flink+kafka";
  }

  @BeforeEach
  void setUpExternalRedpanda() {
    startRedpandaContainer();
  }

  @AfterEach
  void tearDownExternalRedpanda() {
    if (redpandaContainer != null && redpandaContainer.isRunning()) {
      log.info("Stopping external Redpanda container");
      redpandaContainer.stop();
      redpandaContainer = null;
    }
  }

  @Test
  @SneakyThrows
  void givenExternalRedpandaContainer_whenCompileAndRunFlinkKafka_thenUsesExternalKafka() {
    // Given - External Redpanda container is running on port 9093
    assertThat(redpandaContainer.isRunning()).isTrue();
    assertThat(redpandaContainer.getMappedPort(REDPANDA_PORT)).isPositive();

    // When - Compile SQRL script with external Kafka configuration
    var cmdContainer = createCmdContainerWithExternalKafka();
    cmd = cmdContainer.withCommand("test", "flink_kafka.sqrl");

    log.info("Starting compilation with external Redpanda container");
    log.info(
        "DataSQRL container environment: KAFKA_HOST={}, KAFKA_PORT={},"
            + " PROPERTIES_BOOTSTRAP_SERVERS={}",
        REDPANDA_CONTAINER_NAME,
        REDPANDA_PORT,
        REDPANDA_CONTAINER_NAME + ":" + REDPANDA_PORT);
    cmd.start();

    // Wait for compilation to complete
    await().atMost(Duration.ofMinutes(5)).until(() -> !cmd.isRunning());

    var exitCode = cmd.getCurrentContainerInfo().getState().getExitCodeLong();
    var logs = cmd.getLogs();

    if (exitCode != 0) {
      log.error("SQRL compilation failed with exit code {}\n{}", exitCode, logs);
      throw new ContainerError("SQRL compilation failed", exitCode, logs);
    }

    log.info("SQRL script test was successful with external Kafka");

    // Then - Verify external Kafka configuration in generated artifacts
    assertThat(logs).contains("Snapshot OK for ApplicationStatusTest");

    // Verify that no internal Kafka processes were started
    assertThat(logs)
        .as("Should not start internal Kafka when external Kafka is configured")
        .doesNotContain("Starting Redpanda")
        .doesNotContain("rpk redpanda");

    // Verify log files and build ownership
    assertLogFiles(logs, testDir);
    assertBuildNotOwnedByRoot(testDir, logs);

    log.info("External Redpanda integration test completed successfully");
  }

  private void startRedpandaContainer() {
    log.info("Starting external Redpanda container on port {}", REDPANDA_PORT);

    redpandaContainer =
        new GenericContainer<>(DockerImageName.parse(REDPANDA_IMAGE))
            .withNetwork(sharedNetwork)
            .withNetworkAliases(REDPANDA_CONTAINER_NAME)
            .withExposedPorts(REDPANDA_PORT, 8081) // Kafka port and schema registry
            .withCommand(
                "redpanda",
                "start",
                "--overprovisioned",
                "--smp",
                "1",
                "--memory",
                "1G",
                "--reserve-memory",
                "0M",
                "--node-id",
                "0",
                "--kafka-addr",
                "0.0.0.0:" + REDPANDA_PORT,
                "--advertise-kafka-addr",
                REDPANDA_CONTAINER_NAME + ":" + REDPANDA_PORT,
                "--pandaproxy-addr",
                "0.0.0.0:8082",
                "--advertise-pandaproxy-addr",
                REDPANDA_CONTAINER_NAME + ":8082",
                "--schema-registry-addr",
                "0.0.0.0:8081",
                "--rpc-addr",
                "0.0.0.0:33145",
                "--advertise-rpc-addr",
                REDPANDA_CONTAINER_NAME + ":33145",
                "--check=false")
            .waitingFor(
                Wait.forLogMessage(".*Successfully started Redpanda!.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    redpandaContainer.start();

    var mappedPort = redpandaContainer.getMappedPort(REDPANDA_PORT);
    log.info("External Redpanda container started successfully on port {}", mappedPort);
  }

  private GenericContainer<?> createCmdContainerWithExternalKafka() {
    var container =
        createCmdContainer(testDir, true)
            .withNetwork(sharedNetwork)
            .withEnv("KAFKA_HOST", REDPANDA_CONTAINER_NAME)
            .withEnv("KAFKA_PORT", String.valueOf(REDPANDA_PORT))
            .withEnv("PROPERTIES_BOOTSTRAP_SERVERS", REDPANDA_CONTAINER_NAME + ":" + REDPANDA_PORT);

    // Add additional mount to resolve the symlink
    // flink+kafka/loan-local -> ../banking/loan-local
    var bankingDir = testDir.getParent().resolve("banking");

    if (bankingDir.toFile().exists()) {
      container =
          container.withFileSystemBind(
              bankingDir.toString(), BUILD_DIR + "/../banking", BindMode.READ_ONLY);
      log.info("Mounted banking directory: {} -> {}", bankingDir, BUILD_DIR + "/../banking");
    }

    return container;
  }
}
