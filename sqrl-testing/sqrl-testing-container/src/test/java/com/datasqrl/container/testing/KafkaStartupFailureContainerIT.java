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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.output.ToStringConsumer;

/**
 * Test to verify that the Vert.x service properly terminates when Kafka configuration is invalid or
 * missing, instead of continuing to run in a broken state.
 */
@Slf4j
public class KafkaStartupFailureContainerIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "sensors-mutation";
  }

  @Test
  void givenMissingKafkaEnvVar_whenServerStarts_thenServiceTerminatesCleanly() {
    // First compile the SQRL script that uses Kafka
    compileSqrlProject(testDir);

    // Create a server container but don't set the KAFKA_BOOTSTRAP_SERVERS env var
    // which should cause the service to fail and terminate
    var deployPlanPath = testDir.resolve("build/deploy/plan");
    assertThat(deployPlanPath).exists().isDirectory();

    // Use ToStringConsumer to capture logs even if container fails
    var logConsumer = new ToStringConsumer();

    serverContainer =
        createServerContainer(testDir)
            .withEnv("SQRL_DEBUG", "1")
            // Set an invalid bootstrap server URL
            .withEnv("KAFKA_BOOTSTRAP_SERVERS", "${KAFKA_BOOTSTRAP_SERVERS}")
            .withLogConsumer(logConsumer);

    assertThatThrownBy(() -> serverContainer.start())
        .isExactlyInstanceOf(ContainerLaunchException.class)
        .hasMessageContaining("failed");

    // Get container logs from the consumer
    var logs = logConsumer.toUtf8String();
    log.info("Container logs:\n{}", logs);

    // Verify the container is not running
    assertThat(serverContainer.isRunning()).isFalse();

    // Verify the expected error messages are present
    assertThat(logs).contains("Invalid url in bootstrap.servers: ${KAFKA_BOOTSTRAP_SERVERS}");
    assertThat(logs).contains("Unable to create GraphQL");
    assertThat(logs).contains("Failed to deploy GraphQL verticle");
  }

  @Test
  void givenInvalidKafkaBootstrapServers_whenServerStarts_thenServiceTerminatesCleanly() {
    // First compile the SQRL script that uses Kafka
    compileSqrlProject(testDir);

    // Create a server container with an invalid Kafka bootstrap server address
    var deployPlanPath = testDir.resolve("build/deploy/plan");
    assertThat(deployPlanPath).exists().isDirectory();

    // Use ToStringConsumer to capture logs even if container fails
    var logConsumer = new ToStringConsumer();

    serverContainer =
        createServerContainer(testDir)
            .withEnv("SQRL_DEBUG", "1")
            // Set an invalid bootstrap server URL
            .withEnv("KAFKA_BOOTSTRAP_SERVERS", "not-a-valid-url:definitely-not-a-port")
            .withLogConsumer(logConsumer);

    assertThatThrownBy(() -> serverContainer.start())
        .isExactlyInstanceOf(ContainerLaunchException.class)
        .hasMessageContaining("failed");

    // Get container logs from the consumer
    var logs = logConsumer.toUtf8String();
    log.info("Container logs:\n{}", logs);

    // Verify the container is not running
    assertThat(serverContainer.isRunning()).isFalse();

    // Verify error messages
    assertThat(logs).contains("Invalid url in bootstrap.servers");
    assertThat(logs).contains("Failed to deploy GraphQL verticle, will trigger orderly shutdown");
  }
}
