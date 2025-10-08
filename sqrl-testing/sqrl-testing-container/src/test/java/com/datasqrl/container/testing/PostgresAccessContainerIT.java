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

import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@Slf4j
public class PostgresAccessContainerIT extends SqrlContainerTestBase {

  private GenericContainer<?> runContainer;

  @Override
  protected String getTestCaseName() {
    return "avro-schema";
  }

  @Test
  @SneakyThrows
  void givenRunningContainer_whenExecutePostgresVersionQuery_thenReturnsPostgresVersion() {
    // Start the run container which compiles and runs the server
    runContainer =
        createCmdContainer(testDir)
            .withCommand("run", "avro-schema.sqrl")
            .withExposedPorts(HTTP_SERVER_PORT)
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(HTTP_SERVER_PORT)
                    .forStatusCode(204)
                    .withStartupTimeout(Duration.ofSeconds(30)));

    runContainer.start();

    var logs = runContainer.getLogs();
    log.info("SQRL run command executed successfully");
    log.info("Container logs:\n{}", logs);

    // Verify server started successfully
    assertThat(logs).contains("GraphQL verticle deployed successfully");

    // Execute psql command to get PostgreSQL version
    var execResult =
        runContainer.execInContainer("psql", "-U", "postgres", "-c", "SELECT version();");

    // Verify the command executed successfully
    assertThat(execResult.getExitCode())
        .as("psql command should execute successfully")
        .isEqualTo(0);

    // Verify the output contains PostgreSQL version information
    var output = execResult.getStdout();
    assertThat(output).as("Output should contain 'PostgreSQL'").containsIgnoringCase("PostgreSQL");

    // Verify version number format (e.g., "PostgreSQL 14.x", "PostgreSQL 15.x", etc.)
    assertThat(output)
        .as("Output should contain version number")
        .containsPattern("PostgreSQL\\s+\\d+\\.\\d+");

    log.info("PostgreSQL version verification completed successfully");
  }

  @Override
  protected void cleanupContainers() {
    super.cleanupContainers();
    if (runContainer != null && runContainer.isRunning()) {
      runContainer.stop();
      runContainer = null;
    }
  }
}
