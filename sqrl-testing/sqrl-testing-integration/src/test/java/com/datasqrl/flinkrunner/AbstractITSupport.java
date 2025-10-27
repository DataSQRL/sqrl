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
package com.datasqrl.flinkrunner;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.cli.DatasqrlCli;
import com.nextbreakpoint.flink.client.api.ApiException;
import com.nextbreakpoint.flink.client.api.FlinkApi;
import com.nextbreakpoint.flink.client.model.TerminationMode;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class AbstractITSupport {

  protected GenericContainer<?> flinkContainer;

  @SuppressWarnings("resource")
  @BeforeEach
  void startFlink() {
    var imgRepo = System.getProperty("flinkrunner.image.repo", "ghcr.io");
    var tag = System.getProperty("flinkrunner.version", "latest");
    if (!tag.equals("latest") && !tag.toLowerCase().endsWith("snapshot")) {
      tag += "-flink-2.1";
    }

    flinkContainer =
        new GenericContainer<>(DockerImageName.parse(imgRepo + "/datasqrl/flink-sql-runner:" + tag))
            .withExposedPorts(8081)
            .withFileSystemBind("target/test-classes/usecases", "/flink/sql", BindMode.READ_ONLY)
            .withCommand("bash", "-c", "bin/start-cluster.sh && tail -f /dev/null");
    flinkContainer.start();
  }

  @AfterEach
  void stopFlink() {
    flinkContainer.stop();
  }

  protected FlinkApi createClient(int serverPort) throws ApiException {
    var serverUrl = "http://localhost:" + serverPort;
    var client = new FlinkApi();
    client.getApiClient().setBasePath(serverUrl);

    client
        .getApiClient()
        .setHttpClient(
            client
                .getApiClient()
                .getHttpClient()
                .newBuilder()
                .connectTimeout(Duration.ofMinutes(2))
                .writeTimeout(Duration.ofMinutes(2))
                .readTimeout(Duration.ofMinutes(2))
                .build());

    await()
        .atMost(100, SECONDS)
        .pollInterval(500, MILLISECONDS)
        .ignoreExceptions()
        .until(() -> client.getJobsOverview() != null);

    final var statusOverview = client.getJobIdsWithStatusesOverview();
    statusOverview
        .getJobs()
        .forEach(
            jobIdWithStatus -> {
              try {
                client.cancelJob(jobIdWithStatus.getId(), TerminationMode.CANCEL);
              } catch (ApiException ignored) {
              }
            });

    return client;
  }

  public void untilAssert(ThrowingRunnable assertion) {
    await()
        .atMost(20, SECONDS)
        .pollInterval(100, MILLISECONDS)
        .ignoreExceptions()
        .untilAsserted(assertion);
  }

  @SneakyThrows
  public Path compilePlan(String name) {
    Path script = Path.of("target/test-classes/usecases", name, name + ".sqrl").toAbsolutePath();
    assertThat(script).exists();
    Path baseDir = script.getParent();
    List<String> arguments = new ArrayList<>();
    arguments.add("compile");
    arguments.add(script.getFileName().toString());
    arguments.add("-c");
    arguments.add("package.json");
    AssertStatusHook statusHook = new AssertStatusHook();
    int code =
        new DatasqrlCli(baseDir, statusHook, true)
            .getCmd()
            .execute(arguments.toArray(String[]::new));
    if (statusHook.failure() != null) {
      throw statusHook.failure();
    }
    if (statusHook.isSuccess() && code != 0) assertThat(code).isZero();

    return Path.of("target/test-classes/usecases", name, "deploy/plan/flink-compiled-plan.json");
  }
}
