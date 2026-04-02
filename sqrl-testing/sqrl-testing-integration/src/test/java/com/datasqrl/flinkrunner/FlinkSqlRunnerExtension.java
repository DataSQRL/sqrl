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
import static org.awaitility.Awaitility.await;

import com.nextbreakpoint.flink.client.api.ApiException;
import com.nextbreakpoint.flink.client.api.FlinkApi;
import com.nextbreakpoint.flink.client.model.TerminationMode;
import java.time.Duration;
import java.util.Objects;
import lombok.Getter;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class FlinkSqlRunnerExtension implements BeforeEachCallback, AfterEachCallback {

  private static final int FLINK_REST_PORT = 8081;

  @Getter private GenericContainer<?> container;
  @Getter private FlinkApi client;

  @SuppressWarnings("resource")
  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    var imgRepo = System.getProperty("flinkrunner.image.repo", "ghcr.io");
    var tag = System.getProperty("flinkrunner.version", "latest");
    if (!tag.equals("latest") && !tag.toLowerCase().endsWith("snapshot")) {
      tag += "-flink-2.2";
    }

    container =
        new GenericContainer<>(DockerImageName.parse(imgRepo + "/datasqrl/flink-sql-runner:" + tag))
            .withExposedPorts(FLINK_REST_PORT)
            .withFileSystemBind("target/test-classes/usecases", "/flink/sql", BindMode.READ_ONLY)
            .withCommand("bash", "-c", "bin/start-cluster.sh && tail -f /dev/null");
    container.start();

    client = createClient(container.getMappedPort(FLINK_REST_PORT));
  }

  @Override
  public void afterEach(ExtensionContext context) {
    client = null;
    if (container != null) {
      container.stop();
      container = null;
    }
  }

  private FlinkApi createClient(int serverPort) throws ApiException {
    var serverUrl = "http://localhost:" + serverPort;
    var flinkClient = new FlinkApi();
    flinkClient.getApiClient().setBasePath(serverUrl);

    flinkClient
        .getApiClient()
        .setHttpClient(
            flinkClient
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
        .until(flinkClient::getJobsOverview, Objects::nonNull);

    var statusOverview = flinkClient.getJobIdsWithStatusesOverview();
    statusOverview
        .getJobs()
        .forEach(
            jobIdWithStatus -> {
              try {
                flinkClient.cancelJob(jobIdWithStatus.getId(), TerminationMode.CANCEL);
              } catch (ApiException ignored) {
              }
            });

    return flinkClient;
  }
}
