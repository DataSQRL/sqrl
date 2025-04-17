/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.sqrl.runner;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.datasqrl.cmd.RootCommand;
import com.nextbreakpoint.flink.client.api.ApiException;
import com.nextbreakpoint.flink.client.api.FlinkApi;
import com.nextbreakpoint.flink.client.model.JobIdsWithStatusOverview;
import com.nextbreakpoint.flink.client.model.TerminationMode;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AbstractITSupport {

  protected GenericContainer<?> flinkContainer;

  @SuppressWarnings("resource")
  @BeforeEach
  void startFlink() {
    flinkContainer = new GenericContainer<>(DockerImageName.parse("sqrl-flink-runner")).withExposedPorts(8081)
        .withFileSystemBind("target/test-classes/use-cases", // local path
            "/flink/sql", // container path
            BindMode.READ_ONLY)
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

    client.getApiClient()
        .setHttpClient(client.getApiClient().getHttpClient().newBuilder().connectTimeout(Duration.ofMinutes(2))
            .writeTimeout(Duration.ofMinutes(2)).readTimeout(Duration.ofMinutes(2)).build());

    await().atMost(100, SECONDS).pollInterval(500, MILLISECONDS).ignoreExceptions().until(() -> {
      log.info("Awaiting for flink api to warm up");
      return client.getJobsOverview() != null;
    });

    final JobIdsWithStatusOverview statusOverview = client.getJobIdsWithStatusesOverview();
    statusOverview.getJobs().forEach(jobIdWithStatus -> {
      try {
        client.cancelJob(jobIdWithStatus.getId(), TerminationMode.CANCEL);
      } catch (ApiException ignored) {
      }
    });

    return client;
  }

  public void untilAssert(ThrowingRunnable assertion) {
    await().atMost(20, SECONDS).pollInterval(100, MILLISECONDS).ignoreExceptions().untilAsserted(assertion);
  }

  @SneakyThrows
  public Path compilePlan(String name)   {
    Path script = Path.of("target/test-classes/use-cases", name, name+".sqrl").toAbsolutePath();
    assertThat(script).exists();
    Path baseDir = script.getParent();
    List<String> arguments = new ArrayList<>();
    arguments.add("compile");
    arguments.add(script.getFileName().toString());
    arguments.add("-c");
    arguments.add("package.json");
    AssertStatusHook statusHook = new AssertStatusHook();
    int code = new RootCommand(baseDir, statusHook).getCmd().execute(arguments.toArray(String[]::new));
    if(statusHook.failure() != null) {
      throw statusHook.failure();
    }
    if (statusHook.isSuccess() && code != 0)
      Assertions.assertEquals(0, code);
    
    return Path.of("target/test-classes/use-cases", name, "deploy/plan/flink-compiled-plan.json");
  }

}
