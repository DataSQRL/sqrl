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
import com.nextbreakpoint.flink.client.model.JobStatus;
import java.nio.file.Path;
import java.util.ArrayList;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class FlinkSqlRunnerIT {

  @RegisterExtension final FlinkContainerExtension flink = new FlinkContainerExtension();

  @Test
  void givenPlan_whenInvokingFormatFunction_thenSuccess() throws Exception {
    var testCase = "flink-functions";
    compilePlan(testCase);

    // Step 1: Submit the job inside the running container
    var output =
        flink
            .getContainer()
            .execInContainer(
                "flink",
                "run",
                "./plugins/flink-sql-runner/flink-sql-runner.uber.jar",
                "--planfile",
                "/flink/sql/" + testCase + "/build/deploy/plan/flink-compiled-plan.json")
            .getStdout();

    //    assertThat(output)
    //        // check for flink runner functions
    //        .contains(text_search.class.getName())
    //        // check for sqrl specific functions
    //        .contains(Noop.class.getName())
    //        .contains(HashColumns.class.getName());

    log.info("Container output: {}", output);

    var clientLog =
        flink
            .getContainer()
            .execInContainer("ls", "-1", "/opt/flink/log")
            .getStdout()
            .lines()
            .filter(name -> name.startsWith("flink--client-") && name.endsWith(".log"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("TaskExecutor log not found"));
    log.info(
        "Flink log: {}",
        flink.getContainer().execInContainer("cat", "/opt/flink/log/" + clientLog).getStdout());

    // Extract Job ID from stdout
    var jobId =
        output
            .lines()
            .filter(line -> line.contains("Job has been submitted with JobID"))
            .map(line -> line.substring(line.lastIndexOf(" ") + 1).trim())
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Job ID not found in output"));

    // Poll Flink API to wait for job completion
    await()
        .atMost(20, SECONDS)
        .pollInterval(100, MILLISECONDS)
        .ignoreExceptions()
        .untilAsserted(
            () -> flink.getClient().getJobStatusInfo(jobId),
            jobStatus -> assertThat(jobStatus.getStatus()).isEqualTo(JobStatus.FINISHED));

    var logFile =
        flink
            .getContainer()
            .execInContainer("ls", "-1", "/opt/flink/log")
            .getStdout()
            .lines()
            .filter(name -> name.startsWith("flink--taskexecutor") && name.endsWith(".out"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("TaskExecutor log not found"));

    var taskExecutorLogs =
        flink.getContainer().execInContainer("cat", "/opt/flink/log/" + logFile).getStdout();

    // check log printed, means sqrl concat function was executed correctly
    assertThat(taskExecutorLogs)
        .as("Expected output not found in TaskManager logs")
        .contains("Hello, Bob");
  }

  @SneakyThrows
  private void compilePlan(String name) {
    var pkg = Path.of("target/test-classes/usecases", name, "package.json").toAbsolutePath();
    assertThat(pkg).isRegularFile();

    var baseDir = pkg.getParent();
    var args = new ArrayList<String>();
    args.add("compile");
    args.add(pkg.getFileName().toString());
    var statusHook = new AssertStatusHook();
    var code =
        new DatasqrlCli(baseDir, statusHook, true).getCmd().execute(args.toArray(String[]::new));

    assertThat(statusHook.failure()).isNull();
    assertThat(statusHook.isSuccess());
    assertThat(code).isZero();
  }
}
