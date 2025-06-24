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

import static org.assertj.core.api.Assertions.assertThat;

import com.nextbreakpoint.flink.client.model.JobStatus;
import org.junit.jupiter.api.Test;

class FlinkSqlRunnerIT extends AbstractITSupport {

  @Test
  void givenPlan_whenInvokingFormatFunction_thenSuccess() throws Exception {
    var testCase = "flink-functions";
    super.compilePlan(testCase);

    var client = createClient(flinkContainer.getMappedPort(8081));

    // Step 1: Submit the job inside the running container
    var output =
        flinkContainer
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

    System.out.println(output);

    var clientLog =
        flinkContainer
            .execInContainer("ls", "-1", "/opt/flink/log")
            .getStdout()
            .lines()
            .filter(name -> name.startsWith("flink--client-") && name.endsWith(".log"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("TaskExecutor log not found"));
    System.out.println(
        flinkContainer.execInContainer("cat", "/opt/flink/log/" + clientLog).getStdout());

    // Extract Job ID from stdout
    var jobId =
        output
            .lines()
            .filter(line -> line.contains("Job has been submitted with JobID"))
            .map(line -> line.substring(line.lastIndexOf(" ") + 1).trim())
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Job ID not found in output"));

    // Poll Flink API to wait for job completion
    untilAssert(
        () -> {
          var jobStatus = client.getJobStatusInfo(jobId);

          assertThat(jobStatus.getStatus()).isIn(JobStatus.FINISHED);
        });

    var logFile =
        flinkContainer
            .execInContainer("ls", "-1", "/opt/flink/log")
            .getStdout()
            .lines()
            .filter(name -> name.startsWith("flink--taskexecutor") && name.endsWith(".out"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("TaskExecutor log not found"));

    var taskExecutorLogs =
        flinkContainer.execInContainer("cat", "/opt/flink/log/" + logFile).getStdout();

    // check log printed, means sqrl concat function was executed correctly
    assertThat(taskExecutorLogs)
        .as("Expected output not found in TaskManager logs")
        .contains("Hello, Bob");
  }
}
