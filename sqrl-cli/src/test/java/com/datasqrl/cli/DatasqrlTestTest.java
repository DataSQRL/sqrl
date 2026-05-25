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
package com.datasqrl.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.cli.output.NoOutputFormatter;
import com.datasqrl.cli.output.TestOutputManager;
import com.datasqrl.config.PackageJson;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Optional;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatasqrlTestTest {

  @TempDir private Path tempDir;

  @Test
  void run_whenPipelineFailsToStart_recordsFailureWithNonZeroExit() throws Exception {
    var planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);
    // The compiled plan references an environment variable that is not provided. The runner throws
    // a clear IllegalStateException, which must be reported as a test failure rather than
    // swallowed.
    Files.writeString(
        planDir.resolve("flink-sql.sql"),
        "CREATE TABLE t (id INT) WITH ('connector' = 'datagen', 'id' = '${DEPLOYMENT_ID}');\n");

    var sqrlConfig = mock(PackageJson.class, RETURNS_DEEP_STUBS);
    when(sqrlConfig.getCompilerConfig().compileFlinkPlan()).thenReturn(false);
    when(sqrlConfig.getTestConfig().getSnapshotDir(any())).thenReturn(tempDir.resolve("snapshots"));
    when(sqrlConfig.getTestConfig().getTestDir(any())).thenReturn(Optional.empty());
    when(sqrlConfig.getTestConfig().getDelaySec()).thenReturn(5);
    when(sqrlConfig.getTestConfig().getMutationDelaySec()).thenReturn(0);
    when(sqrlConfig.getTestConfig().getRequiredCheckpoints()).thenReturn(0);

    var underTest =
        new DatasqrlTest(
            tempDir,
            planDir,
            sqrlConfig,
            new Configuration(),
            new HashMap<>(),
            new TestOutputManager(tempDir),
            new NoOutputFormatter());

    var exitCode = underTest.run();

    assertThat(exitCode).isNotZero();
  }
}
