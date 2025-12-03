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

import com.datasqrl.cli.output.TestOutputManager;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.env.GlobalEnvironmentStore;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.ConfigLoaderUtils;
import java.time.LocalDateTime;
import picocli.CommandLine;

@CommandLine.Command(name = "test", description = "Compiles, then tests a SQRL script")
public class TestCmd extends AbstractCompileCmd {

  @Override
  protected void execute(ErrorCollector errors) throws Exception {
    // Skip execution in case we call the CMD from a test class, as it will be executed manually.
    if (cli.internalTestExec) {
      return;
    }

    try (var outputMgr = new TestOutputManager(cli.rootDir)) {
      outputMgr.disableConsoleLogs();

      var formatter = getOutputFormatter();
      formatter.header("DataSQRL Test Execution");

      var targetDir = getTargetDir();
      var planDir = targetDir.resolve(SqrlConstants.PLAN_DIR);

      // Start services before testing
      formatter.phaseStart("Initializing test environment");
      getOsProcessManager().startDependentServices(planDir);

      // Test
      var env = GlobalEnvironmentStore.getAll();
      var sqrlConfig = ConfigLoaderUtils.loadResolvedConfig(errors, getBuildDir());
      var flinkConfig = ConfigLoaderUtils.loadFlinkConfig(planDir);

      var sqrlTest =
          new DatasqrlTest(
              cli.rootDir, planDir, sqrlConfig, flinkConfig, env, outputMgr, formatter);
      var testExitCode = sqrlTest.run();
      exitCode.set(testExitCode);

      var success = testExitCode == 0;
      formatter.buildStatus(success, getElapsedTime(), LocalDateTime.now());

      if (!success) {
        formatter.helpLink("Help 1", "https://docs.datasqrl.com/docs/howto/testing");
      }
    }
  }

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.TEST;
  }
}
