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

import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.ConfigLoaderUtils;
import picocli.CommandLine;

@CommandLine.Command(
    name = "run",
    description = "Compiles, then runs a SQRL script in a lightweight, standalone environment")
public class RunCmd extends AbstractCompileCmd {

  @Override
  protected void execute(ErrorCollector errors) throws Exception {
    // Skip execution in case we call the CMD from a test class, as it will be executed manually.
    if (cli.internalTestExec) {
      return;
    }

    // Start services before running
    var serviceManager = new DependentServiceManager(System.getenv());
    serviceManager.startServices();

    // Run
    var env = getSystemProperties();
    var targetDir = getTargetDir();
    var planDir = targetDir.resolve(SqrlConstants.PLAN_DIR);
    var sqrlConfig = ConfigLoaderUtils.loadResolvedConfig(errors, getBuildDir());
    var flinkConfig = ConfigLoaderUtils.loadFlinkConfig(planDir);

    var sqrlRun = new DatasqrlRun(planDir, sqrlConfig, flinkConfig, env, false);
    sqrlRun.run(true, true);
  }

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.RUN;
  }
}
