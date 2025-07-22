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
import com.datasqrl.env.GlobalEnvironmentStore;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigLoaderUtils;
import picocli.CommandLine;

@CommandLine.Command(
    name = "execute",
    description = "Executes an already compiled SQRL script using its existing build artifacts")
public class ExecuteCmd extends AbstractCmd {

  @Override
  protected void runInternal(ErrorCollector errors) throws Exception {
    // Start services before executing
    getOsProcessManager().startDependentServices();

    var env = GlobalEnvironmentStore.getAll();
    var targetDir = getTargetDir();
    var planDir = targetDir.resolve(SqrlConstants.PLAN_DIR);
    var sqrlConfig = ConfigLoaderUtils.loadResolvedConfig(errors, getBuildDir());
    var flinkConfig = ConfigLoaderUtils.loadFlinkConfig(planDir);

    var sqrlRun = new DatasqrlRun(planDir, sqrlConfig, flinkConfig, env, false);
    sqrlRun.run(true, true);
  }
}
