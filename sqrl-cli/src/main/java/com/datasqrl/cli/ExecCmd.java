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
import java.io.IOException;
import java.nio.file.Path;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import picocli.CommandLine.Command;

@Command(
    name = "exec",
    description = "Executes an already compiled SQRL script using its existing build artifacts.",
    mixinStandardHelpOptions = true,
    versionProvider = CliVersionProvider.class)
public class ExecCmd extends BaseOsProcessManagerCmd {

  @Override
  protected void runInternal(ErrorCollector errors) throws Exception {
    var targetDir = getTargetFolder();
    var planDir = targetDir.resolve(SqrlConstants.PLAN_DIR);

    // Start services before executing
    getOsProcessManager().startDependentServices(planDir);

    var env = GlobalEnvironmentStore.getAll();
    var sqrlConfig = ConfigLoaderUtils.loadResolvedConfig(errors, getBuildDir());
    var flinkConfig = getFlinkConfig(planDir);

    var sqrlRun = DatasqrlRun.blocking(planDir, sqrlConfig, flinkConfig, env);
    sqrlRun.run();
  }

  /**
   * Loads the Flink configuration from the plan directory. If no deployment target is configured,
   * defaults to local attached execution.
   */
  private Configuration getFlinkConfig(Path planDir) throws IOException {
    var flinkConfig = ConfigLoaderUtils.loadFlinkConfig(planDir);
    if (!flinkConfig.contains(DeploymentOptions.TARGET)) {
      flinkConfig.set(DeploymentOptions.TARGET, "local");
      flinkConfig.set(DeploymentOptions.ATTACHED, true);
    }

    return flinkConfig;
  }
}
