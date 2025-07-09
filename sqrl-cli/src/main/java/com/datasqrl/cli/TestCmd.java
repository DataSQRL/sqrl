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
import java.util.List;
import picocli.CommandLine;

@CommandLine.Command(name = "test", description = "Compiles, then tests a SQRL script")
public class TestCmd extends AbstractCompileCmd {

  @Override
  protected void execute(ErrorCollector errors) throws Exception {
    // Skip execution in case we call the CMD from a test class, as it will be executed manually.
    if (cli.internalTestExec) {
      return;
    }

    // Test
    var targetDir = getTargetDir();
    var planDir = targetDir.resolve(SqrlConstants.PLAN_DIR);
    var sqrlConfig = ConfigLoaderUtils.loadResolvedConfig(errors, getBuildDir());
    var flinkConfig = ConfigLoaderUtils.loadFlinkConfig(planDir);

    var sqrlTest = new DatasqrlTest(cli.rootDir, planDir, sqrlConfig, flinkConfig, System.getenv());
    exitCode.set(sqrlTest.run());
  }

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.TEST;
  }

  @Override
  protected List<String> getEngines() {
    return List.of(
        EngineIds.TEST,
        EngineIds.DATABASE,
        EngineIds.LOG,
        EngineIds.ICEBERG,
        EngineIds.DUCKDB,
        EngineIds.SERVER,
        EngineIds.STREAMS);
  }
}
