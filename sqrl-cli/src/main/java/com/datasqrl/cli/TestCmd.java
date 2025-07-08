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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import picocli.CommandLine;

@CommandLine.Command(name = "test", description = "Compiles, then tests a SQRL script")
public class TestCmd extends AbstractCompileCmd {

  @CommandLine.Option(
      names = {"-s", "--snapshots"},
      description = "Path to snapshots")
  protected Path snapshotPath = null;

  @CommandLine.Option(
      names = {"--tests"},
      description = "Path to test queries")
  protected Path tests = null;

  @Override
  protected void execute(ErrorCollector errors) throws Exception {
    // Compile
    super.execute(
        errors,
        snapshotPath == null
            ? cli.rootDir.resolve("snapshots")
            : snapshotPath.isAbsolute() ? snapshotPath : cli.rootDir.resolve(snapshotPath),
        tests == null
            ? (Files.isDirectory(cli.rootDir.resolve("tests"))
                ? Optional.of(cli.rootDir.resolve("tests"))
                : Optional.empty())
            : Optional.of((tests.isAbsolute() ? tests : cli.rootDir.resolve(tests))));

    // Skip test part in case we call the CMD from a test class, as it will be executed manually.
    if (cli.internalTestExec) {
      return;
    }

    // Test
    var targetDir = getTargetDir();
    var planDir = targetDir.resolve(SqrlConstants.PLAN_DIR);
    var sqrlConfig = ConfigLoaderUtils.loadResolvedConfig(errors, cli.rootDir);
    var flinkConfig = ConfigLoaderUtils.loadFlinkConfig(planDir);

    var sqrlTest =
        new DatasqrlTest(planDir, sqrlConfig.getCompilerConfig(), flinkConfig, System.getenv());
    sqrlTest.run();
  }

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.TEST;
  }

  @Override
  protected List<String> getEngines() {
    return List.of(
        EngineKeys.TEST,
        EngineKeys.DATABASE,
        EngineKeys.LOG,
        EngineKeys.ICEBERG,
        EngineKeys.DUCKDB,
        EngineKeys.SERVER,
        EngineKeys.STREAMS);
  }
}
