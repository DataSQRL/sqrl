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

import com.datasqrl.cli.util.FlinkConfigLoader;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.validate.ExecutionGoal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import picocli.CommandLine;

@CommandLine.Command(name = "test", description = "Tests a SQRL script")
public class TestCommand extends AbstractCompileCommand {
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
            ? root.rootDir.resolve("snapshots")
            : snapshotPath.isAbsolute() ? snapshotPath : root.rootDir.resolve(snapshotPath),
        tests == null
            ? (Files.isDirectory(root.rootDir.resolve("tests"))
                ? Optional.of(root.rootDir.resolve("tests"))
                : Optional.empty())
            : Optional.of((tests.isAbsolute() ? tests : root.rootDir.resolve(tests))));

    // Skip test part in case we call the CMD from a test class, as it will be executed manually.
    if (root.internalTestExec) {
      return;
    }

    // Test
    var planDir = getTargetDir().resolve(SqrlConstants.PLAN_DIR);
    var sqrlConfig = readSqrlConfig();
    var flinkConfig = FlinkConfigLoader.fromYamlFile(planDir);
    var sqrlTest = new DatasqrlTest(planDir, sqrlConfig, flinkConfig, System.getenv());
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
