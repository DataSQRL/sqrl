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
package com.datasqrl.cmd;

import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import com.datasqrl.plan.validate.ExecutionGoal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(name = "test", description = "Tests a SQRL script")
public class TestCommand extends AbstractCompilerCommand {
  @CommandLine.Option(
      names = {"-s", "--snapshots"},
      description = "Path to snapshots")
  protected Path snapshotPath = null;

  @CommandLine.Option(
      names = {"--tests"},
      description = "Path to test queries")
  protected Path tests = null;

  @SneakyThrows
  @Override
  public void execute(ErrorCollector errors) {
    super.execute(
        errors,
        snapshotPath == null
            ? root.rootDir.resolve("snapshots")
            : snapshotPath.isAbsolute() ? snapshotPath : root.rootDir.resolve(snapshotPath),
        tests == null
            ? (Files.isDirectory(root.rootDir.resolve("tests"))
                ? Optional.of(root.rootDir.resolve("tests"))
                : Optional.empty())
            : Optional.of((tests.isAbsolute() ? tests : root.rootDir.resolve(tests))),
        ExecutionGoal.TEST);
  }

  @SneakyThrows
  @Override
  protected void postprocess(
      PackageJson sqrlConfig,
      Packager packager,
      Path targetDir,
      PhysicalPlan plan,
      TestPlan testPlan,
      Path snapshotPath,
      ErrorCollector errors) {
    super.postprocess(sqrlConfig, packager, targetDir, plan, testPlan, snapshotPath, errors);
  }

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.TEST;
  }
}
