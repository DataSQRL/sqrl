/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
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
  @CommandLine.Option(names = {"-s", "--snapshot"},
      description = "Path to snapshots")
  protected Path snapshotPath = null;
  @CommandLine.Option(names = {"--tests"},
      description = "Path to test queries")
  protected Path tests = null;

  @SneakyThrows
  @Override
  public void execute(ErrorCollector errors) {
    super.execute(errors, snapshotPath == null ?
        root.rootDir.resolve("snapshots") :
            snapshotPath.isAbsolute() ? snapshotPath : root.rootDir.resolve(snapshotPath),
        tests == null ? (Files.isDirectory(root.rootDir.resolve("tests")) ?
            Optional.of(root.rootDir.resolve("tests")) : Optional.empty()) :
            Optional.of((tests.isAbsolute() ? tests : root.rootDir.resolve(tests))),
        ExecutionGoal.TEST);
  }

  @SneakyThrows
  @Override
  protected void postprocess(PackageJson sqrlConfig, Packager packager, Path targetDir,
      PhysicalPlan plan, TestPlan testPlan, Path snapshotPath, ErrorCollector errors) {
    super.postprocess(sqrlConfig, packager, targetDir, plan, testPlan, snapshotPath, errors);
  }

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.TEST;
  }
}
