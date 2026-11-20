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
import com.datasqrl.deployment.model.FlinkStateUpgradeCheck;
import com.datasqrl.deployment.model.StateUpgradeBreak;
import com.datasqrl.deployment.model.StateUpgradeBreak.RemovedOperator;
import com.datasqrl.error.ErrorCollector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import picocli.CommandLine.Option;

/**
 * The {@code --check-state-upgrade-from} option of the {@code compile} command. It fails the
 * compile when Flink cannot restore the state of the old build from the new compiled plan.
 *
 * <p>Each message goes to the {@link ErrorCollector} as the argument of {@code "%s"}, because the
 * collector formats its message text, and an operator description can contain a {@code %} sign.
 */
public class CheckStateUpgradeOption {

  @Option(
      names = "--check-state-upgrade-from",
      paramLabel = "<build-folder>",
      description =
          "Build folder of the deployed (old) version, for example \"build/shop-replay\"."
              + " After the compile, the compile fails when Flink cannot restore the state of"
              + " that build from the new compiled plan. Also accepts a target folder or a plan"
              + " folder. A relative path resolves against the project root.\n")
  Optional<Path> oldBuildFolder = Optional.empty();

  /**
   * Fails when the folder of the option does not exist, has no plan folder, or has its plan folder
   * inside the build folder or the target folder of this compile. Call it before the compile,
   * because the compile empties both folders.
   */
  void checkOldBuildFolder(Path projectRoot, Path buildDir, Path targetDir, ErrorCollector errors) {
    oldBuildFolder.ifPresent(
        givenOldBuildFolder -> {
          var oldBuildFolderPath = resolveOldBuildFolder(projectRoot, givenOldBuildFolder);
          errors.checkFatal(
              Files.exists(oldBuildFolderPath),
              "%s",
              "The --check-state-upgrade-from folder %s does not exist"
                  .formatted(oldBuildFolderPath));

          var oldPlanDir = findPlanDir(oldBuildFolderPath, errors);
          checkPlanDirOutside(oldPlanDir, "build folder", buildDir, errors);
          checkPlanDirOutside(oldPlanDir, "target folder", targetDir, errors);
        });
  }

  /**
   * Compares the old build of the option with the new build in the target folder. Call it after a
   * compile without errors.
   */
  void checkUpgrade(Path projectRoot, Path targetDir, ErrorCollector errors) {
    oldBuildFolder.ifPresent(
        givenOldBuildFolder -> {
          var oldPlanDir =
              findPlanDir(resolveOldBuildFolder(projectRoot, givenOldBuildFolder), errors);
          compareBuildPlans(
              FlinkBuildPlan.read(oldPlanDir, errors),
              FlinkBuildPlan.read(targetDir.resolve(SqrlConstants.PLAN_DIR), errors),
              errors);
        });
  }

  /**
   * Reports if Flink can restore the state of the old build from the new build. A break is a fatal
   * error. A Flink version change is a notice, and a risk that the check cannot decide is a
   * warning.
   */
  static void compareBuildPlans(
      FlinkBuildPlan oldBuildPlan, FlinkBuildPlan newBuildPlan, ErrorCollector errors) {
    if (oldBuildPlan.flinkJobPlan().isEmpty()) {
      errors.notice(
          "%s",
          "The old build in %s has no Flink plan, so there is no Flink state to check."
              .formatted(oldBuildPlan.planDir()));
      return;
    }
    if (newBuildPlan.flinkJobPlan().isEmpty()) {
      errors.notice("%s", "The new compile has no Flink plan, so Flink restores no state.");
      return;
    }

    var stateUpgradeResult =
        FlinkStateUpgradeCheck.check(
            oldBuildPlan.flinkJobPlan().get(), newBuildPlan.flinkJobPlan().get());
    stateUpgradeResult
        .flinkVersionChange()
        .ifPresent(
            flinkVersionChange ->
                errors.notice("%s", "The Flink version changes: " + flinkVersionChange));
    stateUpgradeResult.warnings().forEach(warning -> errors.warn("%s", warning));

    var stateUpgradeBreaks = stateUpgradeResult.breaks();
    if (stateUpgradeBreaks.isEmpty()) {
      errors.notice(
          "%s",
          "The new Flink plan can restore the state of the old build in %s."
              .formatted(oldBuildPlan.planDir()));
      return;
    }

    var breakMessages =
        new ArrayList<>(stateUpgradeBreaks.stream().map(StateUpgradeBreak::toMessage).toList());
    if (stateUpgradeBreaks.stream().anyMatch(RemovedOperator.class::isInstance)) {
      breakMessages.add(
          "Flink restores a plan without these operators only when"
              + " execution.state-recovery.ignore-unclaimed-state is true, and it then drops their"
              + " state.");
    }
    errors.fatal(
        "%s",
        "The new Flink plan cannot restore the state of the old build in %s:\n- %s"
            .formatted(oldBuildPlan.planDir(), String.join("\n- ", breakMessages)));
  }

  private static Path resolveOldBuildFolder(Path projectRoot, Path givenOldBuildFolder) {
    return projectRoot.resolve(givenOldBuildFolder).toAbsolutePath().normalize();
  }

  /**
   * Fails when the old plan folder is inside a folder that the compile empties. It compares the
   * real paths, so that a symbolic link cannot hide this.
   */
  private static void checkPlanDirOutside(
      Path oldPlanDir, String compileFolderName, Path compileFolder, ErrorCollector errors) {
    var absoluteCompileFolder = compileFolder.toAbsolutePath().normalize();
    errors.checkFatal(
        !realPath(oldPlanDir).startsWith(realPath(absoluteCompileFolder)),
        "%s",
        ("The old plan folder %s of --check-state-upgrade-from is inside the %s %s of this compile."
                + " The compile empties its build folder and its target folder, so it would delete"
                + " the old build. Give this compile a -b value that is different from the old"
                + " build.")
            .formatted(oldPlanDir, compileFolderName, absoluteCompileFolder));
  }

  /**
   * Returns the path with all symbolic links resolved. The target folder can be missing before its
   * first compile, so the part of the path that does not exist stays as it is.
   */
  @SneakyThrows
  private static Path realPath(Path absolutePath) {
    var existingPath = absolutePath;
    while (!Files.exists(existingPath)) {
      existingPath = existingPath.getParent();
    }
    return existingPath.toRealPath().resolve(existingPath.relativize(absolutePath));
  }

  /** Finds the plan folder in a build folder, a target folder, or a plan folder. */
  private static Path findPlanDir(Path oldBuildFolderPath, ErrorCollector errors) {
    var buildPlanDir =
        oldBuildFolderPath.resolve(SqrlConstants.DEPLOY_DIR_NAME).resolve(SqrlConstants.PLAN_DIR);
    var targetPlanDir = oldBuildFolderPath.resolve(SqrlConstants.PLAN_DIR);

    return Stream.of(buildPlanDir, targetPlanDir)
        .filter(Files::isDirectory)
        .findFirst()
        .or(() -> Optional.of(oldBuildFolderPath).filter(FlinkBuildPlan::hasFlinkFiles))
        .orElseThrow(
            () ->
                errors.exception(
                    "%s",
                    ("Cannot find the plan folder in %s. Give a build folder (with deploy/plan), a"
                            + " target folder (with plan), or a plan folder (with"
                            + " flink-compiled-plan.json or flink-config.yaml).")
                        .formatted(oldBuildFolderPath)));
  }
}
