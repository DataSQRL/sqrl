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
import com.datasqrl.deployment.model.StateUpgradeBreak.OldState;
import com.datasqrl.deployment.model.StateUpgradeNote;
import com.datasqrl.error.ErrorCollector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

  /** Marks each break line whose old state no operator of the new plan claims. */
  private static final String UNCLAIMED_STATE_MARKER = "[unclaimed state]";

  /** Marks each break line whose old state an operator claims but cannot read. */
  private static final String INCOMPATIBLE_STATE_MARKER = "[incompatible state]";

  private static final String UNCLAIMED_STATE_NOTE =
      ("The lines marked %s leave old state that no operator of the new plan claims. Flink restores"
              + " without that state only when execution.state-recovery.ignore-unclaimed-state is"
              + " true, and it then drops that state.")
          .formatted(UNCLAIMED_STATE_MARKER);

  private static final String INCOMPATIBLE_STATE_NOTE =
      ("The lines marked %s belong to an operator that claims the old state but cannot read it."
              + " The option execution.state-recovery.ignore-unclaimed-state does not help for them."
              + " Change the script back, or start the new job without the savepoint.")
          .formatted(INCOMPATIBLE_STATE_MARKER);

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
   * The plan folder that {@link #checkOldBuildFolder} finds before the compile. {@link
   * #checkUpgrade} reads this folder and does not search again, because the compile can create a
   * folder that the search finds first.
   */
  private Optional<Path> oldPlanDir = Optional.empty();

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

          var foundOldPlanDir = findPlanDir(oldBuildFolderPath, errors);
          checkPlanDirOutside(foundOldPlanDir, "build folder", "-b", buildDir, errors);
          checkPlanDirOutside(foundOldPlanDir, "target folder", "-t", targetDir, errors);
          oldPlanDir = Optional.of(foundOldPlanDir);
        });
  }

  /**
   * Compares the old plan folder that {@link #checkOldBuildFolder} found with the new build in the
   * target folder. Call it after a compile without errors.
   */
  void checkUpgrade(Path targetDir, ErrorCollector errors) {
    if (oldBuildFolder.isEmpty()) {
      return;
    }
    var checkedOldPlanDir =
        oldPlanDir.orElseThrow(
            () -> new IllegalStateException("Call checkOldBuildFolder before checkUpgrade"));
    compareBuildPlans(
        FlinkBuildPlan.read(checkedOldPlanDir, errors),
        FlinkBuildPlan.read(targetDir.resolve(SqrlConstants.PLAN_DIR), errors),
        errors);
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
    stateUpgradeResult.notes().forEach(stateUpgradeNote -> report(stateUpgradeNote, errors));

    var stateUpgradeBreaks = stateUpgradeResult.breaks();
    if (stateUpgradeBreaks.isEmpty()) {
      errors.notice(
          "%s",
          "The new Flink plan can restore the state of the old build in %s."
              .formatted(oldBuildPlan.planDir()));
      return;
    }

    var breakMessages =
        new ArrayList<>(
            stateUpgradeBreaks.stream().map(CheckStateUpgradeOption::breakLine).toList());
    if (hasOldState(stateUpgradeBreaks, OldState.UNCLAIMED)) {
      breakMessages.add(UNCLAIMED_STATE_NOTE);
    }
    if (hasOldState(stateUpgradeBreaks, OldState.INCOMPATIBLE)) {
      breakMessages.add(INCOMPATIBLE_STATE_NOTE);
    }
    errors.fatal(
        "%s",
        "The new Flink plan cannot restore the state of the old build in %s:\n- %s"
            .formatted(oldBuildPlan.planDir(), String.join("\n- ", breakMessages)));
  }

  /**
   * Returns the line of a break with the mark of its old state. A break of an operator without
   * state has no mark.
   */
  private static String breakLine(StateUpgradeBreak stateUpgradeBreak) {
    return switch (stateUpgradeBreak.oldState()) {
      case UNCLAIMED -> stateUpgradeBreak.toMessage() + " " + UNCLAIMED_STATE_MARKER;
      case INCOMPATIBLE -> stateUpgradeBreak.toMessage() + " " + INCOMPATIBLE_STATE_MARKER;
      case NONE -> stateUpgradeBreak.toMessage();
    };
  }

  private static boolean hasOldState(
      List<StateUpgradeBreak> stateUpgradeBreaks, OldState oldState) {
    return stateUpgradeBreaks.stream()
        .anyMatch(stateUpgradeBreak -> stateUpgradeBreak.oldState() == oldState);
  }

  /** Reports the note at the level that the check sets for it. */
  private static void report(StateUpgradeNote stateUpgradeNote, ErrorCollector errors) {
    switch (stateUpgradeNote.level()) {
      case NOTICE -> errors.notice("%s", stateUpgradeNote.toMessage());
      case WARN -> errors.warn("%s", stateUpgradeNote.toMessage());
    }
  }

  private static Path resolveOldBuildFolder(Path projectRoot, Path givenOldBuildFolder) {
    return projectRoot.resolve(givenOldBuildFolder).toAbsolutePath().normalize();
  }

  /**
   * Fails when the old plan folder is inside a folder that the compile empties. The message names
   * the option that sets the compile folder, so that the user knows which option to change.
   */
  private static void checkPlanDirOutside(
      Path oldPlanDir,
      String compileFolderName,
      String compileFolderOption,
      Path compileFolder,
      ErrorCollector errors) {
    var absoluteCompileFolder = compileFolder.toAbsolutePath().normalize();
    errors.checkFatal(
        !isInsideFolder(oldPlanDir, absoluteCompileFolder),
        "%s",
        ("The old plan folder %s of --check-state-upgrade-from is inside the %s %s of this compile."
                + " The compile empties its build folder and its target folder, so it would delete"
                + " the old build or the symbolic link to it. Give this compile a %s value that is"
                + " outside the old build.")
            .formatted(oldPlanDir, compileFolderName, absoluteCompileFolder, compileFolderOption));
  }

  /**
   * Returns true when the path is inside the folder before or after the symbolic links resolve. A
   * link to the folder hides the path before the links resolve. A link inside the folder hides the
   * path after the links resolve, and the compile deletes that link when it empties the folder.
   */
  private static boolean isInsideFolder(Path absolutePath, Path absoluteFolder) {
    return absolutePath.startsWith(absoluteFolder)
        || realPath(absolutePath).startsWith(realPath(absoluteFolder));
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
