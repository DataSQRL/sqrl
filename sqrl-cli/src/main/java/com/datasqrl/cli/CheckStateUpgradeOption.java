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

import com.datasqrl.cli.FlinkStateUpgradeCheck.FlinkJob;
import com.datasqrl.cli.StateUpgradeBreak.OldState;
import com.datasqrl.config.ProjectPaths;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigLoaderUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.flink.configuration.PipelineOptions;
import picocli.CommandLine.Option;

/**
 * The {@code --check-state-upgrade-from} option of the {@code compile} command. It fails the
 * compile when Flink cannot restore the state of the old build from the new compiled plan.
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
              + " folder. The path must be relative to the project root and stay inside the"
              + " workspace.\n")
  Optional<Path> oldBuildFolder = Optional.empty();

  /**
   * The plan folder that {@link #checkOldBuildFolder} finds before the compile. {@link
   * #checkUpgrade} reads this folder and does not search again, because the compile can create a
   * folder that the search finds first.
   */
  private Optional<Path> oldPlanDir = Optional.empty();

  /**
   * Fails when the path of the option breaks the rule of {@link ProjectPaths}, when the folder does
   * not exist or has no plan folder, or when its plan folder is inside the build folder or the
   * target folder of this compile. Call it before the compile, because the compile empties both
   * folders.
   */
  void checkOldBuildFolder(
      ProjectPaths projectPaths, Path buildDir, Path targetDir, ErrorCollector errors) {
    oldBuildFolder.ifPresent(
        givenOldBuildFolder -> {
          var oldBuildDir =
              projectPaths
                  .resolve("--check-state-upgrade-from", givenOldBuildFolder.toString(), errors)
                  .toAbsolutePath()
                  .normalize();
          errors.checkFatal(
              Files.exists(oldBuildDir),
              "The --check-state-upgrade-from folder %s does not exist",
              oldBuildDir);

          var foundOldPlanDir = findPlanDir(oldBuildDir, errors);
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
    var newPlanDir = targetDir.resolve(SqrlConstants.PLAN_DIR);
    compareFlinkJobs(
        checkedOldPlanDir,
        readFlinkJob(checkedOldPlanDir, errors),
        readFlinkJob(newPlanDir, errors),
        errors);
  }

  /**
   * Reads the Flink job of a plan folder: the compiled plan and its {@code
   * pipeline.max-parallelism}. Returns empty when the folder has no compiled Flink plan. A Flink
   * file that exists but cannot be read is a fatal error.
   */
  static Optional<FlinkJob> readFlinkJob(Path planDir, ErrorCollector errors) {
    return ConfigLoaderUtils.loadFlinkCompiledPlan(planDir, errors)
        .map(compiledPlan -> new FlinkJob(compiledPlan, readMaxParallelism(planDir, errors)));
  }

  private static Optional<Integer> readMaxParallelism(Path planDir, ErrorCollector errors) {
    var flinkConfigFile = planDir.resolve(SqrlConstants.FLINK_CONFIG_FILE);
    if (!Files.isRegularFile(flinkConfigFile)) {
      return Optional.empty();
    }

    try {
      return ConfigLoaderUtils.loadFlinkConfig(planDir)
          .getOptional(PipelineOptions.MAX_PARALLELISM);
    } catch (IOException | RuntimeException exception) {
      // Flink reports an invalid YAML file with a plain RuntimeException, and an invalid value
      // with an IllegalArgumentException.
      throw errors.exception("Cannot read %s: %s", flinkConfigFile, exception.getMessage());
    }
  }

  /**
   * Reports if Flink can restore the state of the old build from the new build. A break is a fatal
   * error. A Flink version change is a notice, and a risk that the check cannot decide is a
   * warning. Each message names the plan folder of the old build.
   */
  static void compareFlinkJobs(
      Path oldPlanDir,
      Optional<FlinkJob> oldFlinkJob,
      Optional<FlinkJob> newFlinkJob,
      ErrorCollector errors) {
    if (oldFlinkJob.isEmpty()) {
      errors.notice(
          "The old build in %s has no compiled Flink plan, so the check cannot compare the Flink"
              + " state.",
          oldPlanDir);
      return;
    }
    if (newFlinkJob.isEmpty()) {
      errors.notice(
          "The new compile has no compiled Flink plan, so the check cannot compare the Flink"
              + " state.");
      return;
    }

    var stateUpgradeBreaks =
        FlinkStateUpgradeCheck.check(oldFlinkJob.get(), newFlinkJob.get(), errors);
    if (stateUpgradeBreaks.isEmpty()) {
      errors.notice(
          "The new Flink plan passes the state upgrade check against the old build in %s. The"
              + " check does not prove that the results match a fresh run, for example after a"
              + " new table.",
          oldPlanDir);
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
        "The new Flink plan cannot restore the state of the old build in %s:\n- %s",
        oldPlanDir, String.join("\n- ", breakMessages));
  }

  /**
   * Returns the line of a break with the mark of its old state. A break of an operator without
   * state has no mark.
   */
  private static String breakLine(StateUpgradeBreak stateUpgradeBreak) {
    return switch (stateUpgradeBreak.oldState()) {
      case UNCLAIMED -> stateUpgradeBreak.message() + " " + UNCLAIMED_STATE_MARKER;
      case INCOMPATIBLE -> stateUpgradeBreak.message() + " " + INCOMPATIBLE_STATE_MARKER;
      case NONE -> stateUpgradeBreak.message();
    };
  }

  private static boolean hasOldState(
      List<StateUpgradeBreak> stateUpgradeBreaks, OldState oldState) {
    return stateUpgradeBreaks.stream()
        .anyMatch(stateUpgradeBreak -> stateUpgradeBreak.oldState() == oldState);
  }

  /**
   * Fails when the old plan folder is inside a folder that the compile empties. The message names
   * the option that sets the compile folder, so that the user knows which option to change.
   */
  private static void checkPlanDirOutside(
      Path oldPlanDir,
      String compileFolderName,
      String compileFolderOption,
      Path compileDir,
      ErrorCollector errors) {
    var absoluteCompileDir = compileDir.toAbsolutePath().normalize();
    errors.checkFatal(
        !isInsideDir(oldPlanDir, absoluteCompileDir),
        "The old plan folder %s of --check-state-upgrade-from is inside the %s %s of this compile."
            + " The compile empties its build folder and its target folder, so it would delete the"
            + " old build or the symbolic link to it. Give this compile a %s value that is outside"
            + " the old build.",
        oldPlanDir,
        compileFolderName,
        absoluteCompileDir,
        compileFolderOption);
  }

  /**
   * Returns true when the path is inside the folder before or after the symbolic links resolve. A
   * link to the folder hides the path before the links resolve. A link inside the folder hides the
   * path after the links resolve, and the compile deletes that link when it empties the folder.
   *
   * <p>{@link ProjectPaths#isInsideWorkspace} needs both forms inside, because it decides if a path
   * is safe to read. This check needs one form inside, because it decides if the compile can delete
   * the old build.
   */
  private static boolean isInsideDir(Path absolutePath, Path absoluteDir) {
    return absolutePath.startsWith(absoluteDir)
        || ProjectPaths.realPath(absolutePath).startsWith(ProjectPaths.realPath(absoluteDir));
  }

  /** Finds the plan folder in a build folder, a target folder, or a plan folder. */
  private static Path findPlanDir(Path oldBuildDir, ErrorCollector errors) {
    var buildPlanDir =
        oldBuildDir.resolve(SqrlConstants.DEPLOY_DIR_NAME).resolve(SqrlConstants.PLAN_DIR);
    var targetPlanDir = oldBuildDir.resolve(SqrlConstants.PLAN_DIR);

    return Stream.of(buildPlanDir, targetPlanDir)
        .filter(Files::isDirectory)
        .findFirst()
        .or(() -> Optional.of(oldBuildDir).filter(CheckStateUpgradeOption::hasFlinkFiles))
        .orElseThrow(
            () ->
                errors.exception(
                    "Cannot find the plan folder in %s. Give a build folder (with deploy/plan), a"
                        + " target folder (with plan), or a plan folder (with"
                        + " flink-compiled-plan.json or flink-config.yaml).",
                    oldBuildDir));
  }

  /** Returns true when the folder holds a Flink file, so the folder is itself a plan folder. */
  static boolean hasFlinkFiles(Path folder) {
    return Files.isRegularFile(folder.resolve(SqrlConstants.FLINK_COMPILED_PLAN_FILE))
        || Files.isRegularFile(folder.resolve(SqrlConstants.FLINK_CONFIG_FILE));
  }
}
