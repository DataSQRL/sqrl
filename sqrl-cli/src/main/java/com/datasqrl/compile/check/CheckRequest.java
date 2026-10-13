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
package com.datasqrl.compile.check;

import com.datasqrl.config.SqrlConstants;
import com.datasqrl.plan.validate.ExecutionGoal;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * What {@link ScriptChecker} checks.
 *
 * @param projectRoot the directory of the project
 * @param packageFiles the package configuration files of the project
 * @param workDir a private directory for the build output, outside of the project
 * @param overlays content of source files that replaces their content on disk, by absolute path
 * @param fullCompile whether to run the full compilation (DAG planning, API generation) instead of
 *     only planning the scripts
 * @param goal the execution goal, {@link ExecutionGoal#TEST} also checks test tables
 * @param keepCatalog whether to keep the planned catalog open for completion, see {@link
 *     CheckResult#catalog()}; not supported for a full compilation
 */
public record CheckRequest(
    Path projectRoot,
    List<Path> packageFiles,
    Path workDir,
    Map<Path, String> overlays,
    boolean fullCompile,
    ExecutionGoal goal,
    boolean keepCatalog) {

  public CheckRequest {
    projectRoot = projectRoot.toAbsolutePath().normalize();
    packageFiles = packageFiles.stream().map(p -> p.toAbsolutePath().normalize()).toList();
    workDir = workDir.toAbsolutePath().normalize();
    overlays = Map.copyOf(overlays);
  }

  /** Checks the project's default package configuration, including test tables. */
  public static CheckRequest of(Path projectRoot, Path workDir) {
    return new CheckRequest(
        projectRoot,
        List.of(projectRoot.resolve(SqrlConstants.DEFAULT_PACKAGE)),
        workDir,
        Map.of(),
        false,
        ExecutionGoal.TEST,
        false);
  }

  public CheckRequest withPackageFiles(List<Path> files) {
    return new CheckRequest(projectRoot, files, workDir, overlays, fullCompile, goal, keepCatalog);
  }

  public CheckRequest withOverlays(Map<Path, String> newOverlays) {
    return new CheckRequest(
        projectRoot, packageFiles, workDir, newOverlays, fullCompile, goal, keepCatalog);
  }

  public CheckRequest withFullCompile(boolean full) {
    return new CheckRequest(projectRoot, packageFiles, workDir, overlays, full, goal, keepCatalog);
  }

  public CheckRequest withGoal(ExecutionGoal newGoal) {
    return new CheckRequest(
        projectRoot, packageFiles, workDir, overlays, fullCompile, newGoal, keepCatalog);
  }

  public CheckRequest withKeepCatalog(boolean keep) {
    return new CheckRequest(projectRoot, packageFiles, workDir, overlays, fullCompile, goal, keep);
  }
}
