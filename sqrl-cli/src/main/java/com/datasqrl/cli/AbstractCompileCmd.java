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

import static com.datasqrl.config.SqrlConstants.PACKAGE_JSON;

import com.datasqrl.compile.CompilationProcess;
import com.datasqrl.compile.DirectoryManager;
import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.ExecutionEnginesHolder;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.packager.Packager;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.FlinkCompileException;
import com.datasqrl.util.SqrlInjector;
import com.google.inject.Guice;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

public abstract class AbstractCompileCmd extends AbstractCmd {

  public abstract ExecutionGoal getGoal();

  protected void compile(ErrorCollector errors) {
    var formatter = getOutputFormatter();

    if (getGoal() == ExecutionGoal.COMPILE) {
      formatter.header("DataSQRL Compilation");
    }

    if (getGoal() == ExecutionGoal.COMPILE) {
      formatter.phaseStart("Initializing build environment");
    }
    var sqrlConfig = initPackageJson(errors, cli.rootDir, packageFiles);

    DirectoryManager.prepareTargetDirectory(getTargetDir());
    errors.checkFatal(
        Files.isDirectory(cli.rootDir), "Not a valid root directory: %s", cli.rootDir);

    var injector =
        Guice.createInjector(
            new SqrlInjector(
                errors, cli.rootDir, getTargetDir(), sqrlConfig, getGoal(), cli.internalTestExec));

    var engineHolder = injector.getInstance(ExecutionEnginesHolder.class);
    engineHolder.initEnabledEngines();

    if (getGoal() == ExecutionGoal.COMPILE) {
      formatter.phaseStart("Processing dependencies");
    }
    var packager = injector.getInstance(Packager.class);
    packager.preprocess(errors.withLocation(ErrorPrefix.CONFIG.resolve(PACKAGE_JSON)));
    if (errors.hasErrors()) {
      return;
    }

    if (getGoal() == ExecutionGoal.COMPILE) {
      formatter.phaseStart("Compiling SQRL script");
    }
    var compilationProcess = injector.getInstance(CompilationProcess.class);
    var testDir = sqrlConfig.getTestConfig().getTestDir(cli.rootDir);
    testDir.ifPresent(this::validateTestPath);

    Pair<PhysicalPlan, ? extends TestPlan> plan;
    try {
      plan = compilationProcess.executeCompilation(testDir);

    } catch (FlinkCompileException e) {
      packager.postprocessFlinkCompileError(e);
      throw e;
    }

    if (errors.hasErrors()) {
      return;
    }

    if (getGoal() == ExecutionGoal.COMPILE) {
      formatter.phaseStart("Generating deployment artifacts");
    }
    postprocess(packager, getTargetDir(), plan.getLeft(), plan.getRight());

    if (getGoal() == ExecutionGoal.COMPILE) {
      printCompilationResults(formatter);
    }
  }

  private void printCompilationResults(com.datasqrl.cli.output.OutputFormatter formatter) {
    formatter.newline();
    formatter.sectionHeader("Compilation Results");
    formatter.info("Deployment artifacts: " + getTargetDir());
    formatter.info("Pipeline DAG:         " + getBuildDir().resolve("pipeline_explain.txt"));
    formatter.info("Visual DAG:           " + getBuildDir().resolve("pipeline_visual.html"));
    formatter.newline();

    formatter.buildStatus(true, getElapsedTime(), LocalDateTime.now());
  }

  protected void execute(ErrorCollector errors) throws Exception {
    // Do nothing by default
  }

  @Override
  protected void runInternal(ErrorCollector errors) throws Exception {
    compile(errors);

    if (!errors.hasErrors()) {
      execute(errors);
    }
  }

  protected void postprocess(
      Packager packager, Path targetDir, PhysicalPlan plan, TestPlan testPlan) {
    packager.postprocess(targetDir, plan, testPlan);
  }

  /**
   * Initializes the {@link PackageJson} configuration by merging all provided package configuration
   * files. If no files are provided, attempts to load the default package configuration file.
   *
   * @param errors error collector
   * @param rootDir the root project directory
   * @param packageFiles the list of given package configuration file paths
   * @return the merged {@link PackageJson} configuration
   */
  private PackageJson initPackageJson(
      ErrorCollector errors, Path rootDir, List<Path> packageFiles) {
    var locErrors = errors.withLocation(ErrorPrefix.CONFIG).resolve("package");

    var finalPackageFiles = new ArrayList<Path>();
    if (packageFiles.isEmpty()) {
      // Try to find default package
      var defaultPkg = rootDir.resolve(SqrlConstants.DEFAULT_PACKAGE);
      if (Files.isRegularFile(defaultPkg)) {
        finalPackageFiles.add(defaultPkg);
      }

    } else {
      packageFiles.stream().map(rootDir::resolve).forEach(finalPackageFiles::add);
    }

    if (finalPackageFiles.isEmpty()) {
      locErrors.fatal("No package file were given, and default %s not found", packageFiles);
    }

    return loadRunDefaults()
        ? ConfigLoaderUtils.loadUnresolvedRunConfig(locErrors, finalPackageFiles)
        : ConfigLoaderUtils.loadUnresolvedConfig(locErrors, finalPackageFiles);
  }

  private void validateTestPath(Path path) {
    if (!Files.isDirectory(path)) {
      throw new RuntimeException("Could not find test path: " + path.toAbsolutePath());
    }
  }

  private boolean loadRunDefaults() {
    return getGoal() == ExecutionGoal.RUN || getGoal() == ExecutionGoal.TEST;
  }
}
