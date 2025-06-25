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

import static com.datasqrl.config.ScriptConfigImpl.GRAPHQL_NORMALIZED_FILE_NAME;
import static com.datasqrl.config.SqrlConstants.PACKAGE_JSON;

import com.datasqrl.compile.CompilationProcess;
import com.datasqrl.compile.DirectoryManager;
import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.graphql.APIType;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.datasqrl.inject.SqrlInjector;
import com.datasqrl.packager.PackageBootstrap;
import com.datasqrl.packager.Packager;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.google.inject.Guice;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import picocli.CommandLine;

public abstract class AbstractCompileCommand extends AbstractCommand {

  public static final Path DEFAULT_TARGET_DIR =
      Path.of(SqrlConstants.BUILD_DIR_NAME, SqrlConstants.DEPLOY_DIR_NAME);

  @CommandLine.Parameters(
      arity = "0..2",
      description = "Main script and (optional) API specification")
  protected Path[] files = new Path[0];

  @CommandLine.Option(
      names = {"-a", "--api"},
      description = "Generates the API specification for the given type")
  protected APIType[] generateAPI = new APIType[0];

  @CommandLine.Option(
      names = {"-t", "--target"},
      description = "Target directory for deployment artifacts and plans")
  protected Path targetDir = DEFAULT_TARGET_DIR;

  public abstract ExecutionGoal getGoal();

  @Override
  protected void execute(ErrorCollector errors) throws Exception {
    execute(errors, root.rootDir.resolve("snapshots"), Optional.empty());
  }

  protected void execute(ErrorCollector errors, Path snapshotPath, Optional<Path> testsPath) {
    var packageBootstrap = new PackageBootstrap(errors);
    var sqrlConfig = packageBootstrap.bootstrap(root.rootDir, this.root.packageFiles, this.files);

    var snapshotPathConf = sqrlConfig.getCompilerConfig().getSnapshotPath();
    if (snapshotPathConf.isEmpty()) {
      sqrlConfig.getCompilerConfig().setSnapshotPath(snapshotPath.toAbsolutePath().toString());
    }

    var engines = getEngines();
    if (!engines.isEmpty()) {
      sqrlConfig.setPipeline(engines);
    }

    DirectoryManager.prepareTargetDirectory(getTargetDir());
    errors.checkFatal(
        Files.isDirectory(root.rootDir), "Not a valid root directory: %s", root.rootDir);

    var injector =
        Guice.createInjector(
            new SqrlInjector(errors, root.rootDir, getTargetDir(), sqrlConfig, getGoal()));

    var packager = injector.getInstance(Packager.class);
    packager.preprocess(errors.withLocation(ErrorPrefix.CONFIG.resolve(PACKAGE_JSON)));
    if (errors.hasErrors()) {
      return;
    }

    var compilationProcess = injector.getInstance(CompilationProcess.class);
    testsPath.ifPresent(this::validateTestPath);

    Pair<PhysicalPlan, ? extends TestPlan> plan = compilationProcess.executeCompilation(testsPath);

    if (errors.hasErrors()) {
      return;
    }

    if (isGenerateGraphql()) {
      addGraphql(plan.getLeft(), root.rootDir);
    }

    postprocess(packager, getTargetDir(), plan.getLeft(), plan.getRight());
  }

  protected List<String> getEngines() {
    return List.of();
  }

  protected void postprocess(
      Packager packager, Path targetDir, PhysicalPlan plan, TestPlan testPlan) {
    packager.postprocess(targetDir, plan, testPlan);
  }

  protected boolean isGenerateGraphql() {
    if (generateAPI != null) {
      return Arrays.stream(generateAPI).anyMatch(APIType.GraphQL::equals);
    }
    return false;
  }

  @SneakyThrows
  protected void addGraphql(PhysicalPlan plan, Path rootDir) {
    List<ServerPhysicalPlan> plans = plan.getPlans(ServerPhysicalPlan.class).toList();
    if (plans.isEmpty()) {
      return;
    }

    Path path = rootDir.resolve(GRAPHQL_NORMALIZED_FILE_NAME);
    Files.deleteIfExists(path);
    StringSchema stringSchema = (StringSchema) plans.get(0).getModel().getSchema();
    Files.writeString(path, stringSchema.getSchema(), StandardOpenOption.CREATE);
  }

  protected Path getTargetDir() {
    if (targetDir.isAbsolute()) {
      return targetDir;
    } else {
      return root.rootDir.resolve(targetDir);
    }
  }

  protected PackageJson readSqrlConfig() {
    return SqrlConfig.fromFilesPackageJson(
        ErrorCollector.root(),
        List.of(getTargetDir().getParent().resolve(SqrlConstants.PACKAGE_JSON)));
  }

  private void validateTestPath(Path path) {
    if (!Files.isDirectory(path)) {
      throw new RuntimeException("Could not find test path: " + path.toAbsolutePath());
    }
  }
}
