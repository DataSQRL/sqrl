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
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.graphql.APIType;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.datasqrl.packager.PackageBootstrap;
import com.datasqrl.packager.Packager;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.SqrlInjector;
import com.google.inject.Guice;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import picocli.CommandLine;

public abstract class AbstractCompileCmd extends AbstractCmd {

  private static final String GRAPHQL_FILE_NAME_PATTERN = "schema.%s.graphqls";

  @CommandLine.Parameters(
      arity = "0..2",
      description = "Main script and (optional) API specification")
  protected Path[] files = new Path[0];

  @CommandLine.Option(
      names = {"-a", "--api"},
      description = "Generates the API specification for the given type")
  protected APIType[] generateAPI = new APIType[0];

  public abstract ExecutionGoal getGoal();

  protected void compile(ErrorCollector errors) {
    var packageBootstrap = new PackageBootstrap(errors);
    var sqrlConfig =
        packageBootstrap.bootstrap(
            cli.rootDir,
            this.cli.packageFiles,
            this.files,
            getGoal() == ExecutionGoal.RUN || getGoal() == ExecutionGoal.TEST);

    DirectoryManager.prepareTargetDirectory(getTargetDir());
    errors.checkFatal(
        Files.isDirectory(cli.rootDir), "Not a valid root directory: %s", cli.rootDir);

    var injector =
        Guice.createInjector(
            new SqrlInjector(errors, cli.rootDir, getTargetDir(), sqrlConfig, getGoal()));

    var engineHolder = injector.getInstance(ExecutionEnginesHolder.class);
    engineHolder.initEnabledEngines();

    var packager = injector.getInstance(Packager.class);
    packager.preprocess(errors.withLocation(ErrorPrefix.CONFIG.resolve(PACKAGE_JSON)));
    if (errors.hasErrors()) {
      return;
    }

    var compilationProcess = injector.getInstance(CompilationProcess.class);
    var testDir = sqrlConfig.getTestConfig().getTestDir(cli.rootDir);
    testDir.ifPresent(this::validateTestPath);

    Pair<PhysicalPlan, ? extends TestPlan> plan = compilationProcess.executeCompilation(testDir);

    if (errors.hasErrors()) {
      return;
    }

    if (isGenerateGraphql()) {
      addGraphql(plan.getLeft(), cli.rootDir);
    }

    postprocess(packager, getTargetDir(), plan.getLeft(), plan.getRight());
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

  protected boolean isGenerateGraphql() {
    if (generateAPI != null) {
      return Arrays.stream(generateAPI).anyMatch(APIType.GraphQL::equals);
    }
    return false;
  }

  @SneakyThrows
  protected void addGraphql(PhysicalPlan plan, Path rootDir) {
    var serverPlan = plan.getPlans(ServerPhysicalPlan.class).findFirst();
    if (serverPlan.isEmpty()) {
      return;
    }

    var models = serverPlan.get().getModels();
    for (var entry : models.entrySet()) {
      var version = entry.getKey();
      var model = entry.getValue();

      var path = rootDir.resolve(GRAPHQL_FILE_NAME_PATTERN.formatted(version));
      Files.deleteIfExists(path);

      var stringSchema = (StringSchema) model.getSchema();
      Files.writeString(path, stringSchema.getSchema(), StandardOpenOption.CREATE);
    }
  }

  private void validateTestPath(Path path) {
    if (!Files.isDirectory(path)) {
      throw new RuntimeException("Could not find test path: " + path.toAbsolutePath());
    }
  }
}
