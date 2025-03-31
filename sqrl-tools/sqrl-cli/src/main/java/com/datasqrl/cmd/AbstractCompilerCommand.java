/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static com.datasqrl.config.ScriptConfigImpl.GRAPHQL_NORMALIZED_FILE_NAME;
import static com.datasqrl.config.SqrlConstants.PACKAGE_JSON;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.compile.CompilationProcessV2;
import com.datasqrl.compile.DirectoryManager;
import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.graphql.APIType;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.datasqrl.inject.SqrlInjector;
import com.datasqrl.inject.StatefulModule;
import com.datasqrl.packager.Packager;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.commons.lang3.tuple.Pair;
import picocli.CommandLine;

@Slf4j
public abstract class AbstractCompilerCommand extends AbstractCommand {

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

  @SneakyThrows
  public void execute(ErrorCollector errors) {
    execute(errors, root.rootDir.resolve("snapshots"), Optional.empty(), ExecutionGoal.COMPILE);
  }

  public void execute(
      ErrorCollector errors, Path snapshotPath, Optional<Path> testsPath, ExecutionGoal goal) {

    PackageBootstrap packageBootstrap = new PackageBootstrap(errors);
    PackageJson sqrlConfig =
        packageBootstrap.bootstrap(root.rootDir, this.root.packageFiles, this.files);

    Optional<String> snapshotPathConf = sqrlConfig.getCompilerConfig().getSnapshotPath();
    if (snapshotPathConf.isEmpty()) {
      sqrlConfig.getCompilerConfig().setSnapshotPath(snapshotPath.toAbsolutePath().toString());
    }

    if (goal == ExecutionGoal.TEST) {
      List<String> defaultKeys =
          List.of(
              EngineKeys.TEST,
              EngineKeys.DATABASE,
              EngineKeys.LOG,
              EngineKeys.ICEBERG,
              EngineKeys.DUCKDB,
              EngineKeys.SERVER,
              EngineKeys.STREAMS);
      sqrlConfig.setPipeline(defaultKeys);
    }

    DirectoryManager.prepareTargetDirectory(getTargetDir());
    errors.checkFatal(
        Files.isDirectory(root.rootDir), "Not a valid root directory: %s", root.rootDir);

    Injector injector =
        Guice.createInjector(
            new SqrlInjector(errors, root.rootDir, getTargetDir(), sqrlConfig, getGoal()),
            new StatefulModule(new SqrlSchema(new TypeFactory(), NameCanonicalizer.SYSTEM)));

    Packager packager = injector.getInstance(Packager.class);
    packager.preprocess(errors.withLocation(ErrorPrefix.CONFIG.resolve(PACKAGE_JSON)));
    if (errors.hasErrors()) {
      return;
    }

    CompilationProcessV2 compilationProcess = injector.getInstance(CompilationProcessV2.class);
    testsPath.ifPresent(this::validateTestPath);

    Pair<PhysicalPlan, ? extends TestPlan> plan = compilationProcess.executeCompilation(testsPath);

    if (errors.hasErrors()) {
      return;
    }

    if (isGenerateGraphql()) {
      addGraphql(plan.getLeft(), root.rootDir);
    }

    postprocess(
        sqrlConfig,
        packager,
        getTargetDir(),
        plan.getLeft(),
        plan.getRight(),
        snapshotPath,
        errors);
  }

  private void validateTestPath(Path path) {
    if (!Files.isDirectory(path)) {
      throw new RuntimeException("Could not find test path: " + path.toAbsolutePath());
    }
  }

  protected void postprocess(
      PackageJson sqrlConfig,
      Packager packager,
      Path targetDir,
      PhysicalPlan plan,
      TestPlan testPlan,
      Path snapshotPath,
      ErrorCollector errors) {

    packager.postprocess(sqrlConfig, root.rootDir, getTargetDir(), plan, testPlan);
  }

  protected boolean isGenerateGraphql() {
    if (generateAPI != null) {
      return Arrays.stream(generateAPI).anyMatch(APIType.GraphQL::equals);
    }
    return false;
  }

  @SneakyThrows
  protected void addGraphql(PhysicalPlan plan, Path rootDir) {
    List<ServerPhysicalPlan> plans =
        plan.getPlans(ServerPhysicalPlan.class).collect(Collectors.toList());
    if (plans.isEmpty()) {
      return;
    }

    Path path = rootDir.resolve(GRAPHQL_NORMALIZED_FILE_NAME);
    Files.deleteIfExists(path);
    StringSchema stringSchema = (StringSchema) plans.get(0).getModel().getSchema();
    Files.writeString(path, stringSchema.getSchema(), StandardOpenOption.CREATE);
  }

  private Path getTargetDir() {
    if (targetDir.isAbsolute()) {
      return targetDir;
    } else {
      return root.rootDir.resolve(targetDir);
    }
  }

  public abstract ExecutionGoal getGoal();
}
