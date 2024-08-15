/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static com.datasqrl.config.ScriptConfigImpl.GRAPHQL_NORMALIZED_FILE_NAME;
import static com.datasqrl.packager.Packager.PACKAGE_JSON;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.compile.CompilationProcess;
import com.datasqrl.compile.DirectoryManager;
import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.graphql.APIType;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.datasqrl.inject.SqrlInjector;
import com.datasqrl.inject.StatefulModule;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.repository.CompositeRepositoryImpl;
import com.datasqrl.packager.repository.LocalRepositoryImplementation;
import com.datasqrl.packager.repository.RemoteRepositoryImplementation;
import com.datasqrl.packager.repository.Repository;
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

  public static final Path DEFAULT_PLAN_DIR = Path.of("build", "deploy");
  public static final Path DEFAULT_DEPLOY_DIR = Path.of("build", "deploy");

  @CommandLine.Parameters(arity = "0..2", description = "Main script and (optional) API specification")
  protected Path[] files = new Path[0];

  @CommandLine.Option(names = {"-a", "--api"},
      description = "Generates the API specification for the given type")
  protected APIType[] generateAPI = new APIType[0];

  @CommandLine.Option(names = {"-t", "--target"},
      description = "Target directory for deployment artifacts")
  protected Path targetDir = DEFAULT_DEPLOY_DIR;

  @CommandLine.Option(names = {"--profile"},
      description = "An alternative set of configuration values which override the default package.json")
  protected String[] profiles = new String[0];

  @CommandLine.Option(names = {"--plan"},
      description = "Target directory for the plan jsons")
  protected Path planDir = DEFAULT_PLAN_DIR;

  @SneakyThrows
  public void execute(ErrorCollector errors) {
    execute(errors, profiles, targetDir.resolve("snapshots"),
        Optional.empty(), ExecutionGoal.COMPILE);
  }

  public void execute(ErrorCollector errors, String[] profiles, Path snapshotPath, Optional<Path> testsPath,
      ExecutionGoal goal) {
    Repository repository = createRepository(errors);

    PackageBootstrap packageBootstrap = new PackageBootstrap(repository, errors);
    PackageJson sqrlConfig = packageBootstrap.bootstrap(root.rootDir, this.root.packageFiles,
        profiles, this.files);

    Optional<String> snapshotPathConf = sqrlConfig.getCompilerConfig()
        .getSnapshotPath();
    if (snapshotPathConf.isEmpty()) {
      sqrlConfig.getCompilerConfig()
          .setSnapshotPath(snapshotPath.toAbsolutePath().toString());
    }

    if (goal == ExecutionGoal.TEST) {
      List<String> defaultKeys = List.of(EngineKeys.TEST, EngineKeys.DATABASE, EngineKeys.LOG,
          EngineKeys.SERVER, EngineKeys.STREAMS);
      sqrlConfig.setPipeline(defaultKeys);
    }

    DirectoryManager.prepareTargetDirectory(getTargetDir());
    errors.checkFatal(Files.isDirectory(root.rootDir), "Not a valid root directory: %s", root.rootDir);

    Injector injector = Guice.createInjector(
        new SqrlInjector(errors, root.rootDir, getTargetDir(), sqrlConfig, getGoal(), repository),
        new StatefulModule(new SqrlSchema(new TypeFactory(), NameCanonicalizer.SYSTEM)));

    Packager packager = injector.getInstance(Packager.class);
    packager.preprocess(errors.withLocation(ErrorPrefix.CONFIG.resolve(PACKAGE_JSON)));
    if (errors.hasErrors()) {
      return;
    }

    CompilationProcess compilationProcess = injector.getInstance(CompilationProcess.class);
    testsPath.ifPresent(this::validateTestPath);

    Pair<PhysicalPlan, TestPlan> plan = compilationProcess.executeCompilation(testsPath);

    if (errors.hasErrors()) {
      return;
    }

    if (isGenerateGraphql()) {
      addGraphql(plan.getLeft(), root.rootDir);
    }

    postprocess(sqrlConfig, packager, planDir, getTargetDir(), plan.getLeft(), plan.getRight(), errors);
  }

  private void validateTestPath(Path path) {
    if (!Files.isDirectory(path)) {
      throw new RuntimeException("Could not find test path: "+ path.toAbsolutePath());
    }
  }

  protected void postprocess(PackageJson sqrlConfig, Packager packager, Path planDir, Path targetDir,
      PhysicalPlan plan, TestPlan testPlan, ErrorCollector errors) {
    packager.postprocess(sqrlConfig, root.rootDir, planDir, getTargetDir(), plan, testPlan,
        sqrlConfig.getProfiles());

  }

  protected boolean isGenerateGraphql() {
    if (generateAPI != null) {
      return Arrays.stream(generateAPI).anyMatch(APIType.GraphQL::equals);
    }
    return false;
  }

  @SneakyThrows
  protected void addGraphql(PhysicalPlan plan, Path rootDir) {
    List<ServerPhysicalPlan> plans = plan.getPlans(ServerPhysicalPlan.class)
        .collect(Collectors.toList());
    if (plans.isEmpty()) {
      return;
    }

    Path path = rootDir.resolve(GRAPHQL_NORMALIZED_FILE_NAME);
    Files.deleteIfExists(path);
    StringSchema stringSchema = (StringSchema)plans.get(0).getModel().getSchema();
    Files.writeString(path, stringSchema.getSchema(), StandardOpenOption.CREATE);
  }

  private Path getTargetDir() {
    if (DEFAULT_DEPLOY_DIR.equals(targetDir)) {
      return root.rootDir.resolve(targetDir);
    }
    return targetDir;
  }

  protected Repository createRepository(ErrorCollector errors) {
    LocalRepositoryImplementation localRepo = LocalRepositoryImplementation.of(errors,
        root.rootDir);
    //TODO: read remote repository URLs from configuration?
    RemoteRepositoryImplementation remoteRepo = new RemoteRepositoryImplementation();
    remoteRepo.setCacheRepository(localRepo);
    return new CompositeRepositoryImpl(List.of(localRepo, remoteRepo));
  }

  public abstract ExecutionGoal getGoal();
}
