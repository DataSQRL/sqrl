/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;


import static com.datasqrl.config.ScriptConfigImpl.GRAPHQL_NORMALIZED_FILE_NAME;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.compile.CompilationProcess;
import com.datasqrl.compile.DirectoryManager;
import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIType;
import com.datasqrl.inject.SqrlInjector;
import com.datasqrl.inject.StatefulModule;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.repository.CompositeRepositoryImpl;
import com.datasqrl.packager.repository.LocalRepositoryImplementation;
import com.datasqrl.packager.repository.RemoteRepositoryImplementation;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

@Slf4j
public abstract class AbstractCompilerCommand extends AbstractCommand {

  public static final Path DEFAULT_DEPLOY_DIR = Path.of("build", "deploy");

  @CommandLine.Parameters(arity = "1..2", description = "Main script and (optional) API specification")
  protected Path[] files;

  @CommandLine.Option(names = {"-a", "--api"},
      description = "Generates the API specification for the given type")
  protected APIType[] generateAPI = new APIType[0];

  @CommandLine.Option(names = {"-d", "--debug"},
      description = "Outputs table changestream to configured sink for debugging")
  protected boolean debug = false;

  @CommandLine.Option(names = {"-t", "--target"},
      description = "Target directory for deployment artifacts")
  protected Path targetDir = DEFAULT_DEPLOY_DIR;

  @CommandLine.Option(names = {"--mnt"}, description = "Local directory to mount for data access")
  protected Path mountDirectory = null;

  @CommandLine.Option(names = {"--nolookup"},
      description = "Do not look up package dependencies in the repository",
      scope = ScopeType.INHERIT)
  protected boolean noinfer = false;

  @CommandLine.Option(names = {"-p", "--profile"},
      description = "An alternative set of configuration values which override the default package.json")
  protected String[] profiles = new String[0];

  @SneakyThrows
  public void execute(ErrorCollector errors) {
    execute(errors, profiles, targetDir.resolve("snapshots"),
        targetDir.resolve("tests")
        );
  }

  public void execute(ErrorCollector errors, String[] profiles, Path snapshotPath, Path testsPath) {
    Repository repository = createRepository(errors);

    PackageBootstrap packageBootstrap = new PackageBootstrap(root.rootDir,
        this.root.packageFiles, profiles, this.files, !noinfer);
    PackageJson sqrlConfig = packageBootstrap.bootstrap(repository, errors,
        this::createDefaultConfig,
        this::postProcessConfig, targetDir);

    Optional<String> snapshotPathConf = sqrlConfig.getCompilerConfig()
        .getSnapshotPath();
    if (snapshotPathConf.isEmpty()) {
      sqrlConfig.getCompilerConfig()
          .setSnapshotPath(snapshotPath.toAbsolutePath().toString());
    }

    sqrlConfig.setPipeline(ListUtils.union(sqrlConfig.getPipeline(), List.of("test")));

    Packager packager = new Packager(repository, root.rootDir, sqrlConfig, errors);
    Path path = packager.preprocess();

    if (errors.hasErrors()) {
      return;
    }

    Preconditions.checkArgument(Files.isRegularFile(path));

    DirectoryManager.prepareTargetDirectory(getTargetDir());

    Injector injector = Guice.createInjector(
        new SqrlInjector(errors, root.rootDir, getTargetDir(), debug, sqrlConfig, getGoal()),
        new StatefulModule(new SqrlSchema(new TypeFactory(), NameCanonicalizer.SYSTEM)));
    CompilationProcess compilationProcess = injector.getInstance(CompilationProcess.class);
    Pair<PhysicalPlan, TestPlan> plan = compilationProcess.executeCompilation(testsPath);

    if (errors.hasErrors()) {
      return;
    }

    if (isGenerateGraphql()) {
      addGraphql(root.rootDir.resolve(Packager.BUILD_DIR_NAME), root.rootDir);
    }

    postprocess(sqrlConfig, packager, getTargetDir(), plan.getLeft(), plan.getRight(), errors);
  }

  public abstract PackageJson createDefaultConfig(ErrorCollector errors);

  public PackageJson postProcessConfig(PackageJson config) {
    return config;
  }

  protected void postprocess(PackageJson sqrlConfig, Packager packager, Path targetDir,
      PhysicalPlan plan, TestPlan testPlan, ErrorCollector errors) {
    packager.postprocess(sqrlConfig, root.rootDir, getTargetDir(), plan, Optional.ofNullable(mountDirectory), testPlan, profiles);
  }

  protected boolean isGenerateGraphql() {
    if (generateAPI != null) {
      return Arrays.stream(generateAPI).anyMatch(APIType.GraphQL::equals);
    }
    return false;
  }

  @SneakyThrows
  protected void addGraphql(Path build, Path rootDir) {
    Files.copy(build.resolve(GRAPHQL_NORMALIZED_FILE_NAME),
        rootDir.resolve(GRAPHQL_NORMALIZED_FILE_NAME));
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
