/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static com.datasqrl.packager.config.ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME;

import com.datasqrl.compile.Compiler;
import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIType;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.repository.CompositeRepositoryImpl;
import com.datasqrl.packager.repository.LocalRepositoryImplementation;
import com.datasqrl.packager.repository.RemoteRepositoryImplementation;
import com.datasqrl.packager.repository.Repository;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
    Repository repository = createRepository(errors);

    PackageBootstrap packageBootstrap = new PackageBootstrap(root.rootDir,
        this.root.packageFiles, this.profiles, this.files, !noinfer);
    SqrlConfig sqrlConfig = packageBootstrap.bootstrap(repository, errors,
        this::createDefaultConfig,
        (c)-> postProcessConfig(c));

    Packager packager = new Packager(repository, root.rootDir, sqrlConfig, errors);
    Path path = packager.preprocess();

    if (errors.hasErrors()) {
      return;
    }

    Compiler compiler = new Compiler();
    Preconditions.checkArgument(Files.isRegularFile(path));

    Compiler.CompilerResult result = compiler.run(errors, path.getParent(), debug, getTargetDir());

    if (errors.hasErrors()) {
      return;
    }

    if (isGenerateGraphql()) {
      addGraphql(root.rootDir.resolve(Packager.BUILD_DIR_NAME), root.rootDir);
    }

    postprocess(packager, result, getTargetDir(), errors);
  }

  public abstract SqrlConfig createDefaultConfig(ErrorCollector errors);

  public SqrlConfig postProcessConfig(SqrlConfig config) {
    return config;
  }

  protected void postprocess(Packager packager, CompilerResult result, Path targetDir,
      ErrorCollector errors) {
    packager.postprocess(result, getTargetDir(), Optional.ofNullable(mountDirectory), profiles);
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
    LocalRepositoryImplementation localRepo = LocalRepositoryImplementation.of(errors);
    //TODO: read remote repository URLs from configuration?
    RemoteRepositoryImplementation remoteRepo = new RemoteRepositoryImplementation();
    remoteRepo.setCacheRepository(localRepo);
    return new CompositeRepositoryImpl(List.of(localRepo, remoteRepo));
  }
}
