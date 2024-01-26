/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static com.datasqrl.packager.Packager.PACKAGE_JSON;
import static com.datasqrl.packager.config.ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME;

import com.datasqrl.compile.Compiler;
import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.graphql.APIType;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.config.DependencyConfig;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.packager.repository.CompositeRepositoryImpl;
import com.datasqrl.packager.repository.LocalRepositoryImplementation;
import com.datasqrl.packager.repository.RemoteRepositoryImplementation;
import com.datasqrl.packager.repository.Repository;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    SqrlConfig sqrlConfig = bootstrapPackageJson(repository, errors);

    Packager packager = new Packager(repository, root.rootDir, sqrlConfig, errors);
    Path path = packager.preprocess(!noinfer);

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

  @SneakyThrows
  private SqrlConfig bootstrapPackageJson(Repository repository, ErrorCollector errors) {
    errors = errors.withLocation(ErrorPrefix.CONFIG).resolve("package");

    //Create build dir to unpack resolved dependencies
    Path buildDir = root.rootDir.resolve(Packager.BUILD_DIR_NAME);
    Packager.cleanUp(buildDir);
    Packager.cleanBuildDir(buildDir);
    Packager.createBuildDir(buildDir);

    Map<String, Dependency> dependencies = new HashMap<>();
    //Download any profiles
    for (String profile : profiles) {
      if (profile.contains(".")) { //possible repo profile
        Optional<Dependency> dependency = repository.resolveDependency(profile);
        if (dependency.isPresent()) {
          boolean success = repository.retrieveDependency(root.rootDir.resolve("build"), dependency.get());
          if (success) {
            dependencies.put(profile, dependency.get());
          } else {
            throw new RuntimeException("Could not retrieve profile dependency: " + profile);
          }
        }
      }
    }

    //Create package.json
    List<Path> configFiles = new ArrayList<>();

    //explicit cli package overrides
    if (!root.packageFiles.isEmpty()) {
      configFiles.addAll(root.packageFiles);
    } else { //check for implicit package json
      if (Files.exists(root.rootDir.resolve(PACKAGE_JSON))) {
        configFiles.add(root.rootDir.resolve(PACKAGE_JSON));
      }
    }

    // Add profile config files
    for (String profile : profiles) {
      Path localProfile = root.rootDir.resolve(profile).resolve(PACKAGE_JSON);
      Path remoteProfile = root.rootDir.resolve(Packager.BUILD_DIR_NAME).resolve(profile).resolve(PACKAGE_JSON);

      if (Files.isRegularFile(localProfile)) {//Look for local
        configFiles.add(localProfile);
      } else if (Files.isRegularFile(remoteProfile)) { //look for remote
        configFiles.add(remoteProfile);
      } else {
        throw new RuntimeException("Could not find profile: " + profile);
      }
    }

    if (root.packageFiles.isEmpty() && configFiles.isEmpty()) { //No profiles found, use default
      SqrlConfig defaultConfig = createSqrlConfig(errors);
      Path path = buildDir.resolve(PACKAGE_JSON);
      defaultConfig.toFile(path);
      configFiles.add(path);
    }

    // Could not find any package json
    if (configFiles.isEmpty()) {
      throw new RuntimeException("Could not find package.json");
    }

    // Merge all configurations
    SqrlConfig sqrlConfig = SqrlConfigCommons.fromFiles(errors, configFiles);

    //Add dependencies of discovered profiles
    dependencies.forEach((key, dep) -> {
      sqrlConfig.getSubConfig(DependencyConfig.DEPENDENCIES_KEY).getSubConfig(key).setProperties(dep);
    });

    //Override main and graphql if they are specified as command line arguments
    Optional<Path> mainScript = (files.length > 0) ? Optional.of(root.rootDir.resolve(files[0])) : Optional.empty();
    Optional<Path> graphQLSchemaFile = (files.length > 1) ? Optional.of(root.rootDir.resolve(files[1])) : Optional.empty();

    SqrlConfig scriptConfig = ScriptConfiguration.fromScriptConfig(sqrlConfig);
    boolean isMainScriptSet = scriptConfig.asString(ScriptConfiguration.MAIN_KEY).getOptional()
        .isPresent();
    boolean isGraphQLSet = scriptConfig.asString(ScriptConfiguration.GRAPHQL_KEY).getOptional()
        .isPresent();

    // Set main script if not already set and if it's a regular file
    if (mainScript.isPresent() && Files.isRegularFile(mainScript.get())) {
      scriptConfig.setProperty(ScriptConfiguration.MAIN_KEY, root.rootDir.relativize(mainScript.get()).normalize().toString());
    } else if (!isMainScriptSet && mainScript.isPresent()) {
      errors.fatal("Main script is not a regular file: %s", mainScript.get());
    } else {
      errors.fatal("No main sqrl script specified");
    }

    // Set GraphQL schema file if not already set and if it's a regular file
    if (graphQLSchemaFile.isPresent() && Files.isRegularFile(graphQLSchemaFile.get())) {
      scriptConfig.setProperty(ScriptConfiguration.GRAPHQL_KEY, root.rootDir.relativize(graphQLSchemaFile.get()).normalize().toString());
    } else if (!isGraphQLSet && graphQLSchemaFile.isPresent()) {
      errors.fatal("GraphQL schema file is not a regular file: %s", graphQLSchemaFile.get());
    }

    return sqrlConfig;
  }

  public abstract SqrlConfig createSqrlConfig(ErrorCollector errors);

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
