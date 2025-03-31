package com.datasqrl.cmd;

import com.datasqrl.config.Dependency;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.DependenciesConfig;
import com.datasqrl.config.PackageJson.ScriptConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.packager.Packager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class PackageBootstrap {
  ErrorCollector errors;

  @SneakyThrows
  public PackageJson bootstrap(Path rootDir, List<Path> packageFiles, Path[] files) {
    ErrorCollector errors = this.errors.withLocation(ErrorPrefix.CONFIG).resolve("package");

    // Create build dir to unpack resolved dependencies
    Path buildDir = rootDir.resolve(SqrlConstants.BUILD_DIR_NAME);
    Packager.cleanBuildDir(buildDir);
    Packager.createBuildDir(buildDir);

    Optional<List<Path>> existingPackage = Packager.findPackageFile(rootDir, packageFiles);

    Map<String, Dependency> dependencies = new HashMap<>();

    // Create package.json from project root if exists
    List<Path> configFiles = new ArrayList<>();

    existingPackage.ifPresent(configFiles::addAll);

    // Could not find any package json
    PackageJson packageJson = SqrlConfigCommons.fromFilesPackageJson(errors, configFiles);

    // Add dependencies of discovered profiles
    dependencies.forEach(
        (key, dep) -> {
          DependenciesConfig dependenciesConfig = packageJson.getDependencies();
          dependenciesConfig.addDependency(key, dep);
        });

    // Override main and graphql if they are specified as command line arguments
    Optional<Path> mainScript =
        (files.length > 0 && files[0].getFileName().toString().toLowerCase().endsWith(".sqrl"))
            ? Optional.of(files[0])
            : Optional.empty();
    Optional<Path> graphQLSchemaFile =
        (files.length > 1) ? Optional.of(files[1]) : Optional.empty();

    ScriptConfig scriptConfig = packageJson.getScriptConfig();
    boolean isMainScriptSet = scriptConfig.getMainScript().isPresent();
    boolean isGraphQLSet = scriptConfig.getGraphql().isPresent();

    // Set main script if not already set and if it's a regular file
    if (mainScript.isPresent() && Files.isRegularFile(relativize(rootDir, mainScript))) {
      scriptConfig.setMainScript(mainScript.get().toString());
    } else if (!isMainScriptSet && mainScript.isPresent()) {
      errors.fatal("Main script is not a regular file: %s", mainScript.get());
    } else if (!isMainScriptSet && files.length > 0) {
      errors.fatal("Main script is not a sqrl script: %s", files[0].getFileName().toString());
    } else if (!isMainScriptSet && mainScript.isEmpty()) {
      errors.fatal("No main sqrl script specified");
    }

    // Set GraphQL schema file if not already set and if it's a regular file
    if (graphQLSchemaFile.isPresent()
        && Files.isRegularFile(relativize(rootDir, graphQLSchemaFile))) {
      scriptConfig.setGraphql(graphQLSchemaFile.get().toString());
    } else if (!isGraphQLSet && graphQLSchemaFile.isPresent()) {
      errors.fatal("GraphQL schema file is not a regular file: %s", graphQLSchemaFile.get());
    }

    return packageJson;
  }

  /**
   * We want to guard against confusion with remote repo names and filesystem paths so we can throw
   * sensible error messages
   */
  public static boolean isLocalProfile(Path rootDir, String profile) {
    // 1. Check if it's on the local file system
    if (Files.isDirectory(rootDir.resolve(profile))) {
      // 1. Profile must contain a package json
      if (!Files.isRegularFile(rootDir.resolve(profile).resolve(SqrlConstants.PACKAGE_JSON))) {
        log.info(
            "Profile ["
                + profile
                + "] is a directory but missing a package.json. Attempting to resolve as a remote"
                + " profile.");
        return false;
      }

      return true;
    }
    // 2. Check if it looks like a repo link
    if (Pattern.matches("^\\w+(?:\\.\\w+)+$", profile)) {
      return false;
    }

    throw new RuntimeException(
        String.format(
            "Unknown profile format [%s]. It must be either be a filesystem folder or a repository"
                + " name.",
            profile));
  }

  private Path relativize(Path rootDir, Optional<Path> path) {
    return path.get().isAbsolute() ? path.get() : rootDir.resolve(path.get());
  }
}
