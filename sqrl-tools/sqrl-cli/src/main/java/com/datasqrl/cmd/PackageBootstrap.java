package com.datasqrl.cmd;

import static com.datasqrl.packager.Packager.PACKAGE_JSON;
import static com.datasqrl.packager.Packager.PROFILES_KEY;
import static com.datasqrl.packager.config.DependencyConfig.PKG_NAME_KEY;
import static com.datasqrl.packager.config.DependencyConfig.VARIANT_KEY;
import static com.datasqrl.packager.config.DependencyConfig.VERSION_KEY;
import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.impl.print.PrintDataSystemFactory;
import com.datasqrl.loaders.StandardLibraryLoader;
import com.datasqrl.packager.ImportExportAnalyzer;
import com.datasqrl.packager.ImportExportAnalyzer.Result;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.config.DependencyConfig;
import com.datasqrl.packager.config.PackageConfiguration;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.packager.repository.Repository;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor
public class PackageBootstrap {
  Path rootDir;
  List<Path> packageFiles;
  String[] profiles;
  Path[] files;
  boolean inferDependencies;
  

  @SneakyThrows
  public SqrlConfig bootstrap(Repository repository, ErrorCollector errors,
      Function<ErrorCollector, SqrlConfig> defaultConfigFnc,
      Function<SqrlConfig, SqrlConfig> postProcess) {
    errors = errors.withLocation(ErrorPrefix.CONFIG).resolve("package");

    //Create build dir to unpack resolved dependencies
    Path buildDir = rootDir.resolve(Packager.BUILD_DIR_NAME);
    Packager.cleanBuildDir(buildDir);
    Packager.createBuildDir(buildDir);

    Optional<List<Path>> existingPackage = Packager.findPackageFile(rootDir, packageFiles);
    Optional<SqrlConfig> existingConfig;
    if (existingPackage.isPresent()) {
      existingConfig = Optional.of(SqrlConfigCommons.fromFiles(errors, existingPackage.get()));
    } else {
      existingConfig = Optional.empty();
    }

    Map<String, Dependency> dependencies = new HashMap<>();
    // Check if 'profiles' key is set, merge result with switches
    String[] profiles;
    if (existingConfig.isPresent() && existingConfig.get().hasKey(PROFILES_KEY)) {
      List<String> configProfiles = existingConfig.get().asList(PROFILES_KEY, String.class)
          .get();
      Set<String> profileSet = new LinkedHashSet<>();
      profileSet.addAll(configProfiles);
      profileSet.addAll(Arrays.asList(this.profiles));
      profiles = profileSet.toArray(String[]::new);
    } else {
      profiles = this.profiles;
    }

    //Download any profiles
    for (String profile : profiles) {
      if (profile.contains(".")) { //possible repo profile
        //check to see if it's already in the package json, download the correct dep
        Optional<Dependency> dependency;
        if (hasVersionedProfileDependency(existingConfig, profile)) {
          SqrlConfig depConfig = existingConfig.get()
              .getSubConfig(DependencyConfig.DEPENDENCIES_KEY)
              .getSubConfig(profile);
          dependency = Optional.of(new Dependency(depConfig.asString(PKG_NAME_KEY).get(),
              depConfig.asString(VERSION_KEY).get(),
              depConfig.asString(VARIANT_KEY).getOptional()
                  .orElse(PackageConfiguration.DEFAULT_VARIANT)));
        } else {
          dependency = repository.resolveDependency(profile);
        }

        if (dependency.isPresent()) {
          boolean success = repository.retrieveDependency(rootDir.resolve("build"),
              dependency.get());
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
    if (!packageFiles.isEmpty()) {
      configFiles.addAll(packageFiles);
    } else { //check for implicit package json
      if (Files.exists(rootDir.resolve(PACKAGE_JSON))) {
        configFiles.add(rootDir.resolve(PACKAGE_JSON));
      }
    }

    // Add profile config files
    for (String profile : profiles) {
      Path localProfile = rootDir.resolve(profile).resolve(PACKAGE_JSON);
      Path remoteProfile = rootDir.resolve(Packager.BUILD_DIR_NAME).resolve(profile).resolve(PACKAGE_JSON);

      if (Files.isRegularFile(localProfile)) {//Look for local
        configFiles.add(localProfile);
      } else if (Files.isRegularFile(remoteProfile)) { //look for remote
        configFiles.add(remoteProfile);
      } else {
        throw new RuntimeException("Could not find profile: " + profile);
      }
    }

    if (packageFiles.isEmpty() && configFiles.isEmpty()) { //No profiles found, use default
      SqrlConfig defaultConfig = defaultConfigFnc.apply(errors);
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

    sqrlConfig.setProperty(PROFILES_KEY, profiles);

    //Add dependencies of discovered profiles
    dependencies.forEach((key, dep) -> {
      sqrlConfig.getSubConfig(DependencyConfig.DEPENDENCIES_KEY).getSubConfig(key).setProperties(dep);
    });

    //Override main and graphql if they are specified as command line arguments
    Optional<Path> mainScript = (files.length > 0) ? Optional.of(files[0]) : Optional.empty();
    Optional<Path> graphQLSchemaFile = (files.length > 1) ? Optional.of(files[1]) : Optional.empty();

    SqrlConfig scriptConfig = ScriptConfiguration.fromScriptConfig(sqrlConfig);
    boolean isMainScriptSet = scriptConfig.asString(ScriptConfiguration.MAIN_KEY).getOptional()
        .isPresent();
    boolean isGraphQLSet = scriptConfig.asString(ScriptConfiguration.GRAPHQL_KEY).getOptional()
        .isPresent();

    // Set main script if not already set and if it's a regular file
    if (mainScript.isPresent() && Files.isRegularFile(relativize(mainScript))) {
      scriptConfig.setProperty(ScriptConfiguration.MAIN_KEY, mainScript.get().toString());
    } else if (!isMainScriptSet && mainScript.isPresent()) {
      errors.fatal("Main script is not a regular file: %s", mainScript.get());
    } else if (!isMainScriptSet && mainScript.isEmpty()){
      errors.fatal("No main sqrl script specified");
    }

    // Set GraphQL schema file if not already set and if it's a regular file
    if (graphQLSchemaFile.isPresent() && Files.isRegularFile(relativize(graphQLSchemaFile))) {
      scriptConfig.setProperty(ScriptConfiguration.GRAPHQL_KEY, graphQLSchemaFile.get().toString());
    } else if (!isGraphQLSet && graphQLSchemaFile.isPresent()) {
      errors.fatal("GraphQL schema file is not a regular file: %s", graphQLSchemaFile.get());
    }

    if (inferDependencies) {
      inferDependencies(repository, sqrlConfig, errors);
    }

    return postProcess.apply(sqrlConfig);
  }

  private Path relativize(Optional<Path> path) {
    return path.get().isAbsolute() ? path.get() : rootDir.resolve(path.get());
  }

  private boolean hasVersionedProfileDependency(Optional<SqrlConfig> existingConfig, String profile) {
    return existingConfig.isPresent()
        && existingConfig.get()
        .getSubConfig(DependencyConfig.DEPENDENCIES_KEY)
        .hasSubConfig(profile)
        && existingConfig.get()
        .getSubConfig(DependencyConfig.DEPENDENCIES_KEY)
        .getSubConfig(profile)
        .asString(VERSION_KEY).getOptional().isPresent();
  }

  private void inferDependencies(Repository repository, SqrlConfig config, ErrorCollector errors) throws IOException {
    //Analyze all local SQRL files to discovery transitive or undeclared dependencies
    //At the end, we'll add the new dependencies to the package config.
    ImportExportAnalyzer analyzer = new ImportExportAnalyzer();

    BiPredicate<Path, BasicFileAttributes> FIND_SQRL_SCRIPT = (p, f) ->
        f.isRegularFile() && p.getFileName().toString().toLowerCase().endsWith(".sqrl");

    // Find all SQRL script files
    Result allResults = Files.find(rootDir, 128, FIND_SQRL_SCRIPT)
        .map(script -> analyzer.analyze(script, errors))
        .reduce(Result.EMPTY, Result::add);

    StandardLibraryLoader standardLibraryLoader = new StandardLibraryLoader();
    Set<NamePath> pkgs = new HashSet<>(allResults.getPkgs());
    pkgs.removeAll(standardLibraryLoader.loadedLibraries());
    pkgs.remove(Name.system(PrintDataSystemFactory.SYSTEM_NAME).toNamePath());

    Set<NamePath> unloadedDeps = new HashSet<>();
    for (NamePath packagePath : pkgs) {
      Path dir = namepath2Path(rootDir, packagePath);
      if (!Files.exists(dir)) {
        unloadedDeps.add(packagePath);
      }
    }

    LinkedHashMap<String, Dependency> inferredDependencies = new LinkedHashMap<>();

    //Resolve dependencies
    for (NamePath unloadedDep : unloadedDeps) {
      repository
          .resolveDependency(unloadedDep.toString())
          .ifPresentOrElse((dep) -> inferredDependencies.put(unloadedDep.toString(), dep),
              () -> errors.checkFatal(true, "Could not infer dependency: %s", unloadedDep));
    }

    // Add inferred dependencies to package config
    inferredDependencies.forEach((key, dep) -> {
      config.getSubConfig(DependencyConfig.DEPENDENCIES_KEY).getSubConfig(key).setProperties(dep);
    });
  }
}
