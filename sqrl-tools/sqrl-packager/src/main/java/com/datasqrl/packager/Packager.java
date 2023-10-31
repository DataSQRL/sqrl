/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import static com.datasqrl.packager.LambdaUtil.rethrowCall;
import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.impl.print.PrintDataSystemFactory;
import com.datasqrl.loaders.StandardLibraryLoader;
import com.datasqrl.packager.ImportExportAnalyzer.Result;
import com.datasqrl.packager.Preprocessors.PreprocessorsContext;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.config.DependencyConfig;
import com.datasqrl.packager.preprocess.DataSystemPreprocessor;
import com.datasqrl.packager.preprocess.JarPreprocessor;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.packager.preprocess.TablePreprocessor;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.calcite.shaded.org.apache.commons.io.FileUtils;

@Value
public class Packager {

  public static final String BUILD_DIR_NAME = "build";
  public static final String PACKAGE_FILE_NAME = "package.json";

  private static final BiPredicate<Path, BasicFileAttributes> FIND_SQRL_SCRIPT = (p, f) ->
      f.isRegularFile() && p.getFileName().toString().toLowerCase().endsWith(".sqrl");

  Repository repository;
  Path rootDir;
  SqrlConfig config;
  ErrorCollector errors;
  Path buildDir;

  public Packager(@NonNull Repository repository, @NonNull Path rootDir,
      @NonNull SqrlConfig config,
      @NonNull ErrorCollector errors) {
    Preconditions.checkArgument(Files.isDirectory(rootDir));
    this.repository = repository;
    this.rootDir = rootDir;
    this.buildDir = rootDir.resolve(BUILD_DIR_NAME);
    this.config = config;
    this.errors = errors.withLocation(ErrorPrefix.CONFIG.resolve(PACKAGE_FILE_NAME));
  }

  public Path populateBuildDir(boolean inferDependencies) {
    errors.checkFatal(
        ScriptConfiguration.fromRootConfig(config).asString(ScriptConfiguration.MAIN_KEY)
            .getOptional().map(StringUtils::isNotBlank).orElse(false),
        "No config or main script specified");
    try {
      cleanBuildDir();
      createBuildDir();
      if (inferDependencies) {
        inferDependencies();
      }
      retrieveDependencies();
      copyFilesToBuildDir();
      preProcessFiles(config);
      writePackageConfig();
      return buildDir.resolve(PACKAGE_FILE_NAME);
    } catch (IOException e) {
      throw errors.handle(e);
    }
  }

  private void createBuildDir() throws IOException {
    Files.createDirectories(buildDir);
  }


  private void inferDependencies() throws IOException {
    //Analyze all local SQRL files to discovery transitive or undeclared dependencies
    //At the end, we'll add the new dependencies to the package config.
    ImportExportAnalyzer analyzer = new ImportExportAnalyzer();

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

  /**
   * Helper function for retrieving listed dependencies.
   */
  private void retrieveDependencies() {
    LinkedHashMap<String, Dependency> dependencies = DependencyConfig.fromRootConfig(config);
    ErrorCollector depErrors = config.getErrorCollector()
        .resolve(DependencyConfig.DEPENDENCIES_KEY);
    dependencies.entrySet().stream()
        .map(entry -> rethrowCall(() ->
            retrieveDependency(buildDir, NamePath.parse(entry.getKey()),
                entry.getValue().normalize(entry.getKey(), depErrors))
                ? Optional.<NamePath>empty()
                : Optional.of(NamePath.parse(entry.getKey()))))
        .flatMap(Optional::stream)
        .forEach(failedDep -> depErrors.fatal("Could not retrieve dependency: %s", failedDep));
  }

  /**
   * Helper function to copy files to build directory.
   */

  private void copyFilesToBuildDir() throws IOException {
    Map<String, Optional<Path>> destinationPaths = copyScriptFilesToBuildDir();
    //Files should exist, if error occurs its internal, hence we create root error collector
    PackagerConfig.setScriptFiles(buildDir, ScriptConfiguration.fromRootConfig(config),
        destinationPaths, ErrorCollector.root());

    String buildFile = FileUtil.readResource("build.gradle");
    Files.copy(new ByteArrayInputStream(buildFile.getBytes()),
        buildDir.resolve("build.gradle"));
  }

  /**
   * Copies all the files in the script configuration section of the config to the build dir
   * and either normalizes the file or preserves the relative path.
   *
   * @throws IOException
   */
  private Map<String, Optional<Path>> copyScriptFilesToBuildDir() throws IOException {
    SqrlConfig scriptConfig = ScriptConfiguration.fromRootConfig(config);
    Map<String, Optional<Path>> destinationPaths = new HashMap<>();
    for (String fileKey : ScriptConfiguration.FILE_KEYS) {
      Optional<String> configuredFile = scriptConfig.asString(fileKey).getOptional();
      if (configuredFile.isPresent()) {
        Path destinationPath = copyRelativeFile(rootDir.resolve(configuredFile.get()), rootDir,
            buildDir);
        destinationPaths.put(fileKey,Optional.of(destinationPath));
      }
    }
    return destinationPaths;
  }

  /**
   * Helper function to preprocess files.
   */
  private void preProcessFiles(SqrlConfig config) throws IOException {
    //Preprocessor will normalize files
    List<Preprocessor> processorList = ListUtils.union(List.of(new TablePreprocessor(),
            new JarPreprocessor(), new DataSystemPreprocessor()),
        ServiceLoaderDiscovery.getAll(Preprocessor.class));
    Preprocessors preprocessors = new Preprocessors(processorList, errors);
    preprocessors.handle(
        PreprocessorsContext.builder()
            .rootDir(rootDir)
            .buildDir(buildDir)
            .config(config)
            .build());
  }


  /**
   * Helper function to update package config.
   */
  private void writePackageConfig() throws IOException {
    config.toFile(buildDir.resolve(PACKAGE_FILE_NAME), true);
  }

  //Todo; Something wrong here?
  private void cleanBuildDir() throws IOException {
    if (Files.exists(buildDir) && Files.isDirectory(buildDir)) {
      Files.walk(buildDir)
          // Sort the paths in reverse order so that directories are deleted last
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } else if (Files.exists(buildDir) && !Files.isDirectory(buildDir)) {
      buildDir.toFile().delete();
    }
  }

  private boolean retrieveDependency(Path buildDir, NamePath packagePath, Dependency dependency)
      throws IOException {
    Path targetPath = namepath2Path(buildDir, packagePath);
    Preconditions.checkArgument(FileUtil.isEmptyDirectory(targetPath),
        "Dependency [%s] conflicts with existing module structure in directory: [%s]", dependency,
        targetPath);
    return repository.retrieveDependency(targetPath, dependency);
  }

  public void cleanUp() {
    try {
      Path buildDir = rootDir.resolve(BUILD_DIR_NAME);
      if (Files.exists(buildDir)) {
        FileUtils.deleteDirectory(buildDir.toFile());
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not read or write files on local file-system", e);
    }
  }

  public static Path copyRelativeFile(Path srcFile, Path srcDir, Path destDir) throws IOException {
    return copyFile(srcFile, destDir, srcDir.relativize(srcFile));
  }

  public static Path copyFile(Path srcFile, Path destDir, Path relativeDestPath)
      throws IOException {
    Preconditions.checkArgument(Files.isRegularFile(srcFile), "Is not a file: {}", srcFile);
    Path targetPath = destDir.resolve(relativeDestPath);
    if (!Files.exists(targetPath.getParent())) {
      Files.createDirectories(targetPath.getParent());
    }
    Files.copy(srcFile, targetPath, StandardCopyOption.REPLACE_EXISTING);
    return targetPath;
  }
}
