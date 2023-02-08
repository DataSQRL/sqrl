/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import static com.datasqrl.packager.LambdaUtil.rethrowCall;
import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.packager.ImportExportAnalyzer.Result;
import com.datasqrl.packager.Preprocessors.PreprocessorsContext;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.config.GlobalPackageConfiguration;
import com.datasqrl.packager.preprocess.DataSystemPreprocessor;
import com.datasqrl.packager.preprocess.FlexibleSchemaPreprocessor;
import com.datasqrl.packager.preprocess.JarPreprocessor;
import com.datasqrl.packager.preprocess.TablePreprocessor;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.spi.ManifestConfiguration;
import com.datasqrl.util.FileUtil;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiPredicate;
import lombok.NonNull;
import lombok.Setter;
import lombok.Value;
import org.apache.flink.calcite.shaded.org.apache.commons.io.FileUtils;

@Value
public class Packager {

  public static final String BUILD_DIR_NAME = "build";
  public static final String GRAPHQL_SCHEMA_FILE_NAME = "schema.graphqls";
  public static final String PACKAGE_FILE_NAME = "package.json";

  private static final BiPredicate<Path, BasicFileAttributes> FIND_SQLR_SCRIPT = (p, f) ->
      f.isRegularFile() && p.getFileName().toString().toLowerCase().endsWith(".sqrl");

  Repository repository;
  Path rootDir;
  ObjectNode packageConfig;
  GlobalPackageConfiguration config;
  ErrorCollector errors;
  Path buildDir;
  protected static final ObjectMapper mapper = createMapper();

  public Packager(@NonNull Repository repository, @NonNull Path rootDir,
      @NonNull ObjectNode packageConfig, @NonNull GlobalPackageConfiguration config,
      @NonNull ErrorCollector errors) {
    Preconditions.checkArgument(Files.isDirectory(rootDir));
    this.repository = repository;
    this.rootDir = rootDir;
    this.buildDir = rootDir.resolve(BUILD_DIR_NAME);

    this.packageConfig = packageConfig;
    this.config = config;
    this.errors = errors;
  }

  public Path populateBuildDir(boolean inferDependencies) {
    Preconditions.checkArgument(
        config.getManifest() != null && !Strings.isNullOrEmpty(config.getManifest().getMain()),
        "No config or main script specified");
    try {
      cleanBuildDir();
      createBuildDir();
      validateDependencyConfig();
      if (inferDependencies) {
        inferDependencies();
      }
      retrieveDependencies();
      copySystemFilesToBuildDir();
      preProcessFiles();
      updatePackageConfig();
      return buildDir.resolve(PACKAGE_FILE_NAME);
    } catch (IOException e) {
      e.printStackTrace();
      errors.fatal("Could not read or write files on local file-system: %s", e);
      return null;
    }
  }

  private void createBuildDir() throws IOException {
    Files.createDirectories(buildDir);
  }

  /**
   * Helper function to validate dependency config.
   */
  private void validateDependencyConfig() {
    Preconditions.checkArgument(config.getDependencies() != null,
        "No dependency config found");
  }

  private void inferDependencies() throws IOException {
    //Analyze all local SQRL files to discovery transitive or undeclared dependencies
    //At the end, we'll add the new dependencies to the package config.
    LinkedHashMap<String, Dependency> dependencies = new LinkedHashMap<>(
        config.getDependencies());
    ImportExportAnalyzer analyzer = new ImportExportAnalyzer();

    // Find all SQRL script files
    Result allResults = Files.find(rootDir, 128, FIND_SQLR_SCRIPT)
        .map(script -> analyzer.analyze(script, errors))
        .reduce(Result.EMPTY, (r1, r2) -> r1.add(r2));

    Set<NamePath> pkgs = allResults.getPkgs();

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
    dependencies.putAll(inferredDependencies);
    config.setDependencies(dependencies);
  }

  /**
   * Helper function for retrieving listed dependencies.
   */
  private void retrieveDependencies() {
    config.getDependencies().entrySet().stream()
        .map(entry -> rethrowCall(() ->
            retrieveDependency(buildDir, NamePath.parse(entry.getKey()),
                entry.getValue().normalize(entry.getKey()))
                ? Optional.<NamePath>empty()
                : Optional.of(NamePath.parse(entry.getKey()))))
        .flatMap(Optional::stream)
        .forEach(failedDep -> errors.fatal("Could not retrieve dependency: %s", failedDep));
  }

  /**
   * Helper function to copy files to build directory.
   */
  private void copySystemFilesToBuildDir() throws IOException {
    //Copy main script to build
    Path mainFile = copyRelativeFile(
        rootDir.resolve(config.getManifest().getMain()),
        rootDir,
        buildDir);

    //Copy graphql file(s)
    Optional<Path> graphQLSchemaFile = config.getManifest().getOptGraphQL()
        .map(rootDir::resolve)
        .map(gql -> rethrowCall(() -> copyFile(gql, buildDir,
            Path.of(GRAPHQL_SCHEMA_FILE_NAME))));

    relativizeManifest(mainFile, graphQLSchemaFile);
  }

  /**
   * Helper function to relativize manifest.
   */
  private void relativizeManifest(Path mainFile, Optional<Path> graphQLSchemaFile) {
    config.setManifest(PackagerConfig.buildRelativizeManifest(
        buildDir,
        mainFile,
        graphQLSchemaFile));
  }

  /**
   * Helper function to preprocess files.
   */
  private void preProcessFiles() throws IOException {
    //Preprocessor will normalize files
    Preprocessors preprocessors = new Preprocessors(List.of(
        new TablePreprocessor(),
        new JarPreprocessor(),
        new FlexibleSchemaPreprocessor(errors),
        new DataSystemPreprocessor()));
    preprocessors.handle(
        PreprocessorsContext.builder()
            .rootDir(rootDir)
            .buildDir(buildDir)
            .build());
  }


  /**
   * Helper function to update package config.
   */
  private void updatePackageConfig() throws IOException {
    Path packageFile = buildDir.resolve(PACKAGE_FILE_NAME);

    //Update dependencies in place
    packageConfig.set(GlobalPackageConfiguration.DEPENDENCIES_NAME,
        mapper.valueToTree(config.getDependencies()));

    //Update relativized manifest in place
    JsonNode mappedManifest = mapper.valueToTree(config.getManifest());
    packageConfig.set(ManifestConfiguration.MANIFEST, mappedManifest);

    mapper.writeValue(packageFile.toFile(), packageConfig);
  }

  private void cleanBuildDir() throws IOException {
    if (Files.exists(buildDir)) {
      Files.walk(buildDir)
          // Sort the paths in reverse order so that directories are deleted last
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
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

  protected static ObjectMapper createMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    return mapper;
  }
}
