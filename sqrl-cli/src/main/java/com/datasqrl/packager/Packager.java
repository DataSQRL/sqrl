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
package com.datasqrl.packager;

import static com.datasqrl.config.ScriptConfigImpl.GRAPHQL_KEY;
import static com.datasqrl.config.ScriptConfigImpl.MAIN_KEY;
import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.DependenciesConfigImpl;
import com.datasqrl.config.Dependency;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.ScriptConfig;
import com.datasqrl.config.RootPath;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.EnginePhysicalPlan.DeploymentArtifact;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlan.PhysicalStagePlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Preprocessors.PreprocessorsContext;
import com.datasqrl.plan.MainScript;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

@Getter
@AllArgsConstructor(onConstructor_ = @Inject)
public class Packager {

  private final RootPath rootDir;
  private final PackageJson config;
  private final BuildPath buildDir;
  private final Preprocessors preprocessors;
  private final ImportExportAnalyzer analyzer;
  private final MainScript mainScript;

  public void preprocess(ErrorCollector errors) {
    errors.checkFatal(
        config.getScriptConfig().getMainScript().map(StringUtils::isNotBlank).orElse(false),
        "No config or main script specified");
    try {
      cleanBuildDir(buildDir.getBuildDir());
      createBuildDir(buildDir.getBuildDir());
      retrieveDependencies(errors);
      copyFilesToBuildDir(errors);
      preProcessFiles(config, errors);
      writePackageConfig();
    } catch (IOException e) {
      throw errors.handle(e);
    }
  }

  @SneakyThrows
  public static void createBuildDir(Path buildDir) {
    Files.createDirectories(buildDir);
  }

  /** Helper function for retrieving listed dependencies. */
  private void retrieveDependencies(ErrorCollector errors) {
    var depErrors = errors.resolve(DependenciesConfigImpl.DEPENDENCIES_KEY);
    retrieveDependencies(config.getDependencies().getDependencies(), depErrors)
        .forEach(failedDep -> depErrors.fatal("Could not retrieve dependency: %s", failedDep));
  }

  @SneakyThrows
  private Stream<NamePath> retrieveDependencies(
      Map<String, ? extends Dependency> dependencies, ErrorCollector errors) {
    List<Optional<NamePath>> deps = new ArrayList<>();
    for (Map.Entry<String, ? extends Dependency> entry : dependencies.entrySet()) {
      Optional<NamePath> namePath =
          retrieveDependency(
                  rootDir.getRootDir(),
                  buildDir.getBuildDir(),
                  NamePath.parse(entry.getKey()),
                  entry.getValue().normalize(entry.getKey(), errors))
              ? Optional.empty()
              : Optional.of(NamePath.parse(entry.getKey()));
      deps.add(namePath);
    }

    return deps.stream().flatMap(Optional::stream);
  }

  private void copyFilesToBuildDir(ErrorCollector errors) throws IOException {
    Map<String, Optional<Path>> destinationPaths = copyScriptFilesToBuildDir();
    // Files should exist, if error occurs its internal, hence we create root error collector
    addFileToPackageJsonConfig(
        buildDir.getBuildDir(), config.getScriptConfig(), destinationPaths, errors);
  }

  public static void addFileToPackageJsonConfig(
      Path rootDir,
      ScriptConfig scriptConfig,
      Map<String, Optional<Path>> filesByKey,
      ErrorCollector errors) {
    filesByKey.forEach(
        (key, file) -> {
          if (file.isPresent()) {
            errors.checkFatal(
                Files.isRegularFile(file.get()), "Could not locate %s file: %s", key, file.get());
            var normalizedPath = rootDir.relativize(file.get()).normalize().toString();
            if (key.equals(MAIN_KEY)) {
              scriptConfig.setMainScript(normalizedPath);
            } else if (key.equals(GRAPHQL_KEY)) {
              scriptConfig.setGraphql(normalizedPath);
            }
          }
        });
  }

  /**
   * Copies all the files in the script configuration section of the config to the build dir and
   * either normalizes the file or preserves the relative path.
   *
   * @throws IOException
   */
  private Map<String, Optional<Path>> copyScriptFilesToBuildDir() throws IOException {
    var scriptConfig = config.getScriptConfig();
    Map<String, Optional<Path>> destinationPaths = new HashMap<>();
    if (scriptConfig.getMainScript().isPresent()) {
      var destinationPath =
          copyRelativeFile(
              rootDir.getRootDir().resolve(scriptConfig.getMainScript().get()),
              rootDir.getRootDir(),
              buildDir.getBuildDir());
      destinationPaths.put(MAIN_KEY, Optional.of(destinationPath));
    }
    if (scriptConfig.getGraphql().isPresent()) {
      var destinationPath =
          copyRelativeFile(
              rootDir.getRootDir().resolve(scriptConfig.getGraphql().get()),
              rootDir.getRootDir(),
              buildDir.getBuildDir());
      destinationPaths.put(GRAPHQL_KEY, Optional.of(destinationPath));
    }
    for (String operationFile : scriptConfig.getOperations()) {
      var destinationPath =
          copyRelativeFile(
              rootDir.getRootDir().resolve(operationFile),
              rootDir.getRootDir(),
              buildDir.getBuildDir());
    }
    return destinationPaths;
  }

  /** Helper function to preprocess files. */
  private void preProcessFiles(PackageJson config, ErrorCollector errors) throws IOException {
    // Preprocessor will normalize files
    preprocessors.handle(
        PreprocessorsContext.builder()
            .rootDir(rootDir.getRootDir())
            .buildDir(buildDir.getBuildDir())
            .config(config)
            .errors(errors)
            .build());
  }

  private void writePackageConfig() throws IOException {
    config.toFile(buildDir.getBuildDir().resolve(SqrlConstants.PACKAGE_JSON), true);
  }

  public static void cleanBuildDir(Path buildDir) throws IOException {
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

  private boolean retrieveDependency(
      Path rootDir, Path buildDir, NamePath packagePath, Dependency dependency) throws IOException {
    var targetPath = namepath2Path(buildDir, packagePath);
    Preconditions.checkArgument(
        FileUtil.isEmptyDirectory(targetPath),
        "Dependency [%s] conflicts with existing module structure in directory: [%s]",
        dependency,
        targetPath);

    // Determine the directory in the root that corresponds to the dependency's folder
    var depFolder = dependency.getFolder();
    var sourcePath = namepath2Path(rootDir, NamePath.parse(depFolder));

    // Check if the source directory exists and is indeed a directory
    if (Files.isDirectory(sourcePath)) {
      // Copy the entire directory from source to target
      Files.walkFileTree(
          sourcePath,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                throws IOException {
              var targetDir = targetPath.resolve(sourcePath.relativize(dir));
              Files.createDirectories(targetDir);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Files.copy(
                  file,
                  targetPath.resolve(sourcePath.relativize(file)),
                  StandardCopyOption.REPLACE_EXISTING);
              return FileVisitResult.CONTINUE;
            }
          });
      return true;
    } else {
      // If the directory does not exist or is not a directory, then dependency is not available
      return false;
    }
  }

  public static Path copyRelativeFile(Path srcFile, Path srcDir, Path destDir) throws IOException {
    return copyFile(srcFile, destDir, srcDir.relativize(srcFile));
  }

  public static Path copyFile(Path srcFile, Path destDir, Path relativeDestPath)
      throws IOException {
    Preconditions.checkArgument(Files.isRegularFile(srcFile), "Is not a file: %s", srcFile);
    var targetPath = destDir.resolve(relativeDestPath);
    if (!Files.exists(targetPath.getParent())) {
      Files.createDirectories(targetPath.getParent());
    }
    Files.copy(srcFile, targetPath, StandardCopyOption.REPLACE_EXISTING);
    return targetPath;
  }

  @SneakyThrows
  public void postprocess(Path targetDir, PhysicalPlan plan, TestPlan testPlan) {
    Path planDir = targetDir.resolve(SqrlConstants.PLAN_DIR);
    Files.createDirectories(planDir);
    // We'll write a single asset for each folder in the physical plan stage, plus any deployment
    // artifacts that the plan has
    for (PhysicalStagePlan stagePlan : plan.getStagePlans()) {
      writePlan(stagePlan.getStage().name(), stagePlan.getPlan(), planDir);
    }

    if (testPlan != null) {
      Path path = planDir.resolve("test.json");
      SqrlObjectMapper.INSTANCE
          .writerWithDefaultPrettyPrinter()
          .writeValue(path.toFile(), testPlan);
    }

    copyDataFiles(buildDir.getBuildDir());
    moveFolder(targetDir, SqrlConstants.DATA_DIR);
    copyJarFiles(buildDir.getBuildDir());
    moveFolder(targetDir, SqrlConstants.LIB_DIR);
  }

  private void copyDataFiles(Path buildDir) throws IOException {
    Files.walk(buildDir)
        .filter(
            path ->
                (path.toString().endsWith(".jsonl") || path.toString().endsWith(".csv"))
                    && !Files.isDirectory(path))
        .filter(path -> !path.startsWith(buildDir.resolve(SqrlConstants.DATA_DIR)))
        .forEach(
            path -> {
              try {
                var destination =
                    buildDir.resolve(SqrlConstants.DATA_DIR).resolve(path.getFileName());
                destination.toFile().mkdirs();
                Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
              } catch (IOException e) {
                e.printStackTrace();
              }
            });
  }

  private void copyJarFiles(Path buildDir) throws IOException {
    Files.walk(buildDir)
        .filter(path -> path.toString().endsWith(".jar") && !Files.isDirectory(path))
        .filter(path -> !path.startsWith(buildDir.resolve(SqrlConstants.LIB_DIR)))
        .forEach(
            path -> {
              try {
                var destination =
                    buildDir.resolve(SqrlConstants.LIB_DIR).resolve(path.getFileName());
                // Ensure the parent directories exist
                Files.createDirectories(destination.getParent());
                Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
              } catch (IOException e) {
                e.printStackTrace();
              }
            });
  }

  @SneakyThrows
  private void moveFolder(Path targetDir, String folderName) {
    Path targetPath = targetDir.resolve("flink").resolve(folderName);
    Files.createDirectories(targetPath);
    Path sourcePath = buildDir.getBuildDir().resolve(folderName);
    if (!Files.isDirectory(sourcePath)) {
      return;
    }
    // Move each file individually, replacing existing files
    try (Stream<Path> stream = Files.walk(sourcePath)) {
      stream.forEach(
          sourceFile -> {
            try {
              Path relativePath = sourcePath.relativize(sourceFile);
              Path targetFile = targetPath.resolve(relativePath);
              if (Files.isDirectory(sourceFile)) {
                Files.createDirectories(targetFile);
              } else {
                Files.createDirectories(targetFile.getParent());
                Files.move(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
              }
            } catch (IOException e) {
              throw new RuntimeException("Error moving file: " + sourceFile, e);
            }
          });
    }
  }

  @SneakyThrows
  private void writePlan(String name, EnginePhysicalPlan plan, Path planDir) {
    Files.createDirectories(planDir);

    DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
    prettyPrinter.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
    ObjectWriter jsonWriter =
        SqrlObjectMapper.INSTANCE.enable(SerializationFeature.INDENT_OUTPUT).writer(prettyPrinter);

    var artifacts =
        ListUtils.union(
            plan.getDeploymentArtifacts(), List.of(new DeploymentArtifact(".json", plan)));
    for (DeploymentArtifact artifact : artifacts) {
      Path filePath = planDir.resolve(name + artifact.fileSuffix()).toAbsolutePath();
      if (artifact.content() instanceof String) {
        Files.writeString(
            filePath,
            (String) artifact.content(),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE);
      } else { // serialize as json
        jsonWriter.writeValue(filePath.toFile(), plan);
      }
    }
  }

  public static Optional<List<Path>> findPackageFile(Path rootDir, List<Path> packageFiles) {
    if (packageFiles.isEmpty()) {
      var defaultPkg = rootDir.resolve(SqrlConstants.DEFAULT_PACKAGE);
      if (Files.isRegularFile(defaultPkg)) {
        return Optional.of(List.of(defaultPkg));
      } else {
        return Optional.empty();
      }
    } else {
      return Optional.of(packageFiles.stream().map(rootDir::resolve).toList());
    }
  }
}
