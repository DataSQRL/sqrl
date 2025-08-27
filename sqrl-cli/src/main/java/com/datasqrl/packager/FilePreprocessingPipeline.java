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

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.TableSchemaFactory;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.util.FilenameAnalyzer;
import com.datasqrl.util.NameUtil;
import com.google.inject.Inject;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * Helps to preprocess files which means 1) copying all relevant files into the build directory
 * (preserving relative paths) 2) running all registered preprocessors for more elaborate
 * preprocessing than just copying files.
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class FilePreprocessingPipeline {

  private final BuildPath buildPath;
  private final Set<Preprocessor> preprocessors;

  private Set<String> getCopyExtensions() {
    Set<String> copyExtensions = new HashSet<>(TableSchemaFactory.factoriesByExtension().keySet());
    copyExtensions.add(SqrlConstants.SQRL_EXTENSION);
    copyExtensions.add(SqrlConstants.SQL_EXTENSION);
    copyExtensions.add(SqrlConstants.GRAPHQL_EXTENSION);
    copyExtensions.add(SqrlConstants.GRAPHQL_SCHEMA_EXTENSION);
    return copyExtensions;
  }

  public void run(Path sourceDir, ErrorCollector errors) throws IOException {
    run(sourceDir, NamePath.ROOT, errors);
  }

  public void run(Path sourceDir, NamePath namePath, ErrorCollector errors) throws IOException {
    Path buildDir = NameUtil.namepath2Path(buildPath.buildDir(), namePath);
    Context context =
        new Context(sourceDir, buildDir, buildPath.getUdfPath(), buildPath.getDataPath(), errors);
    FilenameAnalyzer fileAnalyzer = FilenameAnalyzer.of(getCopyExtensions());
    Files.walkFileTree(
        sourceDir,
        EnumSet.of(FileVisitOption.FOLLOW_LINKS),
        Integer.MAX_VALUE,
        new SimpleFileVisitor<>() {

          private Context localContext = context;

          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            if (dir.startsWith(buildDir)) {
              return FileVisitResult.SKIP_SUBTREE;
            }
            localContext = context.getSubContext(dir);

            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (Files.isRegularFile(file)) {
              if (fileAnalyzer.analyze(file).isPresent()) {
                context.copyToBuild(file);
              }
              preprocessors.forEach(p -> p.process(file, localContext));
            }
            return FileVisitResult.CONTINUE;
          }
        });
  }

  @AllArgsConstructor
  public static class Context {

    Path sourceDir;
    Path buildDir;
    Path libDir;
    Path dataDir;
    @Getter ErrorCollector errorCollector;

    private Path getRelativeBuildPath(Path srcPath) {
      checkArgument(Files.isDirectory(srcPath));
      var absolutePath = srcPath.toAbsolutePath().normalize();
      var absoluteSourceDir = sourceDir.toAbsolutePath().normalize();
      if (!absolutePath.startsWith(absoluteSourceDir)) {
        throw new IllegalArgumentException("File is not under the source directory: " + srcPath);
      }
      Path relativePath = absoluteSourceDir.relativize(absolutePath);
      return buildDir.resolve(relativePath);
    }

    @SneakyThrows
    public Path createNewBuildFile(Path relativeFilePath) {
      Path result = buildDir.resolve(relativeFilePath);
      ensureDirectoryExists(result.getParent());
      return result;
    }

    public Context getSubContext(Path newSourceDir) {
      return new Context(
          newSourceDir, getRelativeBuildPath(newSourceDir), libDir, dataDir, errorCollector);
    }

    public void copyToBuild(Path file) {
      var destination = getRelativeBuildPath(file.getParent());
      copy(file, destination);
    }

    public Path createNewDataFile(Path file) {
      Path result = dataDir.resolve(file.getFileName());
      ensureDirectoryExists(result.getParent());
      return result;
    }

    public void copyToData(Path file) {
      copy(file, dataDir);
    }

    public void copyToLib(Path file) {
      copy(file, libDir);
    }

    @SneakyThrows
    private void copy(Path file, Path targetDir) {
      var copyPath = targetDir.resolve(file.getFileName());
      ensureDirectoryExists(targetDir);
      Files.copy(file, copyPath, StandardCopyOption.REPLACE_EXISTING);
    }

    @SneakyThrows
    private void ensureDirectoryExists(Path dir) {
      if (!Files.isDirectory(dir)) {
        Files.createDirectories(dir);
      }
    }
  }
}
