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

import static com.datasqrl.packager.Packager.canonicalizePath;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.TableSchemaFactory;
import com.datasqrl.packager.preprocess.CopyStaticDataPreprocessor;
import com.datasqrl.packager.preprocess.JarPreprocessor;
import com.datasqrl.packager.preprocess.NewPreprocessor;
import com.datasqrl.util.FilenameAnalyzer;
import com.datasqrl.util.NameUtil;
import com.google.common.base.Preconditions;
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
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

public class PreprocessorOrchestrator {

  public static final List<NewPreprocessor> DEFAULT_PREPROCESSORS =
      List.of(new CopyStaticDataPreprocessor(), new JarPreprocessor());

  private final PackageJson packageJson;
  private final List<NewPreprocessor> preprocessors;
  private final BuildPath buildPath;

  @Inject
  public PreprocessorOrchestrator(PackageJson packageJson, BuildPath buildPath) {
    this.packageJson = packageJson;
    this.buildPath = buildPath;
    this.preprocessors = DEFAULT_PREPROCESSORS;
  }

  private Set<String> getCopyExtensions() {
    Set<String> copyExtensions = new HashSet<>(TableSchemaFactory.factoriesByExtension().keySet());
    copyExtensions.add(SqrlConstants.SQRL_EXTENSION);
    copyExtensions.add(SqrlConstants.SQL_EXTENSION);
    copyExtensions.add(SqrlConstants.GRAPHQL_EXTENSION);
    copyExtensions.add(SqrlConstants.GRAPHQL_SCHEMA_EXTENSION);
    return copyExtensions;
  }

  public void preprocessDirectory(Path sourceDir, ErrorCollector errors) throws IOException {
    preprocessDirectory(sourceDir, NamePath.ROOT, errors);
  }

  public void preprocessDirectory(Path sourceDir, NamePath namePath, ErrorCollector errors)
      throws IOException {
    Path buildDir = NameUtil.namepath2Path(buildPath.getBuildDir(), namePath);
    Context context =
        new Context(sourceDir, buildDir, buildPath.getUdfPath(), buildPath.getDataPath(), errors);
    FilenameAnalyzer fileAnalyzer = FilenameAnalyzer.of(getCopyExtensions());
    Files.walkFileTree(
        sourceDir,
        EnumSet.of(FileVisitOption.FOLLOW_LINKS),
        Integer.MAX_VALUE,
        new SimpleFileVisitor<Path>() {

          private Context localContext = context;

          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            if (dir.getFileName().toString().equalsIgnoreCase(SqrlConstants.BUILD_DIR_NAME))
              return FileVisitResult.SKIP_SUBTREE;
            localContext = context.getSubContext(dir);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            if (Files.isRegularFile(file)) {
              if (fileAnalyzer.analyze(file).isPresent()) {
                context.copy2build(file);
              }
              preprocessors.stream().forEach(p -> p.process(file, localContext));
            }
            return FileVisitResult.CONTINUE;
          }
        });
  }

  @AllArgsConstructor
  public class Context {

    Path sourceDir;
    Path buildDir;
    Path libDir;
    Path dataDir;
    @Getter ErrorCollector errorCollector;

    private Path getRelativeBuildPath(Path sourcePath) {
      Preconditions.checkArgument(Files.isDirectory(sourcePath));
      Path absoluteFile = sourcePath.toAbsolutePath().normalize();
      Path absoluteSourceDir = sourceDir.toAbsolutePath().normalize();
      if (!absoluteFile.startsWith(absoluteSourceDir)) {
        throw new IllegalArgumentException("File is not under the source directory: " + sourcePath);
      }
      Path relativePath = absoluteSourceDir.relativize(absoluteFile);
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

    public void copy2build(Path file) {
      var destination = getRelativeBuildPath(file.getParent());
      copy(file, destination);
    }

    public PackageJson getPackageJson() {
      return packageJson;
    }

    public Path createNewDataFile(Path file) {
      Path result = dataDir.resolve(file.getFileName());
      ensureDirectoryExists(result.getParent());
      return result;
    }

    public void copy2data(Path file) {
      copy(file, dataDir);
    }

    public void copy2lib(Path file) {
      copy(file, libDir);
    }

    @SneakyThrows
    private void copy(Path file, Path toDir) {
      Path copyPath = toDir.resolve(file.getFileName());
      copyPath = canonicalizePath(copyPath);
      ensureDirectoryExists(toDir);
      Files.copy(file, copyPath, StandardCopyOption.REPLACE_EXISTING);
    }

    @SneakyThrows
    private void ensureDirectoryExists(Path dir) {
      if (!Files.isDirectory(dir)) {
        Files.createDirectories(dir);
      }
    }
  }
  ;
}
