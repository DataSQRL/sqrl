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

import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.preprocessor.Preprocessor;
import com.datasqrl.packager.preprocessor.Preprocessor.ProcessorContext;
import com.google.inject.Inject;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class Preprocessors {
  public static final Set<String> EXCLUDED_DIRS =
      Set.of(SqrlConstants.BUILD_DIR_NAME, SqrlConstants.DEPLOY_DIR_NAME);

  Set<Preprocessor> preprocessors;

  @SneakyThrows
  public boolean handle(PreprocessorsContext ctx) {
    // For each file, test each preprocessor in order if it matches the regex, if so, call
    // preprocessor
    return processUserFiles(getUserFiles(ctx.rootDir), ctx);
  }

  @SneakyThrows
  private List<Path> getUserFiles(Path rootDir) {
    return Files.walk(rootDir, FileVisitOption.FOLLOW_LINKS)
        .filter(path -> !EXCLUDED_DIRS.contains(path.getFileName().toString()))
        .filter(Files::isRegularFile)
        .collect(Collectors.toList());
  }

  /** Processes the given list of user files. */
  protected boolean processUserFiles(List<Path> userFiles, PreprocessorsContext context) {
    for (Path userDir : userFiles) {
      preprocessors.stream()
          .filter(
              preprocessor ->
                  preprocessor
                      .getPattern()
                      .asMatchPredicate()
                      .test(userDir.getFileName().toString()))
          .forEach(preprocessor -> invokePreprocessor(preprocessor, userDir, context));
    }
    return true;
  }

  /** Invokes the given preprocessor and copies relative files. */
  protected void invokePreprocessor(
      Preprocessor preprocessor, Path userDir, PreprocessorsContext ctx) {
    var context = new ProcessorContext(ctx.rootDir, ctx.buildDir, ctx.config);
    log.trace("Invoking preprocessor: {}", preprocessor.getClass());
    preprocessor.processFile(userDir, context, ctx.errors);
    copyRelativeFiles(
        context.getDependencies(),
        getModulePath(context.getName(), ctx.rootDir, ctx.buildDir, userDir));
    copy(context.getLibraries(), ctx.buildDir);
  }

  private Path getModulePath(Optional<NamePath> name, Path rootDir, Path buildDir, Path userDir) {
    if (name.isPresent()) {
      return namepath2Path(buildDir, name.get());
    }

    var relDir = rootDir.relativize(userDir);

    // Check if we at the root folder, if so, copy it to the root dir
    if (relDir.getParent() == null) {
      return buildDir;
    }

    return buildDir.resolve(relDir.getParent());
  }

  /**
   * Copies the given list of relative files from the given root directory to the given build
   * directory.
   */
  @SneakyThrows
  private void copyRelativeFiles(Set<Path> paths, Path copyDir) {
    for (Path file : paths) {
      copy(file, copyDir);
    }
  }

  @SneakyThrows
  private void copy(Path fileOrDir, Path copyDir) {
    var copyPath = copyDir.resolve(fileOrDir.getFileName());
    Files.createDirectories(copyPath.getParent());
    Files.copy(fileOrDir, copyPath, StandardCopyOption.REPLACE_EXISTING);
  }

  /** Creates a `lib` directory in the buildDir and creates a symlink for each library */
  @SneakyThrows
  private void copy(Set<Path> libraries, Path buildDir) {
    if (!libraries.isEmpty()) {
      Path libDir = buildDir.resolve(BuildPath.UDF_DIR);
      Files.createDirectories(libDir);
      libraries.forEach(library -> copy(library.toAbsolutePath(), libDir));
    }
  }

  @Builder
  public static class PreprocessorsContext {
    Path rootDir;
    Path buildDir;
    // The user specified config
    PackageJson config;
    String[] profiles;
    ErrorCollector errors;
  }
}
