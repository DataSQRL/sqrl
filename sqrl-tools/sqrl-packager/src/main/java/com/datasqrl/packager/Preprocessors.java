package com.datasqrl.packager;

import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.packager.preprocess.Preprocessor.ProcessorContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Slf4j
public class Preprocessors {

  public static final Set<String> EXCLUDED_DIRS = Set.of(Packager.BUILD_DIR_NAME, "deploy");
  List<Preprocessor> preprocessors;
  ErrorCollector errors;

  @SneakyThrows
  public boolean handle(PreprocessorsContext ctx) {
    //For each file, test each preprocessor in order if it matches the regex, if so, call preprocessor
    return processUserFiles(getUserFiles(ctx.rootDir), ctx);
  }

  @SneakyThrows
  private List<Path> getUserFiles(Path rootDir) {
    return Files.walk(rootDir)
        .filter(path -> !EXCLUDED_DIRS.contains(path.getFileName().toString()))
        .filter(Files::isRegularFile)
        .collect(Collectors.toList());
  }

  /**
   * Processes the given list of user files.
   */
  private boolean processUserFiles(List<Path> userFiles, PreprocessorsContext context) {
    for (Path userDir : userFiles) {
      preprocessors.stream()
          .filter(preprocessor -> preprocessor.getPattern().asMatchPredicate()
              .test(userDir.getFileName().toString()))
          .findFirst()
          .ifPresent(preprocessor -> invokePreprocessor(preprocessor, userDir, context));
    }
    return true;
  }

  /**
   * Invokes the given preprocessor and copies relative files.
   */
  private void invokePreprocessor(Preprocessor preprocessor, Path userDir, PreprocessorsContext ctx) {
    ProcessorContext context = new ProcessorContext(ctx.rootDir, ctx.buildDir, ctx.config);
    log.trace("Invoking preprocessor: {}", preprocessor.getClass());
    preprocessor.loader(userDir, context, errors);
    copyRelativeFiles(context.getDependencies(),
        getModulePath(context.getName(), ctx.rootDir, ctx.buildDir, userDir));
    copy(context.getLibraries(), ctx.buildDir);
  }

  private Path getModulePath(Optional<NamePath> name, Path rootDir, Path buildDir, Path userDir) {
    if (name.isPresent()) {
      return namepath2Path(buildDir, name.get());
    }

    Path relDir = rootDir.relativize(userDir);

    //Check if we at the root folder, if so, copy it to the root dir
    if (relDir.getParent() == null) {
      return buildDir;
    }

    return buildDir.resolve(relDir);
  }

  /**
   * Copies the given list of relative files from the given root directory to the given build directory.
   */
  @SneakyThrows
  private void copyRelativeFiles(Set<Path> paths, Path copyDir) {
    for (Path file : paths) {
      copy(file, copyDir);
    }
  }

  @SneakyThrows
  private void copy(Path fileOrDir, Path copyDir) {
      Files.createDirectories(copyDir);
      Path copyPath = copyDir.resolve(fileOrDir.getFileName());
      Files.copy(fileOrDir, copyPath, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Creates a `lib` directory in the buildDir and creates a symlink for each library
   */
  @SneakyThrows
  private void copy(Set<Path> libraries, Path buildDir) {
    if (!libraries.isEmpty()) {
      Path libDir = buildDir.resolve("lib");
      Files.createDirectories(libDir);
      libraries.forEach(library -> copy(library.toAbsolutePath(), libDir));
    }
  }

  @Builder
  public static class PreprocessorsContext {
    Path rootDir;
    Path buildDir;
    SqrlConfig config;
  }
}
