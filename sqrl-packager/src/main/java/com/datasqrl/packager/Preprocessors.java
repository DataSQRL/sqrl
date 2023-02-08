package com.datasqrl.packager;

import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.packager.preprocess.Preprocessor.ProcessorContext;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;

@Value
public class Preprocessors {

  public static final Set<String> EXCLUDED_DIRS = Set.of(Packager.BUILD_DIR_NAME, "deploy");
  private List<Preprocessor> preprocessors;

  @SneakyThrows
  public boolean handle(PreprocessorsContext ctx) {
    //Walk file tree and collect list of files
    List<Path> userFiles = getUserFiles(ctx.rootDir);

    //For each file, test each preprocessor in order if it matches the regex, if so, call preprocessor
    processUserFiles(userFiles, ctx.rootDir, ctx.buildDir);

    return true;
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
  private void processUserFiles(List<Path> userFiles, Path rootDir, Path buildDir) {
    for (Path userDir : userFiles) {
      preprocessors.stream()
          .filter(preprocessor -> preprocessor.getPattern().asMatchPredicate()
              .test(userDir.getFileName().toString()))
          .findFirst()
          .ifPresent(preprocessor -> invokePreprocessor(preprocessor, userDir, rootDir, buildDir));
    }
  }

  /**
   * Invokes the given preprocessor and copies relative files.
   */
  private void invokePreprocessor(Preprocessor preprocessor, Path userDir, Path rootDir, Path buildDir) {
    ProcessorContext context = new ProcessorContext(rootDir, buildDir);
    preprocessor.loader(userDir, context);
    copyRelativeFiles(context.getDependencies(), rootDir, buildDir, userDir);
  }

  /**
   * Copies the given list of relative files from the given root directory to the given build directory.
   */
  @SneakyThrows
  private void copyRelativeFiles(Set<Path> relativeFiles, Path rootDir, Path buildDir, Path userFile) {
    Path relativeDir = rootDir.relativize(userFile.getParent());
    Path copyDir = buildDir.resolve(relativeDir);

    for (Path file : relativeFiles) {
      Preconditions.checkArgument(Files.isRegularFile(file), "Is not a file: {}", file);
      Path copyPath = copyDir.resolve(file.getFileName());
      Files.createDirectories(copyDir);
      Files.copy(file, copyPath, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  @Builder
  public static class PreprocessorsContext {

    Path rootDir;
    Path buildDir;
  }
}
