package com.datasqrl.packager.preprocess;

import com.datasqrl.error.ErrorCollector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import lombok.SneakyThrows;

/**
 * Creates a package json based on given profiles and explicit package json references
 */
public class PackageJsonPreprocessor implements Preprocessor {

  public static final Pattern DATASYSTEM_REGEX =  Pattern.compile("package.json");

  @Override
  public Pattern getPattern() {
    return DATASYSTEM_REGEX;
  }

  @SneakyThrows
  @Override
  public void processFile(Path file, ProcessorContext processorContext, ErrorCollector errors) {
    // Ignore root package json
    if (file.getParent() == null || file.getParent().equals(processorContext.rootDir)) {
      return;
    }

    for (Path path : Files.newDirectoryStream(file.getParent(), "*")) {
      processorContext.addDependency(path);
    }
  }
}
