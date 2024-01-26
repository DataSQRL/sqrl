package com.datasqrl.packager.preprocess;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.util.FileUtil;
import com.google.common.base.Preconditions;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;

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
  public void processFile(Path dir, ProcessorContext processorContext, ErrorCollector errors) {
    // Ignore root package json
    if (dir.getParent().equals(processorContext.rootDir)) {
      return;
    }

    for (Path path : Files.newDirectoryStream(dir.getParent(), "*")) {
      processorContext.addDependency(path);
    }
  }
}
