package com.datasqrl.packager.preprocess;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.util.FileUtil;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import lombok.SneakyThrows;

public class DataSystemPreprocessor implements Preprocessor {

  public static final Pattern DATASYSTEM_REGEX =
      Pattern.compile(
          ".*"
              + FileUtil.toRegex(DataSource.DATASYSTEM_FILE_PREFIX)
              + ".*"
              + FileUtil.toRegex(DataSource.TABLE_FILE_SUFFIX));

  @Override
  public Pattern getPattern() {
    return DATASYSTEM_REGEX;
  }

  @SneakyThrows
  @Override
  public void processFile(Path dir, ProcessorContext processorContext, ErrorCollector errors) {
    Preconditions.checkArgument(Files.isRegularFile(dir), "Not a regular file: %s", dir);

    processorContext.addDependency(dir);
  }
}
