package com.datasqrl.packager.preprocess;

import java.nio.file.Path;
import java.util.regex.Pattern;

public class TablePreprocessor implements Preprocessor {
  protected static final Pattern TABLE_FILE_REGEX = Pattern.compile(".*\\.table\\.json");

  @Override
  public Pattern getPattern() {
    // Pattern to match *.table.json files
    return TABLE_FILE_REGEX;
  }

  @Override
  public void loader(Path path, ProcessorContext processorContext) {
    processorContext.addDependency(path);
  }
}
