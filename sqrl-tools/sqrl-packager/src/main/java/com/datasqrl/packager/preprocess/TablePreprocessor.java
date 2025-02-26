package com.datasqrl.packager.preprocess;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.util.FileUtil;
import java.nio.file.Path;
import java.util.regex.Pattern;

public class TablePreprocessor implements Preprocessor {
  protected static final Pattern TABLE_FILE_REGEX =
      Pattern.compile(".*" + FileUtil.toRegex(DataSource.TABLE_FILE_SUFFIX));

  @Override
  public Pattern getPattern() {
    // Pattern to match *.table.json files
    return TABLE_FILE_REGEX;
  }

  @Override
  public void processFile(Path path, ProcessorContext processorContext, ErrorCollector errors) {
    processorContext.addDependency(path);
  }
}
