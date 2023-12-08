package com.datasqrl.packager.preprocess;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.util.FileUtil;
import com.google.auto.service.AutoService;
import java.nio.file.Path;
import java.util.regex.Pattern;

@AutoService(Preprocessor.class)
public class PreparsedQueryPreprocessor implements Preprocessor {
  protected static final Pattern QUERY_FILE_REGEX =
      Pattern.compile(".*"+ FileUtil.toRegex(".graphql"));

  @Override
  public Pattern getPattern() {
    // Pattern to match any persisted query files
    return QUERY_FILE_REGEX;
  }

  @Override
  public void loader(Path path, ProcessorContext processorContext, ErrorCollector errors) {
    processorContext.addDependency(path);
  }
}
