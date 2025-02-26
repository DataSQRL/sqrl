package com.datasqrl.packager.preprocess;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.FileUtil;
import java.nio.file.Path;
import java.util.regex.Pattern;

public class ScriptPreprocessor implements Preprocessor {
  protected static final Pattern SCRIPT_REGEX = Pattern.compile(".*" + FileUtil.toRegex(".sqrl"));

  @Override
  public Pattern getPattern() {
    return SCRIPT_REGEX;
  }

  @Override
  public void processFile(Path path, ProcessorContext processorContext, ErrorCollector errors) {
    processorContext.addDependency(path);
  }
}
