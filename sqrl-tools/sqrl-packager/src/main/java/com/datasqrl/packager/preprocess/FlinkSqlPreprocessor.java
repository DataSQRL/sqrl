package com.datasqrl.packager.preprocess;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.FileUtil;
import java.nio.file.Path;
import java.util.regex.Pattern;
import lombok.SneakyThrows;

/** Creates a package json based on given profiles and explicit package json references */
public class FlinkSqlPreprocessor implements Preprocessor {

  public static final Pattern DATASYSTEM_REGEX = Pattern.compile(".*" + FileUtil.toRegex(".sql"));

  @Override
  public Pattern getPattern() {
    return DATASYSTEM_REGEX;
  }

  @SneakyThrows
  @Override
  public void processFile(Path file, ProcessorContext processorContext, ErrorCollector errors) {

    processorContext.addDependency(file);
  }
}
