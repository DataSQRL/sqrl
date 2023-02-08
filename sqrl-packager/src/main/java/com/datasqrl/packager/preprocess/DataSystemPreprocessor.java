package com.datasqrl.packager.preprocess;

import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import lombok.SneakyThrows;

public class DataSystemPreprocessor implements Preprocessor {

  public static final String DATASYSTEM_REGEX = "datasystem.json";

  @Override
  public Pattern getPattern() {
    return Pattern.compile(DATASYSTEM_REGEX);
  }

  @SneakyThrows
  @Override
  public void loader(Path dir, ProcessorContext processorContext) {
    Preconditions.checkArgument(Files.isRegularFile(dir), "Not a regular file: %s", dir);

    processorContext.addDependency(dir);
  }
}
