package com.datasqrl.packager.preprocess;

import static com.datasqrl.packager.preprocess.TablePreprocessor.TABLE_FILE_REGEX;

import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor
public class FlexibleSchemaPreprocessor implements Preprocessor {

  public static final String SCHEMA_YML_REGEX = "(.*\\.schema\\.yml|schema\\.yml)$";

  public ErrorCollector errors;

  @Override
  public Pattern getPattern() {
    // Get a pattern for all files with the extension .schema.yml
    return Pattern.compile(SCHEMA_YML_REGEX);
  }

  @SneakyThrows
  @Override
  public void loader(Path dir, ProcessorContext processorContext) {
    Preconditions.checkArgument(Files.isRegularFile(dir), "Not a regular file: %s", dir);

    boolean hasTableJson = Files.list(dir.getParent())
        .anyMatch(path -> TABLE_FILE_REGEX.asMatchPredicate().test(path.getFileName().toString()));

    // If the directory does not contain a table json file
    if (!hasTableJson) {
      errors.warn("No file with pattern %s found in directory: %s", TABLE_FILE_REGEX, dir.getParent());
      return;
    }

    processorContext.addDependency(dir);
  }
}
