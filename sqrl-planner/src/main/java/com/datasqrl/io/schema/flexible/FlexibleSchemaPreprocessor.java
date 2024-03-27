package com.datasqrl.io.schema.flexible;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.packager.preprocessor.PreprocessorBase;
import com.datasqrl.util.StringUtil;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

@NoArgsConstructor
@AutoService(Preprocessor.class)
public class FlexibleSchemaPreprocessor extends PreprocessorBase {

  public static final String SCHEMA_YML_REGEX = "(.*)\\.schema\\.yml$";

  @Override
  public Pattern getPattern() {
    // Get a pattern for all files with the extension .schema.yml
    return Pattern.compile(SCHEMA_YML_REGEX);
  }

  @SneakyThrows
  @Override
  public void processFile(Path file, ProcessorContext processorContext, ErrorCollector errors) {
    Preconditions.checkArgument(Files.isRegularFile(file), "Not a regular file: %s", file);

    //Check if the directory contains a table json file
    String tablename = StringUtil.removeFromEnd(file.getFileName().toString(), FlexibleTableSchemaFactory.SCHEMA_EXTENSION);
    if (!tableExists(file.getParent(), tablename)) {
      errors.warn("No table file [%s] for schema file [%s], hence schema is ignored", tablename, file);
      return;
    }

    processorContext.addDependency(file);
  }
}
