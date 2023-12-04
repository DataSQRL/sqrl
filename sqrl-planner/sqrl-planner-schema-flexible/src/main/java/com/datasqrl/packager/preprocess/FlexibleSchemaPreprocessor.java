package com.datasqrl.packager.preprocess;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.util.NameUtil;
import com.datasqrl.util.StringUtil;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

@NoArgsConstructor
@AutoService(Preprocessor.class)
public class FlexibleSchemaPreprocessor implements Preprocessor {

  public static final String SCHEMA_YML_REGEX = "(.*)\\.schema\\.yml$";

  @Override
  public Pattern getPattern() {
    // Get a pattern for all files with the extension .schema.yml
    return Pattern.compile(SCHEMA_YML_REGEX);
  }

  @SneakyThrows
  @Override
  public void loader(Path file, ProcessorContext processorContext, ErrorCollector errors) {
    Preconditions.checkArgument(Files.isRegularFile(file), "Not a regular file: %s", file);

    //Check if the directory contains a table json file
    String tablename = StringUtil.removeFromEnd(file.getFileName().toString(), FlexibleTableSchemaFactory.SCHEMA_EXTENSION);
    if (!Preprocessor.tableExists(file.getParent(), tablename)) {
      errors.warn("No table file [%s] for schema file [%s], hence schema is ignored", tablename, file);
      return;
    }

    processorContext.addDependency(file);
  }
}
