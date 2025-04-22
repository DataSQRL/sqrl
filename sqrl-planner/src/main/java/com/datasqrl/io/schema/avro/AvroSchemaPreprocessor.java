package com.datasqrl.io.schema.avro;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.preprocessor.PreprocessorBase;
import com.datasqrl.util.StringUtil;
import com.google.common.base.Preconditions;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@NoArgsConstructor
public class AvroSchemaPreprocessor extends PreprocessorBase {


  public static final String AVRO_SCHEMA_REGEX = "(.*)\\.avsc$";

  @Override
  public Pattern getPattern() {
    // Get a pattern for all files with the extension .schema.yml
    return Pattern.compile(AVRO_SCHEMA_REGEX);
  }

  @SneakyThrows
  @Override
  public void processFile(Path file, ProcessorContext processorContext, ErrorCollector errors) {
    Preconditions.checkArgument(Files.isRegularFile(file), "Not a regular file: %s", file);

    //Check if the directory contains a table json file
    String tablename = StringUtil.removeFromEnd(file.getFileName().toString(), AvroTableSchemaFactory.SCHEMA_EXTENSION);
    Path parent = file.getParent() == null ? file.toAbsolutePath().getParent() : file.getParent();
    if (!tableExists(parent, tablename)) {
      errors.warn("No table file [%s] for schema file [%s], hence schema is ignored", tablename, file);
      return;
    }

    processorContext.addDependency(file);
  }

}
