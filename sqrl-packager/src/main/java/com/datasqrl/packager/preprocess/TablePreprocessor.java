package com.datasqrl.packager.preprocess;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.SneakyThrows;

public class TablePreprocessor implements Preprocessor {
  protected static final Pattern TABLE_FILE_REGEX = Pattern.compile(".*\\.table\\.json");
  protected static final String CLASS_NAME = "com.datasqrl.loaders.TableLoader";
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public Pattern getPattern() {
    // Pattern to match *.table.json files
    return TABLE_FILE_REGEX;
  }

  @Override
  public void loader(Path path, ProcessorContext processorContext) {
    processorContext.addDependency(path);
//    Path manifestPath = createManifestFile(path.toFile(), processorContext);
//    addDependencies(path, manifestPath, processorContext);
  }

  @SneakyThrows
  private Path createManifestFile(File file, ProcessorContext processorContext) {
    ObjectNode manifest = OBJECT_MAPPER.createObjectNode();
    manifest.put("className", CLASS_NAME);
    manifest.put("file", file.getName());

    //todo: detect the schema type, add a new schema loader if necessary
    manifest.put("schemaFactory", "com.datasqrl.schema.input.FlexibleTableSchemaFactory");

    Path tempFile = Files.createTempFile(file.getName() + ".", ".manifest.json");
    OBJECT_MAPPER.writeValue(tempFile.toFile(), manifest);
    return tempFile;
  }

  private void addDependencies(Path sourcePath, Path manifestPath, ProcessorContext processorContext) {
    // Add the *.table.json file as a path to the dependency list.
    processorContext.addDependency(sourcePath);
    // Also add the table.manifest.json file.
    processorContext.addDependency(manifestPath);
  }
}
