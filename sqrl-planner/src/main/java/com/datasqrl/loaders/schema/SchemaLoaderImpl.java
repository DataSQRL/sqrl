package com.datasqrl.loaders.schema;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.SchemaConversionResult;
import com.datasqrl.io.schema.TableSchemaFactory;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.util.BaseFileUtil;
import com.datasqrl.util.FilenameAnalyzer;
import com.datasqrl.util.StringUtil;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SchemaLoaderImpl implements SchemaLoader {

  private final ResourceResolver resourceResolver;
  private final ErrorCollector errors;

  @Override
  public Optional<SchemaConversionResult> loadSchema(
      String tableName, String relativeSchemaFilePath) {
    Map<String, TableSchemaFactory> schemaFactories = TableSchemaFactory.factoriesByExtension();
    FilenameAnalyzer fileAnalyzer = FilenameAnalyzer.of(schemaFactories.keySet());
    var fileComponentsOpt = fileAnalyzer.analyze(relativeSchemaFilePath);
    if (fileComponentsOpt.isPresent()) {
      var fileComponents = fileComponentsOpt.get();
      var pathNamePath = NamePath.parse(fileComponents.getFilename());
      var namePath =
          pathNamePath
              .popLast()
              .concat(pathNamePath.getLast().append(Name.system(fileComponents.getSuffix())));
      Optional<Path> path = resourceResolver.resolveFile(namePath);
      errors.checkFatal(
          path.isPresent(),
          "Could not load schema from: %s [via resolver %s]",
          namePath,
          resourceResolver);
      SchemaConversionResult schemaResult =
          schemaFactories.get(fileComponents.getExtension()).convert(path.get(), errors);
      return Optional.of(schemaResult);
    }
    return Optional.empty();
  }
}
