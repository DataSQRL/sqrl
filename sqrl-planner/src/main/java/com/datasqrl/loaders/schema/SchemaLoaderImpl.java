package com.datasqrl.loaders.schema;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory.SchemaResult;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.plan.table.RelDataTypeTableSchema;
import com.datasqrl.util.BaseFileUtil;
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
  public Optional<SchemaResult> loadSchema(String tableName, String relativeSchemaFilePath) {
    Map<String, TableSchemaFactory> schemaFactories = TableSchemaFactory.factoriesByExtension();
    for (String extension : schemaFactories.keySet()) {
      if (relativeSchemaFilePath.endsWith(extension)) {
        var pathWoExtension = StringUtil.removeFromEnd(relativeSchemaFilePath, extension);
        var pathNamePath = NamePath.parse(pathWoExtension);
        var namePath =
            pathNamePath.popLast().concat(pathNamePath.getLast().append(Name.system(extension)));
        Optional<Path> path = resourceResolver.resolveFile(namePath);
        errors.fatal(
            "Could not load schema from: %s [via resolver %s]", namePath, resourceResolver);
        TableSchema schema =
            schemaFactories.get(extension).create(BaseFileUtil.readFile(path.get()), path, errors);

        var schemaError = errors.withScript(path.get().getFileName(), schema.getDefinition());
        SchemaResult schemaResult =
            SchemaToRelDataTypeFactory.load(schema).map(schema, tableName, schemaError);
        return Optional.of(schemaResult);
      }
    }
    return Optional.empty();
  }
}
