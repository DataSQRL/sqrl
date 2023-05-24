package com.datasqrl.packager.preprocess;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.packager.preprocess.graphql.InputFieldToFlexibleSchemaRelation;
import com.datasqrl.schema.input.external.TableDefinition;
import com.datasqrl.util.SqrlObjectMapper;
import com.google.auto.service.AutoService;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@AutoService(Preprocessor.class)
@Slf4j
public class GraphqlSchemaPreprocessor implements Preprocessor {

  @Override
  public Pattern getPattern() {
    return Pattern.compile(".*\\.graphqls$");
  }

  @SneakyThrows
  @Override
  public void loader(Path path, ProcessorContext context, ErrorCollector errors) {
    String schema = Files.readString(path);
    TypeDefinitionRegistry registry = new SchemaParser().parse(schema);

    ObjectTypeDefinition mutationType = (ObjectTypeDefinition) registry
        .getType("Mutation")
        .orElse(null);
    if (mutationType == null) {
      log.trace("No mutations");
      return;
    }
    String schemaName = path.getFileName().toString().split("\\.")[0];
    List<TableDefinition> schemas = GraphqlSchemaVisitor.accept(
        new InputFieldToFlexibleSchemaRelation(registry), mutationType, null);

    Path dir = Files.createDirectories(
        Files.createTempDirectory("schemas").resolve(schemaName));

    writeTableSchema(schemas, dir, schemaName, context);

  }

  @SneakyThrows
  private void writeTableSchema(List<TableDefinition> schemas, Path dir, String schemaName,
      ProcessorContext context) {

    for (TableDefinition schema : schemas) {
      Path path = dir.resolve(schema.name + ".schema.yml");
      log.trace("Writing table schema:" + path);

      SqrlObjectMapper.YAML_INSTANCE.writeValue(path.toFile(), schema);

      context.createModuleFolder(NamePath.of(schemaName))
          .addDependency(path);
    }

  }
}