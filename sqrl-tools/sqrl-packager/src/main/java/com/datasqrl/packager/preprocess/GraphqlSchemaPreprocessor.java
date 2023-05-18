package com.datasqrl.packager.preprocess;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.packager.preprocess.graphql.InputFieldToFlexibleSchemaRelation;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.external.SchemaExport;
import com.datasqrl.schema.input.external.TableDefinition;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.auto.service.AutoService;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@AutoService(Preprocessor.class)
/*
 * Reads a jar and creates sqrl manifest entries in the build directory
 */
@Slf4j
public class GraphqlSchemaPreprocessor implements Preprocessor {

  @Override
  public Pattern getPattern() {
    return Pattern.compile(".*\\.graphqls$");
  }

  @SneakyThrows
  @Override
  public void loader(Path path, ProcessorContext processorContext, ErrorCollector errors) {
    //Read file, convert it to flexible schema

    String schema = Files.readString(path);

    // parse the schema
    SchemaParser schemaParser = new SchemaParser();
    TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(schema);

    // get mutation type
    ObjectTypeDefinition mutationType = typeDefinitionRegistry.getType("Mutation")
        .map(type -> (ObjectTypeDefinition) type).orElse(null);

    if (mutationType == null) {
      return;
    }

    InputFieldToFlexibleSchemaRelation toFlexible = new InputFieldToFlexibleSchemaRelation(typeDefinitionRegistry);
    List<TableDefinition> schemas = GraphqlSchemaVisitor.accept(toFlexible, mutationType, null);
    writeTableSchema(schemas, path.getFileName(), processorContext);
  }

  @SneakyThrows
  private void writeTableSchema(List<TableDefinition> tableSchemas, Path fileName,
      ProcessorContext processorContext) {
    Path path = Files.createTempDirectory("schemas")
        .resolve(fileName.toString().split("\\.")[0]);
    Files.createDirectories(path);

//    SchemaExport export = new SchemaExport();
    for (TableDefinition schema : tableSchemas) {
      Path path1 = path.resolve(schema.name + ".schema.json");
      File file = path1.toFile();

      SqrlObjectMapper.YAML_INSTANCE.writeValue(file, schema);
    }

    processorContext.addDependency(path);
  }
}