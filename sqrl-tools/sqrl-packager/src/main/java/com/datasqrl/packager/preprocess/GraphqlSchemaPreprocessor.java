package com.datasqrl.packager.preprocess;

import static com.datasqrl.config.PipelineFactory.ENGINES_PROPERTY;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.packager.preprocess.graphql.InputFieldToFlexibleSchemaRelation;
import com.datasqrl.schema.input.external.TableDefinition;
import com.datasqrl.util.SqrlObjectMapper;
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
      return;
    }
    String schemaName = path.getFileName().toString().split("\\.")[0];
    List<TableDefinition> schemas = GraphqlSchemaVisitor.accept(
        new InputFieldToFlexibleSchemaRelation(registry), mutationType, null);
    writeTableSchema(schemas, schemaName, context);

    //After writing the schema, write the source and sink
    SqrlConfig config = context.getSqrlConfig();
    SqrlConfig log = config.getSubConfig(ENGINES_PROPERTY).getSubConfig("log");

    writeSource(schemaName, schemas, log);
  }

  private void writeSource(String schemaName, List<TableDefinition> schemas, SqrlConfig log) {

  }

  @SneakyThrows
  private void writeTableSchema(List<TableDefinition> schemas, String schemaName,
      ProcessorContext context) {
    var path = Files.createDirectories(
        Files.createTempDirectory("schemas").resolve(schemaName));

    for (var schema : schemas) {
      File file = path.resolve(schema.name + ".schema.json").toFile();
      SqrlObjectMapper.YAML_INSTANCE.writeValue(file, schema);
    }

    context.addDependency(path);
  }
}