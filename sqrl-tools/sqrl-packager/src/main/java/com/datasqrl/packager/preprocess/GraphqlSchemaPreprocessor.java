package com.datasqrl.packager.preprocess;

import static com.datasqrl.config.PipelineFactory.ENGINES_PROPERTY;

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
import java.io.File;
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
      return;
    }
    String schemaName = path.getFileName().toString().split("\\.")[0];
    List<TableDefinition> schemas = GraphqlSchemaVisitor.accept(
        new InputFieldToFlexibleSchemaRelation(registry), mutationType, null);

    Path dir = Files.createDirectories(
        Files.createTempDirectory("schemas").resolve(schemaName));
    writeTableSchema(schemas, dir, schemaName, context);

    //After writing the schema, write the source and sink
    SqrlConfig config = context.getSqrlConfig();
    SqrlConfig log = config.getSubConfig(ENGINES_PROPERTY).getSubConfig("log");

    writeSource(dir, schemas, log, context);
    context.addDependency(dir);
  }

  @SneakyThrows
  private void writeSource(Path dir, List<TableDefinition> schemas, SqrlConfig log,
      ProcessorContext context) {

    for (TableDefinition definition : schemas) {
      Map tableConfig = convertToSourceConfig(definition, log);

      Path f = dir.resolve(definition.name + ".table.json");
      SqrlObjectMapper.INSTANCE.writerWithDefaultPrettyPrinter()
          .writeValue(f.toFile(), tableConfig);
    }
  }

  private Map convertToSourceConfig(TableDefinition tableDefinition, SqrlConfig log) {
    Map connector = new HashMap();
    for (String key : log.getKeys()) {
      connector.put(key, log.asString(key).get());
    }
    connector.put("name", "kafka");
    connector.put("topic", tableDefinition.name);

    Map config = Map.of("type", ExternalDataType.source_and_sink.name(),
        "canonicalizer", "system",
        "format", Map.of(
            "name", "json"
        ),
        "identifier", tableDefinition.name,
        "schema", "flexible",
        "connector", connector
    );

    return config;
  }

  @SneakyThrows
  private void writeTableSchema(List<TableDefinition> schemas, Path dir, String schemaName,
      ProcessorContext context) {

    for (var schema : schemas) {
      Path path = dir.resolve(schema.name + ".schema.yml");
      SqrlObjectMapper.YAML_INSTANCE.writeValue(path.toFile(), schema);
    }

  }
}