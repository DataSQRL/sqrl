//package com.datasqrl;
//
//import com.datasqrl.error.ErrorCollector;
//import com.datasqrl.io.tables.TableConfig;
//import com.datasqrl.io.tables.TableSchema;
//import com.datasqrl.io.tables.TableSchemaFactory;
//import com.datasqrl.util.serializer.Deserializer;
//import com.google.auto.service.AutoService;
//import io.vertx.core.json.JsonObject;
//import io.vertx.json.schema.JsonSchema;
//import io.vertx.json.schema.JsonSchemaOptions;
//import io.vertx.json.schema.SchemaRepository;
//import io.vertx.json.schema.Validator;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.Optional;
//import java.util.Set;
//import lombok.SneakyThrows;
//
//@AutoService(TableSchemaFactory.class)
//public class JsonSchemaTableSchemaFactory implements TableSchemaFactory {
//
//  public static final String SCHEMA_JSON = ".schema.json";
//
//  @Override
//  public Optional<TableSchema> create(Deserializer deserialize, Path baseDir,
//      TableConfig tableConfig, ErrorCollector errors) {
//    Path path = baseDir.resolve(tableConfig.getName() + SCHEMA_JSON);
//    if (!Files.isRegularFile(path)) {
//      return Optional.empty();
//    }
//
//
//    return Optional.of(new JsonTableSchema(createValidator(path)));
//  }
//
//  @SneakyThrows
//  private Validator createValidator(Path path) {
//    JsonObject schema = new JsonObject(Files.readString(path));
//
//    //todo: Add subschemas to repo
//    SchemaRepository repository =
//        SchemaRepository.create(new JsonSchemaOptions().setBaseUri("https://vertx.io"));
//
//    Validator validator = repository.validator(JsonSchema.of(schema));
//    return validator;
//  }
//
//  @Override
//  public String baseFileSuffix() {
//    return SCHEMA_JSON;
//  }
//
//  @Override
//  public Set<String> allSuffixes() {
//    return Set.of(SCHEMA_JSON);
//  }
//
//  @Override
//  public Optional<String> getFileName() {
//    return Optional.empty();
//  }
//}
