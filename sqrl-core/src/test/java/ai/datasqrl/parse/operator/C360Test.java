package ai.datasqrl.parse.operator;

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.datasqrl.Environment;
import ai.datasqrl.server.ScriptDeployment;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.graphql.schema.GraphqlSchemaBuilder;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.config.error.ErrorCollector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;

import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class C360Test {
  public static final Path RETAIL_DIR = Path.of("../sqml-examples/retail/");
  public static final String RETAIL_SCRIPT_NAME = "c360";
  public static final Path RETAIL_SCRIPT_DIR = RETAIL_DIR.resolve(RETAIL_SCRIPT_NAME);
  public static final String SQML_SCRIPT_EXTENSION = ".sqml";
  public static final Path RETAIL_IMPORT_SCHEMA_FILE = RETAIL_SCRIPT_DIR.resolve("pre-schema.yml");
  public static final String RETAIL_DATA_DIR_NAME = "ecommerce-data";
  public static final String RETAIL_DATASET = "ecommerce-data";
  public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);

  Environment env;
  private VertxInternal vertx;
  public static ScriptBundle bundle = createBundle();
  public static DirectorySourceImplementation dd = DirectorySourceImplementation.builder()
      .uri(RETAIL_DATA_DIR.toAbsolutePath().toString()).build();

  @SneakyThrows
  private static ScriptBundle createBundle() {
    return ScriptBundle.Config.builder()
        .name(RETAIL_SCRIPT_NAME)
        .scripts(ImmutableList.of(
            SqrlScript.Config.builder()
                .name(RETAIL_SCRIPT_NAME)
                .main(true)
                .content(Files.readString(RETAIL_SCRIPT_DIR.resolve(RETAIL_SCRIPT_NAME + SQML_SCRIPT_EXTENSION)))
                .inputSchema(Files.readString(RETAIL_IMPORT_SCHEMA_FILE))
                .build()
        ))
        .build().initialize(ErrorCollector.root());
  }


  @BeforeEach
  public void setup() {
    VertxOptions vertxOptions = new VertxOptions();
    this.vertx = (VertxInternal) Vertx.vertx(vertxOptions);

    env = Environment.create(ai.datasqrl.parse.operator.DefaultTestSettings.create(vertx));

    env.getDatasetRegistry().addOrUpdateSource(RETAIL_DATASET, dd, ErrorCollector.root());

  }

  @Test
  @SneakyThrows
  public void testC360() {
//    env.monitorDatasets();

    ScriptBundle bundle = ScriptBundle.Config.builder()
            .name(RETAIL_SCRIPT_NAME)
            .scripts(ImmutableList.of(
                    SqrlScript.Config.builder()
                            .name(RETAIL_SCRIPT_NAME)
                            .main(true)
                            .content(Files.readString(RETAIL_SCRIPT_DIR.resolve(RETAIL_SCRIPT_NAME + SQML_SCRIPT_EXTENSION)))
                            .inputSchema(Files.readString(RETAIL_IMPORT_SCHEMA_FILE))
                            .build()
            ))
            .build().initialize(ErrorCollector.root());

    env.compile(bundle);

    GraphQLSchema graphQLSchema = GraphqlSchemaBuilder.newGraphqlSchema()
//        .schema(script.getNamespace().getSchema())
//        .setCodeRegistryBuilder(script.getRegistry())
        .build();

    System.out.println(new SchemaPrinter().print(graphQLSchema));

    GraphQL graphQL = GraphQL.newGraphQL(graphQLSchema).build();

    ExecutionResult result = graphQL.execute(
        "{"
            + "  orders{data{id, customerid, entries {productid, quantity, unit_price, discount, product{name}}}}"
//            + "  customer{data{customerid, email, name}}"
//            + "  product{data{productid, name, description, category}}"
            + "}");

    System.out.println(result.getErrors());
    assertTrue(result.getErrors().isEmpty());

    System.out.println(pretty(result.getData()));
  }

  @SneakyThrows
  private String pretty(Object data) {
    return (new ObjectMapper())
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(data);
  }
}
