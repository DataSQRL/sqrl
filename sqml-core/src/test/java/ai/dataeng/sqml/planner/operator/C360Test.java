package ai.dataeng.sqml.planner.operator;

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.ScriptBundle;
import ai.dataeng.sqml.api.graphql.GraphqlSchemaBuilder;
import ai.dataeng.sqml.execution.flink.ingest.DatasetRegistration;
import ai.dataeng.sqml.importer.source.simplefile.DirectoryDataset;
import ai.dataeng.sqml.planner.Script;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
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

  @BeforeEach
  public void setup() {
    VertxOptions vertxOptions = new VertxOptions();
    this.vertx = (VertxInternal) Vertx.vertx(vertxOptions);

    env = Environment.create(DefaultTestSettings.create(vertx));

    DirectoryDataset dd = new DirectoryDataset(DatasetRegistration.of(RETAIL_DATASET), RETAIL_DATA_DIR);
    env.registerDataset(dd);
  }

  @Test
  @SneakyThrows
  public void testC360() {
    env.monitorDatasets();

    ScriptBundle bundle = new ScriptBundle.Builder().createScript()
        .setName(RETAIL_SCRIPT_NAME)
        .setScript(RETAIL_SCRIPT_DIR.resolve(RETAIL_SCRIPT_NAME + SQML_SCRIPT_EXTENSION))
        .setImportSchema(RETAIL_IMPORT_SCHEMA_FILE)
        .asMain()
        .add().build();

    Script script = env.compile(bundle);

    GraphQLSchema graphQLSchema = GraphqlSchemaBuilder.newGraphqlSchema()
        .schema(script.getNamespace().getSchema())
        .setCodeRegistryBuilder(script.getRegistry())
        .build();

    System.out.println(new SchemaPrinter().print(graphQLSchema));

    GraphQL graphQL = GraphQL.newGraphQL(graphQLSchema).build();

    ExecutionResult result = graphQL.execute(
        "{"
            + "  orders(filter: {id: {eq: 10007140}}){data{id, customerid, entries {productid, quantity, unit_price, discount}}}"
            + "  customer{data{customerid, email, name}}"
            + "  product{data{productid, name, description, category}}"
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
