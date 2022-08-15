package ai.datasqrl.flink;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.EnvironmentConfiguration.MetaData;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.physical.stream.Job;
import ai.datasqrl.physical.stream.ScriptExecutor;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.generate.Generator;
import ai.datasqrl.plan.local.generate.GeneratorBuilder;
import ai.datasqrl.util.ResultSetPrinter;
import ai.datasqrl.util.data.C360;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.ResultSet;

public class LPFlinkTest extends AbstractSQRLIT {

  private ErrorCollector error;
  private PhysicalPlanner physicalPlanner;
  private Generator generator;
  private ConfiguredSqrlParser parser;
  private JDBCConnectionProvider jdbc;

  @BeforeEach
  public void setup() throws IOException {
    error = ErrorCollector.root();
    initialize(IntegrationTestSettings.getFlinkWithDB());
    C360 example = C360.INSTANCE;
    example.registerSource(env);

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().setIncludeSchema(true).getBundle();
    Assertions.assertTrue(
        importManager.registerUserSchema(bundle.getMainScript().getSchema(), error));

    this.parser = new ConfiguredSqrlParser(error);
    this.generator = GeneratorBuilder.build(importManager, error);

    DatabaseConnectionProvider db = sqrlSettings.getDatabaseEngineProvider().getDatabase(MetaData.DEFAULT_DATABASE);
    jdbc = (JDBCConnectionProvider) db;

    physicalPlanner = new PhysicalPlanner(importManager, jdbc,
        sqrlSettings.getStreamEngineProvider().create());
  }

  @SneakyThrows
  @Test
  public void testLP() {
    generator.generate(parser.parse(
            "IMPORT ecommerce-data.Product;\n" +
            "ProductSub := SELECT productid, name, description FROM Product;"));

    OptimizedDAG optimizedDAG = generator.planDAG();
    PhysicalPlan plan = physicalPlanner.plan(optimizedDAG);
    ScriptExecutor scriptExecutor = new ScriptExecutor();
    Job job = scriptExecutor.execute(plan);
    System.out.println(job.getExecutionId());

    ResultSet resultSet = this.jdbc.getConnection().createStatement()
        .executeQuery("SELECT * FROM productsub$5;");

    System.out.println("Results: ");
    ResultSetPrinter.print(resultSet, System.out);
  }
}
