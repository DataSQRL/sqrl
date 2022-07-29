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
import ai.datasqrl.plan.calcite.table.AbstractRelationalTable;
import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.generate.Generator;
import ai.datasqrl.plan.local.generate.GeneratorBuilder;
import ai.datasqrl.plan.queries.TableQuery;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.util.ResultSetPrinter;
import ai.datasqrl.util.data.C360;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    generator.generate(parser.parse("IMPORT ecommerce-data.Product;"));
    AbstractRelationalTable sourceTable = (AbstractRelationalTable)generator.getRelSchema().getTable("product$1", false).getTable();

    RelBuilder relBuilder = generator.getPlanner().getRelBuilder();
    RelNode testQuery = relBuilder.scan("product$1")
              .project(RexInputRef.of(1, relBuilder.peek().getRowType()), RexInputRef.of(2, relBuilder.peek().getRowType()))
              .build();
    TableQuery tableQuery = new TableQuery(sourceTable, testQuery);

    System.out.println();

    OptimizedDAG optimizedDAG = new OptimizedDAG(List.of(tableQuery), List.of());
    PhysicalPlan plan = physicalPlanner.plan(optimizedDAG);
    ScriptExecutor scriptExecutor = new ScriptExecutor();
    Job job = scriptExecutor.execute(plan);
    System.out.println(job.getExecutionId());

    ResultSet resultSet = this.jdbc.getConnection().createStatement()
        .executeQuery("SELECT * FROM product$1;");

    ResultSetPrinter.print(resultSet, System.out);
  }
}
