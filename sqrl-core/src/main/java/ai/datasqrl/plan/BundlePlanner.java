package ai.datasqrl.plan;

import ai.datasqrl.config.BundleOptions;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.plan.calcite.BasicSqrlCalciteBridge;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.SqrlSchemaCatalog;
import ai.datasqrl.plan.local.BundleTableFactory;
import ai.datasqrl.plan.local.SchemaUpdatePlanner;
import ai.datasqrl.plan.local.operations.SchemaBuilder;
import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
import ai.datasqrl.schema.Schema;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.BridgedCalciteSchema;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Creates a logical and physical plan for a SQRL {@link ScriptBundle} submitted to the DataSQRL server for
 * compilation or execution.
 *
 * @see ScriptBundle
 */
@Slf4j
public class BundlePlanner {

  private final BundleOptions options;
  private final ErrorCollector errorCollector = ErrorCollector.root();
  private final BundleTableFactory tableFactory;

  public BundlePlanner(BundleOptions options) {
    this.options = options;
    this.tableFactory = new BundleTableFactory();
  }

  public PhysicalPlan processBundle(ScriptBundle bundle) {
    Schema schema = planMain(bundle.getMainScript());

//    Optimizer optimizer = new Optimizer(bundle.getQueries(), true);
    LogicalPlan plan = new LogicalPlan(List.of(), List.of(), schema);

    PhysicalPlanner physicalPlanner = new PhysicalPlanner(options.getImportManager(),
        options.getDbConnection(),
        options.getStreamEngine());
    return physicalPlanner.plan(plan);
  }

  private Schema planMain(SqrlScript mainScript) {
    ConfiguredSqrlParser parser = ConfiguredSqrlParser.newParser(errorCollector);
    ScriptNode scriptAst = parser.parse(mainScript.getContent());

    BasicSqrlCalciteBridge dag = createDag(mainScript.getName().getCanonical());
    SchemaBuilder schema = new SchemaBuilder();

    for (Node node : scriptAst.getStatements()) {
      Optional<SchemaUpdateOp> operation = planStatement(node, schema);
      if (operation.isEmpty()) {
        log.warn("Operation is null for statement: {}", node.getClass());
      }
      operation.ifPresent(schema::apply);
      operation.ifPresent(dag::apply);
    }

    return schema.build();
  }

  public BasicSqrlCalciteBridge createDag(String schemaName) {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    SqrlSchemaCatalog catalog = new SqrlSchemaCatalog(rootSchema);

    BridgedCalciteSchema subSchema = new BridgedCalciteSchema();
    catalog.add(schemaName, subSchema);

    PlannerFactory plannerFactory = new PlannerFactory(catalog);
    Planner planner = plannerFactory.createPlanner(schemaName);

    BasicSqrlCalciteBridge dag = new BasicSqrlCalciteBridge(planner);
    subSchema.setBridge(dag);
    return dag;
  }

  private Optional<SchemaUpdateOp> planStatement(Node statement, SchemaBuilder schema) {
    SchemaUpdatePlanner planner = new SchemaUpdatePlanner(
        this.options.getImportManager(),
        tableFactory,
        this.options.getSchemaSettings(),
        this.errorCollector);
    return planner.plan(schema.peek(), statement);
  }
}
