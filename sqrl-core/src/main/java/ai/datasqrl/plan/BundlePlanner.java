package ai.datasqrl.plan;

import ai.datasqrl.config.BundleOptions;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.tree.SqrlStatement;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.local.analyze.Analyzer;
import ai.datasqrl.plan.local.analyze.Namespace;
import ai.datasqrl.plan.local.generate.Generator;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import java.util.List;
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

  public BundlePlanner(BundleOptions options) {
    this.options = options;
  }

  public PhysicalPlan processBundle(ScriptBundle bundle) {
    Namespace schema = planMain(bundle.getMainScript());

//    Optimizer optimizer = new Optimizer(bundle.getQueries(), true);
//    LogicalPlan plan = new LogicalPlan(List.of(), List.of(), schema);

//    PhysicalPlanner physicalPlanner = new PhysicalPlanner(options.getImportManager(),
//        options.getDbConnection(),
//        options.getStreamEngine());
//    return physicalPlanner.plan(plan);
    return null;
  }

  private Namespace planMain(SqrlScript mainScript) {
    ConfiguredSqrlParser parser = ConfiguredSqrlParser.newParser(errorCollector);
    ScriptNode scriptAst = parser.parse(mainScript.getContent());
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();

    BridgedCalciteSchema subSchema = new BridgedCalciteSchema();
    rootSchema.add(mainScript.getName().getCanonical(), subSchema);

    PlannerFactory plannerFactory = new PlannerFactory(rootSchema);
    Planner planner = plannerFactory.createPlanner(mainScript.getName().getCanonical());

    Analyzer analyzer = new Analyzer(options.getImportManager(), SchemaAdjustmentSettings.DEFAULT,
        errorCollector);

    Generator generator = new Generator(planner, analyzer.getAnalysis());
    subSchema.setBridge(generator);

    for (Node node : scriptAst.getStatements()) {
      analyzer.analyze((SqrlStatement) node);
      generator.generate((SqrlStatement) node);
    }

    return analyzer.getNamespace();
  }
}
