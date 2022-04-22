package ai.datasqrl.plan;

import ai.datasqrl.config.BundleOptions;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.parse.SqrlParser;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.plan.local.LocalPlanner;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.plan.local.operations.SchemaBuilder;
import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
import ai.datasqrl.sqrl2sql.StatementTransformer;
import ai.datasqrl.validate.StatementValidator;
import ai.datasqrl.validate.scopes.StatementScope;
import com.google.common.base.Preconditions;

public class BundlePlanner {

  private final BundleOptions options;
  ErrorCollector errorCollector = ErrorCollector.root();

  public BundlePlanner(BundleOptions options) {
    this.options = options;
  }

  public PhysicalPlan processBundle(ScriptBundle bundle) {
    Schema schema = planMain(bundle.getMainScript());

    Optimizer optimizer = new Optimizer(bundle.getQueries(), true);
    LogicalPlan plan = optimizer.findBestPlan(schema);

    PhysicalPlanner physicalPlanner = new PhysicalPlanner(options.getImportManager(),
        options.getJdbcConfiguration(),
        options.getStreamEngine());
    return physicalPlanner.plan(plan);
  }

  private Schema planMain(SqrlScript mainScript) {
    SqrlParser parser = SqrlParser.newParser(errorCollector);
    ScriptNode scriptAst = parser.parse(mainScript.getContent());

    SchemaBuilder schema = new SchemaBuilder();
    for (Node node : scriptAst.getStatements()) {
      SchemaUpdateOp operation = planStatement(node, schema);
      Preconditions.checkNotNull(operation, "Operation is null for statement: {%s}", node);
      schema.apply(operation);
    }

    return schema.build();
  }

  public SchemaUpdateOp planStatement(Node statement, SchemaBuilder schema) {
    StatementValidator validator = new StatementValidator(
        options.getImportManager(),
        schema.peek());
    StatementScope scope = validator.validate(statement);

    StatementTransformer transformer = new StatementTransformer();
    Node node = transformer.transform(statement, scope);

    LocalPlanner planner = new LocalPlanner(schema.peek());
    return planner.plan(statement, node, scope);
  }
}
