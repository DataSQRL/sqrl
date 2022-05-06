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
import ai.datasqrl.plan.local.operations.SchemaBuilder;
import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
import ai.datasqrl.schema.Schema;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
      Optional<SchemaUpdateOp> operation = planStatement(node, schema);
      if (operation.isEmpty()) {
        log.warn("Operation is null for statement: {}", node.getClass());
      }
      operation.ifPresent(schema::apply);
    }

    return schema.build();
  }

  public Optional<SchemaUpdateOp> planStatement(Node statement, SchemaBuilder schema) {
//    log.info("Statement {}:", NodeFormatter.accept(statement));
//    StatementValidator validator = new StatementValidator(
//        options.getImportManager(),
//        schema.peek());
//    StatementScope scope = validator.validate(statement);
//
//    StatementTransformer transformer = new StatementTransformer();
//    Optional<Node> node = transformer.transform(statement, scope);
//    log.info("Transformed {}:", node.map(NodeFormatter::accept).orElse(""));
//
//    Optional<SqlResult> validationResult = ValidateSql.validate(node, schema.peek());
//    log.info("SqlNode {}:", validationResult.map(v->SqlNodeToString.toString(v.getSqlNode())).orElse(""));
//
//    LocalPlanner planner = new LocalPlanner(schema.peek());
//    return Optional.ofNullable(planner.plan(statement, node, scope, validationResult));
    return Optional.empty();
  }
}
