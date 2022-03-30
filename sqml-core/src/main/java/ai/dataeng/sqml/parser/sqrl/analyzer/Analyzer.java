package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ImportManager;
import ai.dataeng.sqml.parser.operator.ShadowingContainer;
import ai.dataeng.sqml.parser.sqrl.FieldFactory;
import ai.dataeng.sqml.parser.sqrl.LogicalDag;
import ai.dataeng.sqml.parser.sqrl.calcite.CalcitePlanner;
import ai.dataeng.sqml.parser.sqrl.transformers.ExpressionToQueryTransformer;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.InlineJoin;
import ai.dataeng.sqml.tree.JoinDeclaration;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

@Slf4j
@AllArgsConstructor
public class Analyzer {
  private ImportManager importManager;
  private CalcitePlanner planner;

  //Keep namespace here
  protected final LogicalDag logicalDag = new LogicalDag(new ShadowingContainer<>());
  protected final ErrorCollector errors = ErrorCollector.root();

  public Analysis analyze(ScriptNode script) {
    Visitor visitor = new Visitor(this);
    script.accept(visitor, null);
    return new Analysis(logicalDag);
  }

  @Value
  public static class Analysis {
    LogicalDag dag;
  }

  public class Visitor extends AstVisitor<Void, Void> {
    private final AtomicBoolean importResolved = new AtomicBoolean(false);
    private final Analyzer analyzer;

    public Visitor(Analyzer analyzer) {
      this.analyzer = analyzer;
    }

    @Override
    public Void visitNode(Node node, Void context) {
      throw new RuntimeException(String.format("Could not process node %s : %s", node.getClass().getName(), node));
    }

    @Override
    public Void visitScript(ScriptNode node, Void context) {
      List<Node> statements = node.getStatements();
      for (int i = 0; i < statements.size(); i++) {
        statements.get(i).accept(this, null);

        //Test for end of imports
        Optional<Node> nextStatement = (i < statements.size()) ?
          Optional.of(statements.get(i)) : Optional.empty();
        if (nextStatement.map(s->!(s instanceof ImportDefinition))
            .orElse(false)) {
          importResolved.set(true);
        }
      }

      return null;
    }

    //TODO:
    // import ds.*;
    // import ns.function;
    // import ds;
    @Override
    public Void visitImportDefinition(ImportDefinition node, Void context) {
      if (importResolved.get()) {
        throw new RuntimeException(String.format("Import statement must be in header %s", node.getNamePath()));
      }

      if (node.getNamePath().getLength() > 2) {
        throw new RuntimeException(String.format("Cannot import identifier: %s", node.getNamePath()));
      }

      Table table = importManager.resolveTable(node.getNamePath().get(0), node.getNamePath().get(1),
          node.getAliasName(), errors);
      logicalDag.getSchema().add(table);
      return null;
    }

    @Override
    public Void visitQueryAssignment(QueryAssignment queryAssignment, Void context) {
      NamePath name = queryAssignment.getNamePath();
      Query query = queryAssignment.getQuery();

      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analyzer);
      Scope scope = null;//new Scope();
      Scope newScope = query.accept(statementAnalyzer, scope);
      Node rewrittenNode = newScope.getNode();


      RelNode plan = planner.plan(rewrittenNode);
      //0. rewritten node is unsqrled node
      //1. translate node to calcite sql node
      //2. convert sql node to rel, locally optimize, attach to plan dag
      //3. attach rel to create table
      //4. attach table to logical plan
      //5. add parent field to logical plan


      return null;
    }

    @Override
    public Void visitExpressionAssignment(ExpressionAssignment assignment, Void context) {
      NamePath name = assignment.getNamePath();
      Expression expression = assignment.getExpression();

      Optional<Table> table = logicalDag.getSchema().getByName(name.getFirst());

      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analyzer);
      Query query = createExpressionQuery(expression);
//      Scope queryScope = new Scope(table, null, null);
      Scope newScope = query.accept(statementAnalyzer, null);

      //Add a Field to the logical dag
      Field field = FieldFactory.createTypeless(table.get(), name.getLast());
      table.get().addField(field);

      return null;
    }

    private Query createExpressionQuery(Expression expression) {
      ExpressionToQueryTransformer expressionToQueryTransformer = new ExpressionToQueryTransformer();
      return expressionToQueryTransformer.transform(expression);
    }

    @Override
    public Void visitCreateSubscription(CreateSubscription subscription, Void context) {
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analyzer);
//      Scope queryScope = subscription.getQuery().accept(statementAnalyzer, scope);

      return null;
    }

    @Override
    public Void visitDistinctAssignment(DistinctAssignment node, Void context) {

      return null;
    }

    @Override
    public Void visitJoinDeclaration(JoinDeclaration node, Void context) {
      NamePath table = node.getInlineJoin().getJoin().getTable();

      return null;
    }

    @Override
    public Void visitInlineJoin(InlineJoin node, Void context) {

      return context;
    }
  }

  public Optional<Table> lookup(NamePath namePath) {
    if (namePath.isEmpty()) return Optional.empty();

    Table schemaTable = (Table)this.logicalDag.getSchema().getByName(namePath.getFirst()).get();
    if (schemaTable != null) {
      if (namePath.getLength() == 1) {
        return Optional.of(schemaTable);
      }

      return schemaTable.walk(namePath.popFirst());
    }
//
//    //Check if path is qualified
//    Dataset dataset = this.rootDatasets.get(namePath.getFirst());
//    if (dataset != null) {
//      return dataset.walk(namePath.popFirst());
//    }
//
//    //look for table in all root datasets
//    for (Map.Entry<Name, Dataset> rootDataset : rootDatasets.entrySet()) {
//      Optional<Table> ds = rootDataset.getValue().walk(namePath);
//      if (ds.isPresent()) {
//        return ds;
//      }
//    }
//
//    Dataset localDs = scopedDatasets.get(namePath.getFirst());
//    if (localDs != null) {
//      return localDs.walk(namePath.popFirst());
//    }

    return Optional.empty();
  }
}
