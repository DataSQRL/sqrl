package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Relationship.Multiplicity;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ImportManager;
import ai.dataeng.sqml.parser.sqrl.FieldFactory;
import ai.dataeng.sqml.parser.sqrl.LogicalDag;
import ai.dataeng.sqml.parser.sqrl.calcite.CalcitePlanner;
import ai.dataeng.sqml.parser.sqrl.schema.TableFactory;
import ai.dataeng.sqml.parser.sqrl.transformers.ExpressionToQueryTransformer;
import ai.dataeng.sqml.tree.AllColumns;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.JoinDeclaration;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;

@Slf4j
@AllArgsConstructor
public class Analyzer {
  private ImportManager importManager;
  private CalcitePlanner planner;
  private TableFactory tableFactory;
  private LogicalDag logicalDag;

  //Keep namespace here
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

      //Create 'Table'
      RelNode plan = planner.plan(rewrittenNode);

      Table table = tableFactory.create(name, (Query)rewrittenNode);
      table.setRelNode(plan);

      //Todo: create parent field
      return null;
    }

    @Override
    public Void visitExpressionAssignment(ExpressionAssignment assignment, Void context) {
      NamePath name = assignment.getNamePath();
      Expression expression = assignment.getExpression();

      Optional<Table> table = logicalDag.getSchema().getByName(name.getFirst());
      Preconditions.checkState(table.isPresent(), "Expression cannot be assigned to root");

      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analyzer);
      Query query = createExpressionQuery(expression);

      Scope newScope = query.accept(statementAnalyzer, new Scope(table, query, null));

      Node rewrittenNode = newScope.getNode();
      RelNode plan = planner.plan(rewrittenNode);

      table.get().setRelNode(plan);

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
      return null;
    }

    @Override
    public Void visitDistinctAssignment(DistinctAssignment node, Void context) {
      Table table = tableFactory.create(node.getNamePath(), node.getTable());
      Scope scope = new Scope(Optional.empty(), node, new LinkedHashMap<>());
      Optional<Table> oldTable = lookup(node.getTable().toNamePath());

      oldTable.get().getFields().getElements()
          .forEach(table::addField);

      logicalDag.getSchema().add(table);

      List<Field> fields = node.getPartitionKeys().stream()
              .map(e->table.getFieldOpt(e)
                  .orElseThrow(()->new RuntimeException("Could not find primary key " + e)))
              .collect(Collectors.toList());
      table.addUniqueConstraint(fields);

      return null;
    }

    @Override
    public Void visitJoinDeclaration(JoinDeclaration node, Void context) {
      NamePath namePath = node.getNamePath();

      StatementAnalyzer sAnalyzer = new StatementAnalyzer(analyzer);

      Select select = new Select(Optional.empty(), false, Optional.empty(), List.of(new AllColumns()));
      Relation from = new Join(node.getLocation(), node.getInlineJoin().getJoinType(),
          new TableNode(Optional.empty(), NamePath.parse("_"), Optional.empty()),
          node.getInlineJoin().getRelation(),
          Optional.empty());
      Query querySpec = new Query(new QuerySpecification(node.getLocation(),
          select,
          from,
          Optional.<Expression>empty(),
          Optional.<GroupBy>empty(),
          Optional.<Expression>empty(),
          node.getInlineJoin().getOrderBy(),
          node.getInlineJoin().getLimit()),
          Optional.empty(),
          Optional.empty()
      );

      Scope scope = querySpec.accept(sAnalyzer, null);

      Map<Name, Table> joinScope = scope.getJoinScope();
      Node rewritten = scope.getNode();
      if (node.getInlineJoin().getLimit().isEmpty()) {
        Join join = (Join)((QuerySpecification)((Query)rewritten).getQueryBody()).getFrom();
        rewritten = join.getRight();
      }

      Table table = lookup(namePath).get();
      List<Table> tables = new ArrayList(joinScope.values());
      Table lastTable = tables.get(joinScope.values().size());
      Relationship joinField = new Relationship(namePath.getLast(), table, lastTable,
          null, Multiplicity.MANY, null);
      joinField.setNode(rewritten);

      table.addField(joinField);

      return null;
    }
  }

  public Optional<Table> lookup(NamePath namePath) {
    //Get w/context in path
//    if (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) {
//      Table table = scope.getContextTable().get();
//      if (namePath.getLength() > 1) {
//        table = table.walk(namePath.popFirst())
//            .get();
//      }
//      return Optional.of(table);
//    } else {
      Optional<Table> schemaTable = this.logicalDag.getSchema().getByName(namePath.getFirst());
      if (schemaTable.isPresent()) {
        if (namePath.getLength() == 1) {
          return schemaTable;
        }

        return schemaTable.flatMap(t-> t.walk(namePath.popFirst()));
      }
      return Optional.empty();
//    }
  }
  public LogicalDag getDag() {
    return this.logicalDag;
  }
}
