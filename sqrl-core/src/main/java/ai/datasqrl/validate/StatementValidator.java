package ai.datasqrl.validate;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.ViewExpander;
import ai.datasqrl.plan.calcite.CalcitePlanner;
import ai.datasqrl.schema.ShadowingContainer;
import ai.datasqrl.schema.Table;
import ai.datasqrl.validate.imports.ImportManager;
import ai.datasqrl.validate.imports.ImportManager.SourceTableImport;
import ai.datasqrl.validate.scopes.ImportScope;
import ai.datasqrl.validate.scopes.QueryScope;
import ai.datasqrl.validate.scopes.StatementScope;
import ai.datasqrl.validate.scopes.ValidatorScope;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * The validator does a smell test of the statement and assigns scopes
 * for the transform step.
 */
@Slf4j
@AllArgsConstructor
@Getter
public class StatementValidator {
  private ImportManager importManager;
  private CalcitePlanner planner;
  private ShadowingContainer<Table> schema;
  private ViewExpander viewExpander;

  protected final ErrorCollector errors = ErrorCollector.root();

  /**
   * Produces a validated statement with scopes in this object. 
   * @param node
   */
  public StatementScope validate(Node node) {
    Visitor visitor = new Visitor(this);
    return node.accept(visitor, null);
  }

  public class Visitor extends AstVisitor<StatementScope, Void> {
    private final AtomicBoolean importResolved = new AtomicBoolean(false);
    private final StatementValidator analyzer;

    public Visitor(StatementValidator analyzer) {
      this.analyzer = analyzer;
    }

    @Override
    public StatementScope visitNode(Node node, Void context) {
      throw new RuntimeException(String.format("Could not process node %s : %s", node.getClass().getName(), node));
    }

    //TODO:
    // import ds.*;
    // import ns.function;
    // import ds;
    @Override
    public StatementScope visitImportDefinition(ImportDefinition node, Void context) {
      if (importResolved.get()) {
        throw new RuntimeException(String.format("Import statement must be in header %s", node.getNamePath()));
      }

      if (node.getNamePath().getLength() > 2) {
        throw new RuntimeException(String.format("Cannot import identifier: %s", node.getNamePath()));
      }

//      importManager.setTableFactory(new SourceTableFactory(planner));

      SourceTableImport importSource = importManager
          .resolveTable2(node.getNamePath().get(0), node.getNamePath().get(1), node.getAliasName(), errors);

      ImportScope importScope = new ImportScope(node.getNamePath(), node.getAliasName(), importSource);
      HashMap<Node, ValidatorScope> scopes = new HashMap();
      scopes.put(node, importScope);
      return new StatementScope(Optional.empty(), scopes);
    }
//
//    @Override
//    public StatementScope visitQueryAssignment(QueryAssignment queryAssignment, Void context) {
//      NamePath namePath = queryAssignment.getNamePath();
//      Query query = queryAssignment.getQuery();
//
//      Optional<Table> contextTable = namePath.getPrefix().flatMap(p-> getTable(p));
//
//      QueryValidator statementAnalyzer = new QueryValidator(analyzer);
//      ValidatorScope scope = query.accept(statementAnalyzer, new QueryScope(contextTable, new HashMap<>()));
//
//      return null;
//    }

    @Override
    public StatementScope visitExpressionAssignment(ExpressionAssignment assignment, Void context) {
      NamePath namePath = assignment.getNamePath();
      Expression expression = assignment.getExpression();

      Optional<Table> table = schema.getByName(namePath.getFirst());
      Preconditions.checkState(table.isPresent(), "Expression cannot be assigned to root");

      ExpressionValidator expressionValidator = new ExpressionValidator();
      QueryScope scope = new QueryScope(table, Map.of(Name.SELF_IDENTIFIER, table.get()));
      expressionValidator.validate(expression, scope);

      return new StatementScope(table, expressionValidator.getScopes());
    }
//
//    @Override
//    public StatementScope visitCreateSubscription(CreateSubscription subscription, Void context) {
//      return null;
//    }
//
//    @SneakyThrows
//    @Override
//    public StatementScope visitDistinctAssignment(DistinctAssignment node, Void context) {
//      Optional<Table> refTable = getTable(node.getTable().toNamePath());
//
//      Table table = new TableFactory().create(node.getNamePath(), node.getTable());
//
//      //TODO: Validate that everything is valid
//      return null;
//    }
//
//    @Override
//    public StatementScope visitJoinDeclaration(JoinDeclaration node, Void context) {
//      NamePath namePath = node.getNamePath();
//
//      Name name = getLastTableName(node);
//      QueryValidator statementAnalyzer = new QueryValidator(analyzer);
//
//      Select select = new Select(Optional.empty(), false, List.of(new AllColumns(name.toNamePath())));
//      Query querySpec = new Query(new QuerySpecification(node.getLocation(),
//          select,
//          node.getInlineJoin().getRelation(),
//          Optional.<Expression>empty(),
//          Optional.<GroupBy>empty(),
//          Optional.<Expression>empty(),
//          node.getInlineJoin().getOrderBy(),
//          node.getInlineJoin().getLimit()),
//          Optional.empty(),
//          Optional.empty()
//      );
//
//      Optional<Table> ctxTable = getTable(namePath.getPrefix().get());
//
//
////      ValidateScope scope = querySpec.accept(statementAnalyzer, new ValidateScope(ctxTable, querySpec, new LinkedHashMap<>(),new LinkedHashMap<>(),
////          false, null));
//
//      return null;
//    }
//
//    private Name getLastTableName(JoinDeclaration node) {
//      Relation rel = node.getInlineJoin().getRelation();
//      while (rel instanceof Join) {
//        rel = ((Join) rel).getRight();
//      }
//      TableNode table = (TableNode) rel;
//
//      return table.getAlias().orElse(table.getNamePath().getFirst());
//    }
//  }
//
//  public Optional<Table> getTable(NamePath namePath) {
//      Optional<Table> schemaTable = this.schema.getByName(namePath.getFirst());
//      if (schemaTable.isPresent()) {
//        if (namePath.getLength() == 1) {
//          return schemaTable;
//        }
//
//        return schemaTable.flatMap(t-> t.walk(namePath.popFirst()));
//      }
//      return Optional.empty();
////    }
  }
}
