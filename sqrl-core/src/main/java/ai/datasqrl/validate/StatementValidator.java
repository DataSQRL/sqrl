package ai.datasqrl.validate;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.tree.AllColumns;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.GroupBy;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.JoinDeclaration;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import ai.datasqrl.server.ImportManager;
import ai.datasqrl.server.ImportManager.SourceTableImport;
import ai.datasqrl.validate.scopes.DistinctScope;
import ai.datasqrl.validate.scopes.ImportScope;
import ai.datasqrl.validate.scopes.QueryScope;
import ai.datasqrl.validate.scopes.StatementScope;
import ai.datasqrl.validate.scopes.ValidatorScope;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * The validator does a smell test of the statement and assigns scopes for the transform step.
 */
@Slf4j
@AllArgsConstructor
@Getter
public class StatementValidator {

  private ImportManager importManager;
  private Schema schema;

  protected final ErrorCollector errors = ErrorCollector.root();

  /**
   * Produces a validated statement with scopes in this object.
   *
   * @param node
   */
  public StatementScope validate(Node node) {
    Visitor visitor = new Visitor();
    return node.accept(visitor, null);
  }

  public class Visitor extends AstVisitor<StatementScope, Void> {

    private final AtomicBoolean importResolved = new AtomicBoolean(false);

    @Override
    public StatementScope visitNode(Node node, Void context) {
      throw new RuntimeException(
          String.format("Could not process node %s : %s", node.getClass().getName(), node));
    }

    //TODO:
    // import ds.*;
    // import ns.function;
    // import ds;
    @Override
    public StatementScope visitImportDefinition(ImportDefinition node, Void context) {
      if (importResolved.get()) {
        throw new RuntimeException(
            String.format("Import statement must be in header %s", node.getNamePath()));
      }

      if (node.getNamePath().getLength() > 2) {
        throw new RuntimeException(
            String.format("Cannot import identifier: %s", node.getNamePath()));
      }

//      importManager.setTableFactory(new SourceTableFactory(planner));

      SourceTableImport importSource = importManager
          .resolveTable(node.getNamePath().get(0), node.getNamePath().get(1), node.getAliasName(),
              errors);

      ImportScope importScope = new ImportScope(node.getNamePath(), node.getAliasName(),
          importSource);
      HashMap<Node, ValidatorScope> scopes = new HashMap();
      scopes.put(node, importScope);
      return new StatementScope(Optional.empty(), null, scopes);
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

      Optional<Table> table = walk(schema, namePath.popLast());

      Preconditions.checkState(table.isPresent(), "Expression cannot be assigned to root");

      ExpressionValidator expressionValidator = new ExpressionValidator();
      QueryScope scope = new QueryScope(table, Map.of(Name.SELF_IDENTIFIER, table.get()));
      expressionValidator.validate(expression, scope);

      return new StatementScope(table, assignment.getNamePath(), expressionValidator.getScopes());
    }

    private Optional<Table> walk(Schema schema, NamePath namePath) {
      Table table = schema.getByName(namePath.getFirst()).get();
      return table.walk(namePath.popFirst());
    }
//
//    @Override
//    public StatementScope visitCreateSubscription(CreateSubscription subscription, Void context) {
//      return null;
//    }
//

    /**
     * Validates tables and column names.
     */
    @SneakyThrows
    @Override
    public StatementScope visitDistinctAssignment(DistinctAssignment node, Void context) {
      Preconditions.checkState(node.getNamePath().getLength() == 1,
          "Distinct node must be on root (tbd expand)");
      Optional<Table> tableOpt = schema.getByName(node.getTable());
      Preconditions.checkState(tableOpt.isPresent(),
          "Table could not be found: " + node.getTable());
      Table table = tableOpt.get();
      List<Field> partitionKeys = new ArrayList<>();
      for (Name key : node.getPartitionKeys()) {
        Optional<Field> field = table.getFields().getByName(key);
        Preconditions.checkState(field.isPresent(), "Partition key could not be found: " + key);
        partitionKeys.add(field.get());
      }
      List<Field> sortFields = new ArrayList<>();
      for (SortItem sort : node.getOrder()) {
        Preconditions.checkState(sort.getSortKey() instanceof Identifier);
        Optional<Field> field = table.getFields()
            .getByName(((Identifier) sort.getSortKey()).getNamePath().getFirst());
        Preconditions.checkState(field.isPresent(), "Sort Item could not be found: " +
            ((Identifier) sort.getSortKey()).getNamePath().getFirst());
        sortFields.add(field.get());
      }

      Map<Node, ValidatorScope> scopes = Map.of(node,
          new DistinctScope(table, partitionKeys, sortFields));
      return new StatementScope(Optional.empty(), node.getNamePath(), scopes);
    }

//    @Override
//    public StatementScope visitJoinDeclaration(JoinDeclaration node, Void context) {
//      NamePath namePath = node.getNamePath();
//
//      QueryValidator validator = new QueryValidator(schema);
//
//      Table table = schema.getByName(namePath.getFirst()).get()
//          .walk(namePath.popFirst().popLast()).get();
//
//      Map<Name, Table> fieldScope = new HashMap<>();
//      fieldScope.put(Name.SELF_IDENTIFIER, table);
//
//      QueryScope queryScope = new QueryScope(Optional.of(table), fieldScope);
//      ValidatorScope scope = node.getInlineJoin().accept(validator, queryScope);
//
//      return new StatementScope(Optional.of(table), node.getNamePath(), Map.of(node, scope));
//    }
//
//    private Name getLastTableName(JoinDeclaration node) {
//      Relation rel = node.getInlineJoin().getRelation();
//      while (rel instanceof Join) {
//        rel = ((Join) rel).getRight();
//      }
//      TableNode table = (TableNode) rel;
//
//      return table.getAlias().orElse(Name.system(table.getNamePath().toString()));
//    }
  }
}
