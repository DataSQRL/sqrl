package ai.datasqrl.validate;

import static ai.datasqrl.parse.util.SqrlNodeUtil.mapToOrdinal;

import ai.datasqrl.function.FunctionLookup;
import ai.datasqrl.parse.tree.AllColumns;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Except;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.GroupBy;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Intersect;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SetOperation;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.Union;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import ai.datasqrl.validate.aggs.AggregationDetector;
import ai.datasqrl.validate.paths.BaseTablePath;
import ai.datasqrl.validate.paths.RelativeTablePath;
import ai.datasqrl.validate.paths.TablePath;
import ai.datasqrl.validate.scopes.ExpressionScope;
import ai.datasqrl.validate.scopes.QueryScope;
import ai.datasqrl.validate.scopes.TableScope;
import ai.datasqrl.validate.scopes.ValidatorScope;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class RelationValidator extends AstVisitor<ValidatorScope, QueryScope> {
  Schema schema;
  private final AggregationDetector aggregationDetector = new AggregationDetector(
      new FunctionLookup());

  private final Map<TableNode, TableScope> tableScopes = new HashMap<>();
  private final Map<Name, Table> joinScopes = new HashMap<>();
  private final Map<Node, ValidatorScope> scopes = new HashMap<>();

  @Override
  public ValidatorScope visitQuerySpecification(QuerySpecification node, QueryScope scope) {
    ValidatorScope sourceValidateScope = node.getFrom().accept(this, scope);

    Select expandedSelect = expandStar(node.getSelect(), scope);

    // We're doing a lot of transformations so convert grouping conditions to ordinals.
    Optional<GroupBy> groupBy = node.getGroupBy().map(group -> mapToOrdinal(expandedSelect, group));

    Optional<OrderBy> orderBy = node.getOrderBy().map(order -> rewriteOrderBy(order, scope));

    // Qualify other expressions
//    (Select)expandedSelect.accept(this, scope);
//    Optional<Expression> where = node.getWhere().map(w-> validateExpression(w, scope));
//    Optional<Expression> having = node.getHaving().map(h-> validateExpression(h, scope)); //todo identify and replace having clause

    return createValidateScope(scope);
  }

  private OrderBy rewriteOrderBy(OrderBy order, QueryScope scope) {
    return null;
  }


  /**
   * Note: This node is not walked from the rhs of a join.
   * <p>
   * Expand the path, if applicable. The first name will always be either a SELF or a base table.
   * The remaining path are always relationships and are wrapped in a join. The final table is
   * aliased by either a given alias or generated alias.
   */
  @Override
  public ValidatorScope visitTableNode(TableNode tableNode, QueryScope scope) {
    NamePath namePath = tableNode.getNamePath();
    Name aliasName = tableNode.getAlias().orElseGet(() -> Name.system(namePath.toString()));

    Table table;
    TablePath tablePath;
    if (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) {
      table = scope.getContextTable().get();
      tablePath = RelativeTablePath.resolve(table, namePath.popFirst());
    } else { //Table is in the schema
      table = schema.getByName(namePath.getFirst()).get();
      tablePath = BaseTablePath.resolve(table, namePath.popFirst());
    }

    scope.getFieldScope().put(aliasName, table.walk(namePath.popFirst()).get());

    TableScope tableScope = new TableScope(tableNode, tablePath, aliasName);
    tableScopes.put(tableNode, tableScope);
    joinScopes.put(aliasName, table);
    scopes.put(tableNode, tableScope);
    return createValidateScope(scope);
  }

  /**
   * Joins have the assumption that the rhs will always be a TableNode so the rhs will be unwrapped
   * at this node, so it can provide additional criteria to the join.
   * <p>
   * The rhs first table name will either be a join scoped alias or a base table. If it is a join
   * scoped alias, we will use that alias information to construct additional criteria on the join,
   * otherwise it'll be a cross join.
   */
  @Override
  public ValidatorScope visitJoin(Join node, QueryScope scope) {
    ValidatorScope left = node.getLeft().accept(this, scope);

    TableNode rhs = (TableNode) node.getRight();
    Name lastAlias = rhs.getAlias().isPresent()
        ? rhs.getAlias().get()
        : Name.system(rhs.getNamePath().toString());

    NamePath rhsName = rhs.getNamePath();

    Table table;
    TablePath tablePath;
    //A join traversal, e.g. FROM orders AS o JOIN o.entries
    if (scope.getFieldScope().containsKey(rhsName.getFirst())) {
      table = scope.getFieldScope().get(rhsName.getFirst());
      tablePath = RelativeTablePath.resolve(table, rhsName.popFirst());
    } else { //A regular join: FROM Orders JOIN entries
      table = schema.getByName(rhs.getNamePath().get(0)).get();
      tablePath = BaseTablePath.resolve(table, rhsName.popFirst());
    }

    scope.getFieldScope().put(lastAlias, table);
    this.joinScopes.put(lastAlias, table);
    this.tableScopes.put(rhs, new TableScope(rhs, tablePath, lastAlias));

    if (node.getCriteria().isPresent()) {
      ExpressionScope expressionScope = validateExpression(
          ((JoinOn) node.getCriteria().get()).getExpression(), scope);
      //Additional validation:
      //No paths
    }
    return createValidateScope(scope);
  }

  @Override
  public ValidatorScope visitUnion(Union node, QueryScope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public ValidatorScope visitIntersect(Intersect node, QueryScope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public ValidatorScope visitExcept(Except node, QueryScope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public ValidatorScope visitSetOperation(SetOperation node, QueryScope scope) {
    return null;
  }

  @Override
  public ValidatorScope visitSelect(Select select, QueryScope scope) {
    select.getSelectItems().stream()
        .forEach(s -> validateExpression(((SingleColumn) s).getExpression(), scope));

    return createValidateScope(scope);
  }

  /**
   * Expands STAR alias
   */
  private Select expandStar(Select select, QueryScope scope) {
    List<SelectItem> expanded = new ArrayList<>();
    for (SelectItem item : select.getSelectItems()) {
      if (item instanceof AllColumns) {
        Optional<Name> starPrefix = ((AllColumns) item).getPrefix()
            .map(e -> e.getFirst());
        List<Identifier> fields = scope.resolveFieldsWithPrefix(starPrefix);
        for (Identifier identifier : fields) {
          expanded.add(new SingleColumn(identifier, Optional.empty()));
        }
      } else {
        expanded.add(item);
      }
    }
    return new Select(expanded);
  }

  private ValidatorScope createValidateScope(QueryScope parentValidatorScope) {
    return new QueryScope(parentValidatorScope.getContextTable(),
        parentValidatorScope.getFieldScope());
  }

  private ExpressionScope validateExpression(Expression expression, QueryScope scope) {
    ExpressionValidator analyzer = new ExpressionValidator();
    ExpressionScope expr = analyzer.validate(expression, scope);
    this.scopes.putAll(analyzer.getScopes());
    return expr;
  }
}
