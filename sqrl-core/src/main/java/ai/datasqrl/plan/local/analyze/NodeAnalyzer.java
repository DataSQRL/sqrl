package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.function.FunctionMetadataProvider;
import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.function.calcite.CalciteFunctionMetadataProvider;
import ai.datasqrl.parse.tree.AllColumns;
import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.Except;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Intersect;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.JoinDeclaration;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SetOperation;
import ai.datasqrl.parse.tree.SimpleGroupBy;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.parse.tree.SubqueryExpression;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.TableSubquery;
import ai.datasqrl.parse.tree.Union;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedFunctionCall;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.Multiplicity;
import graphql.com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Getter;

/**
 * Produces an Analysis object of a script/statement
 */
public class NodeAnalyzer extends DefaultTraversalVisitor<Scope, Scope> {

  private static final FunctionMetadataProvider functionMetadataProvider =
      new CalciteFunctionMetadataProvider(
      SqrlOperatorTable.instance());

  protected final ErrorCollector errors;
  protected final boolean allowPaths = false;
  protected final Namespace namespace;

  @Getter
  protected Analysis analysis;

  public NodeAnalyzer(
      ErrorCollector errors, Analysis analysis, Namespace namespace) {
    this.errors = errors;
    this.analysis = analysis;
    this.namespace = namespace;
  }

  @Override
  public Scope visitDistinctAssignment(DistinctAssignment node, Scope scope) {
    node.getTableNode().accept(this, scope);
    node.getPartitionKeyNodes().forEach(pk -> pk.accept(this, scope));
    node.getOrder().forEach(o -> o.accept(this, scope));
    return null;
  }

  @Override
  public Scope visitJoinAssignment(JoinAssignment node, Scope context) {
    node.getJoinDeclaration().accept(this, context);
    return null;
  }

  @Override
  public Scope visitExpressionAssignment(ExpressionAssignment node, Scope context) {
    node.getExpression().accept(this, context);
    return null;
  }

  @Override
  public Scope visitQueryAssignment(QueryAssignment queryAssignment, Scope context) {
    Scope queryScope = queryAssignment.getQuery().accept(this, context);
    return queryScope;
  }

  @Override
  public Scope visitJoinDeclaration(JoinDeclaration node, Scope context) {
    Scope relScope = node.getRelation().accept(this, context);

    node.getOrderBy().map(o -> o.accept(this, relScope));
    node.getInverseNode().map(i -> i.accept(this, relScope));

    return relScope;
  }

  /**
   * SET operations define a limit & an order on the outside:
   *
   * <p>
   * SELECT * FROM Product
   * <p>
   * UNION
   * <p>
   * SELECT * FROM Product
   * <p>
   * LIMIT 5; <-- refers to limit on set
   * </p>
   */
  @Override
  public Scope visitQuery(Query node, Scope scope) {
    Scope queryBodyScope = node.getQueryBody().accept(this, scope);
    if (node.getOrderBy().isPresent()) {
      node.getOrderBy().map(o -> o.accept(this, scope));
      //Disallow order & limit in union statements (?)
    }

    return queryBodyScope;
  }

  /**
   * QuerySpecification contains what you expect in a query.
   * <p>
   * 1. Order/group/having should refer to its alias or issue a warning 2.
   */
  @Override
  public Scope visitQuerySpecification(QuerySpecification node, Scope scope) {
    boolean isSelfInScope = hasExplicitSelfTable(node.getFrom());
    /* Desugar self joins. If we're nested, we need to join a self table if one is not present. */
    if (scope.getContextTable().isPresent()) {
      if (!isSelfInScope) {
        analysis.getNeedsSelfTableJoin().add(node);
      }
      Scope.addSelfToScope(scope);
    }

    Scope relScope = node.getFrom().accept(this, scope);

    analysis.getScopes().put(node, relScope);
    node.getSelect().accept(this, relScope);
    node.getWhere().map(w -> w.accept(this, relScope));

    //Rescope for group/having/order
//    List<SingleColumn> expandSelect = scope.expandSelect(node.getSelect().getSelectItems(),
//    scope);
//    analysis.getSelectExpressions().put(node, expandSelect);
    //TODO: If there is a select *, store the visible fields in the current scope.
    // Alternatively, keep the scope around and resolve it later (better)

    //TODO: Group by/having/order must refer to an item in the expanded select list. Repeated
    // select list items should be met with a warning in the future, currently disallow them.
    // This is because it could contain local aggregate statements that aren't expandable here
    // It's not hard, the logic in the git history somewhere
    node.getGroupBy().map(g -> g.accept(this, relScope));
    //Todo: Having must use select list scope.
//    node.getHaving().map(h -> h.accept(this, relScope));
    node.getOrderBy().map(o -> o.accept(this, relScope));

    return relScope;
  }

  @Override
  public Scope visitSelect(Select node, Scope context) {
    List<Name> selectNames = new ArrayList<>();

    for (SelectItem selectItem : node.getSelectItems()) {

      if (selectItem instanceof SingleColumn) {
        selectNames.add(getSelectItemName((SingleColumn) selectItem, context));
      } else if (selectItem instanceof AllColumns) {
        if (((AllColumns) selectItem).getPrefix().isPresent()) {
          //Wrong, need to uniquely alias columns
          List<Identifier> identifiers = context.resolveFieldsWithPrefix(
              ((AllColumns) selectItem).getPrefix().map(p -> p.getLast()));
          selectNames.addAll(identifiers.stream().map(i -> i.getNamePath().getLast())
              .collect(Collectors.toList()));
        }
      } else {
        throw new RuntimeException();
      }

    }
    context.setFieldNames(selectNames);
    return super.visitSelect(node, context);
  }

  @Override
  public Scope visitSimpleGroupBy(SimpleGroupBy node, Scope scope) {
    Scope groupByScope = Scope.createGroupOrSortScope(scope);
    node.getExpressions().forEach(e -> e.accept(this, groupByScope));

    return groupByScope;
  }


  @Override
  public Scope visitOrderBy(OrderBy node, Scope scope) {
    Scope orderByScope = Scope.createGroupOrSortScope(scope);

    node.getSortItems().stream().map(SortItem::getSortKey)
        .forEach(e -> e.accept(this, orderByScope));
    return null;
  }

  @Override
  public Scope visitTableSubquery(TableSubquery node, Scope scope) {

    return node.getQuery().accept(this, scope);
    //add subqueries to analyzer so they can be materialized
    //(What about subquery expressions?)

//    Scope subQueryScope = createEmptyScope(scope.getSchema());
//    Scope norm = node.getQuery().accept(this, subQueryScope);
//
//    scope.getJoinScopes().put(Name.system("_subquery_" + scope.getInternalIncrementer()
//    .incrementAndGet()), norm);
  }

  @Override
  public Scope visitUnion(Union node, Scope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public Scope visitIntersect(Intersect node, Scope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public Scope visitExcept(Except node, Scope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public Scope visitSetOperation(SetOperation node, Scope context) {
    node.getRelations().forEach(e -> e.accept(this, context));
    return null;
  }

  @Override
  public Scope visitJoin(Join node, Scope scope) {
    node.getLeft().accept(this, scope);
    node.getRight().accept(this, scope);

    node.getCriteria().map(j -> j.accept(this, scope));
    return scope;
  }

  @Override
  public Scope visitJoinOn(JoinOn node, Scope context) {
    node.getExpression().accept(this, context);
    return context;
  }

  /**
   * Expands table identifiers.
   * <p>
   * Local scoped tables are expanded when they are first referenced
   */
  @Override
  public Scope visitTableNode(TableNode node, Scope scope) {
    if (node.getNamePath().getLength() == 1 && node.getNamePath().getFirst()
        .equals(ReservedName.SELF_IDENTIFIER)) {
      scope.setSelfInScope(true);
    }

    ResolvedNamePath table = scope.resolveTableOrThrow(node.getNamePath());

    analysis.getResolvedNamePath().put(node, table);

    Name alias = getTableName(node, scope);
    scope.getJoinScopes().put(alias, table);
    analysis.getTableAliases().put(node, alias);

    return scope;
  }

  private Name getSelectItemName(SingleColumn column, Scope scope) {
    if (column.getAlias().isPresent()) {
      return column.getAlias().get().getNamePath().getLast();
    } else if (column.getExpression() instanceof Identifier) {
      return ((Identifier) column.getExpression()).getNamePath().getLast();
    } else if (scope.getIsExpression().isPresent() && scope.getIsExpression().get()) {
      return scope.getTargetName().get();
    }
    return Name.system("_expr");
  }

  @Override
  public Scope visitIdentifier(Identifier node, Scope scope) {
    ResolvedNamePath field = scope.resolveNamePathOrThrow(node.getNamePath());

    //3. If !allowPaths and is path, error and return
//    if (path.getLength() > 1 && !allowPaths) {
//      throw new RuntimeException("Paths encountered where path is not allowed");
//    }
//
//    if (isToMany(field)) {
//      throw new RuntimeException("To-many relationship not expected here:" + path);
//    }

    analysis.getResolvedNamePath().put(node, field);
    return scope;
  }

  @Override
  public Scope visitFunctionCall(FunctionCall node, Scope scope) {
    Optional<SqrlAwareFunction> functionOptional = functionMetadataProvider.lookup(
        node.getNamePath());
    Preconditions.checkState(functionOptional.isPresent(), "Could not find function {}",
        node.getNamePath());
    SqrlAwareFunction function = functionOptional.get();
    analysis.getResolvedFunctions().put(node, new ResolvedFunctionCall(function));

    if (function.isAggregate() && node.getArguments().size() == 1 && node.getArguments()
        .get(0) instanceof Identifier) {
      Identifier identifier = (Identifier) node.getArguments().get(0);

      ResolvedNamePath path = scope.resolveNamePathOrThrow(identifier.getNamePath());
      analysis.getResolvedNamePath().put(identifier, path);
      if (path.getPath().size() > 1 && isToMany(path)) {
        analysis.getIsLocalAggregate().add(node);
        return null;
      }
    }

    for (Expression arg : node.getArguments()) {
      arg.accept(this, scope);
    }

    node.getOver().map(over -> over.accept(this, scope));

    return scope;
  }

  private boolean isToMany(ResolvedNamePath fields) {
    if (fields.getPath().isEmpty()) {
      return false;
    }
    for (Field field : fields.getPath()) {
      if (field instanceof Relationship
          && ((Relationship) field).getMultiplicity() != Multiplicity.MANY) {
        return false;
      }
    }
    return true;
  }

  /**
   * Has a self table as a single defined table:
   * <p>
   * FROM _;
   * <p>
   * Not: FROM _.entries;
   */
  private boolean hasExplicitSelfTable(Relation node) {
    AtomicBoolean atomicBoolean = new AtomicBoolean(false);
    node.accept(new DefaultTraversalVisitor<>() {
      //Don't walk subqueries
      @Override
      public Object visitSubqueryExpression(SubqueryExpression node, Object context) {
        return null;
      }

      @Override
      public Object visitTableSubquery(TableSubquery node, Object context) {
        return null;
      }

      @Override
      public Object visitTableNode(TableNode node, Object context) {
        if (node.getNamePath().getLength() == 1 && node.getNamePath().getFirst()
            .equals(ReservedName.SELF_IDENTIFIER)) {
          atomicBoolean.set(true);
        }
        return null;
      }
    }, null);

    return atomicBoolean.get();
  }

  private Name getTableName(TableNode node, Scope scope) {
    return node.getAlias()
        .or(() -> node.getNamePath().getLength() == 1 ? Optional.of(node.getNamePath().getFirst())
            : Optional.empty())
        //If we're in a table path, the fields cannot be referenced using the path syntax
        .orElseGet(
            () -> Name.system("_internal$" + scope.getInternalIncrementer().incrementAndGet()));
  }
}
