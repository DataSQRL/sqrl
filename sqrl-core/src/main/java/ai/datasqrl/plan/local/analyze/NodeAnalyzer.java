package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.Check;
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
import ai.datasqrl.parse.tree.LongLiteral;
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
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.local.Errors;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedFunctionCall;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.RootTableField;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Getter;

/**
 * Produces an Analysis object of a script/statement
 */
public class NodeAnalyzer extends DefaultTraversalVisitor<Scope, Scope> {
  protected final ErrorCollector errors;
  protected final Namespace namespace;

  @Getter
  protected Analysis analysis;

  public NodeAnalyzer(ErrorCollector errors, Analysis analysis, Namespace namespace) {
    this.errors = errors;
    this.analysis = analysis;
    this.namespace = namespace;
  }

  @Override
  public Scope visitDistinctAssignment(DistinctAssignment node, Scope scope) {
    Scope tableScope = node.getTableNode().accept(this, scope);
    node.getPartitionKeyNodes().forEach(pk -> pk.accept(this, tableScope));
    node.getOrder().forEach(o -> o.accept(this, tableScope));
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

    Check.state_(node.getInverseNode().isEmpty(), () -> node.getInverseNode().get(),
        Errors.NOT_YET_IMPLEMENTED);
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
    }

    return queryBodyScope;
  }

  /**
   * QuerySpecification contains what you expect in a standard query.
   */
  @Override
  public Scope visitQuerySpecification(QuerySpecification node, Scope scope) {
    if (scope.getContextTable().isPresent() && !hasExplicitSelfTable(node.getFrom())) {
      /* Needs a self table added to transpiled query */
      analysis.getNeedsSelfTableJoin().add(node);
    }

    Scope relScope = node.getFrom().accept(this, scope);

    node.getSelect().accept(this, relScope);
    node.getWhere().map(w -> w.accept(this, relScope));
    node.getGroupBy().map(g -> g.accept(this, relScope));
    node.getHaving().map(h -> h.accept(this, relScope));
    node.getOrderBy().map(o -> o.accept(this, relScope));

    return relScope;
  }

  @Override
  public Scope visitSelect(Select node, Scope context) {
    List<SingleColumn> selectItems = new ArrayList<>();
    for (SelectItem selectItem : node.getSelectItems()) {
      if (selectItem instanceof SingleColumn) {
        SingleColumn singleColumn = (SingleColumn) selectItem;
        selectItem.accept(this, context);
        selectItems.add(singleColumn);

        Check.state(context.getIsExpression().isPresent() && context.getIsExpression().get() ||
                singleColumn.getExpression() instanceof Identifier || singleColumn.getAlias()
                .isPresent(),
            selectItem, Errors.UNNAMED_QUERY_COLUMN);
      } else if (selectItem instanceof AllColumns) {
        if (((AllColumns) selectItem).getPrefix().isPresent()) {
          NamePath aliasPath = ((AllColumns) selectItem).getPrefix().get();
          Check.state(aliasPath.getLength() == 1, selectItem, Errors.INVALID_STAR_ALIAS);

          ResolvedNamePath namePath = context.getJoinScopes().get(aliasPath.getFirst());
          List<SingleColumn> resolvedItems = namePath.getToTable().getVisibleColumns().stream()
              .map(c -> new Identifier(selectItem.getLocation(), c.getName()))
              .map(i -> new SingleColumn(selectItem.getLocation(), i, Optional.empty()))
              .collect(Collectors.toList());
          selectItems.addAll(resolvedItems);
        } else {
          List<SingleColumn> resolvedItems = context.getJoinScopes().entrySet().stream().filter(
                  e -> !(e.getKey().equals(ReservedName.SELF_IDENTIFIER) && !context.isSelfInScope()))
              .flatMap(e -> e.getValue().getToTable().getVisibleColumns().stream())
              .map(c -> new Identifier(selectItem.getLocation(), c.getName()))
              .map(i -> new SingleColumn(selectItem.getLocation(), i, Optional.empty()))
              .collect(Collectors.toList());
          selectItems.addAll(resolvedItems);
        }
      }
    }

    Set<Name> names = new LinkedHashSet<>();
    for (SingleColumn item : selectItems) {
      Name columnName = getSelectItemName(item, context);
      Check.state(!names.contains(columnName), item, Errors.DUPLICATE_COLUMN_NAME);
      names.add(columnName);
    }

    context.setFieldNames(new ArrayList<>(names));
    context.setSelectItems(selectItems);

    return context;
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

  /**
   * Finds the ordinal for each group by (as an alias or as an expression)
   */
  @Override
  public Scope visitSimpleGroupBy(SimpleGroupBy node, Scope scope) {
    Scope groupByScope = Scope.createGroupOrSortScope(scope);

    //validate each expression is in the select list by identifier or alias
    List<Integer> groupByOrdinals = new ArrayList<>();
    List<Expression> expressions = scope.getSelectItems().stream().map(SingleColumn::getExpression)
        .collect(Collectors.toList());
    for (Expression expression : node.getExpressions()) {
      if (expression instanceof Identifier) {
        Identifier identifier = (Identifier) expression;
        //Aliased identifier
        if (identifier.getNamePath().getLength() == 1 && scope.getFieldNames()
            .contains(identifier.getNamePath().getFirst())) {
          Integer index = scope.getFieldNames()
              .indexOf(identifier.getNamePath().getFirst());
          groupByOrdinals.add(index);
          continue;
        }
      }

      Check.state(expressions.contains(expression), expression, Errors.GROUP_BY_COLUMN_MISSING);
      groupByOrdinals.add(expressions.indexOf(expression));
      if (!(expression instanceof Identifier)) {
        //Todo: warn to use aliases instead of full literal expressions
      }
    }

    analysis.setGroupByOrdinals(groupByOrdinals);

    return groupByScope;
  }

  /**
   * Order by statements may be unique and expanded for use in the API, otherwise we want to replace
   * them with ordinals.
   */
  @Override
  public Scope visitOrderBy(OrderBy node, Scope scope) {
    Scope orderByScope = Scope.createGroupOrSortScope(scope);

    List<SortItem> sortItems = new ArrayList<>();
    //Replace some sort items with literals before analyzing
    List<Expression> expressions = scope.getSelectItems().stream().map(SingleColumn::getExpression)
        .collect(Collectors.toList());
    List<Expression> uniqueOrderExpressions = new ArrayList<>();
    for (SortItem sortItem : node.getSortItems()) {
      if (sortItem.getSortKey() instanceof Identifier) {
        Identifier identifier = (Identifier) sortItem.getSortKey();
        //Aliased identifier
        if (identifier.getNamePath().getLength() == 1 && scope.getFieldNames()
            .contains(identifier.getNamePath().getFirst())) {
          int index = scope.getFieldNames().indexOf(identifier.getNamePath().getFirst());
          LongLiteral ordinal = new LongLiteral(Integer.toString(index + 1));
          SortItem item = new SortItem(sortItem.getLocation(), ordinal, sortItem.getOrdering());
          sortItems.add(item);
          continue;
        }
      }

      if (expressions.contains(sortItem.getSortKey())) {
        int index = expressions.indexOf(sortItem.getSortKey());
        LongLiteral ordinal = new LongLiteral(Integer.toString(index + 1));
        SortItem item = new SortItem(sortItem.getLocation(), ordinal, sortItem.getOrdering());
        sortItems.add(item);
      } else {
        sortItem.getSortKey().accept(this, scope);
        sortItems.add(sortItem);
        uniqueOrderExpressions.add(sortItem.getSortKey());
      }
    }

    analysis.setOrderByExpressions(sortItems);
    analysis.setUniqueOrderExpressions(uniqueOrderExpressions);
    return orderByScope;
  }

  @Override
  public Scope visitTableSubquery(TableSubquery node, Scope scope) {
    throw new RuntimeException("Not yet implemented");
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
   * Resolved table identifiers
   */
  @Override
  public Scope visitTableNode(TableNode node, Scope scope) {
    if (node.getNamePath().getLength() == 1 && node.getNamePath().getFirst()
        .equals(ReservedName.SELF_IDENTIFIER)) {
      scope.setSelfInScope(true);
    }

    Optional<List<ResolvedNamePath>> paths = scope.resolveTable(node.getNamePath());
    Check.state(paths.isPresent(), node, Errors.TABLE_PATH_NOT_FOUND);
    Check.state(paths.isPresent() && paths.get().size() == 1, node, Errors.TABLE_PATH_AMBIGUOUS);

    ResolvedNamePath table = paths.get().get(0);
    Field last = table.getLast();
    Check.state(last instanceof RootTableField || last instanceof Relationship, node,
        Errors.TABLE_PATH_TYPE_INVALID);

    //todo: note: common mistake is something like: select Orders._uuid from Orders o;
    Name alias = getTableName(node, scope);
    scope.getJoinScopes().put(alias, table);

    analysis.getResolvedNamePath().put(node, table);
    analysis.getTableAliases().put(node, alias);

    return scope;
  }

  @Override
  public Scope visitIdentifier(Identifier node, Scope scope) {
    Optional<List<ResolvedNamePath>> paths = scope.resolveNamePath(node.getNamePath());
    Check.state(paths.isPresent(), node, Errors.PATH_NOT_FOUND);
    Check.state(paths.get().size() == 1, node, Errors.PATH_AMBIGUOUS);

    ResolvedNamePath field = paths.get().get(0);
    Check.state(field.getLast() instanceof Column, node, Errors.IDENTIFIER_MUST_BE_COLUMN);
    Check.state(!(field.getPath().size() > 1 && !scope.isAllowIdentifierPaths()), node,
        Errors.PATH_NOT_ALLOWED);
    Check.state(!(field.getPath().size() > 1 && !field.isToOne()), node, Errors.PATH_NOT_TO_ONE);

    analysis.getResolvedNamePath().put(node, field);

    return scope;
  }

  @Override
  public Scope visitFunctionCall(FunctionCall node, Scope scope) {
    Optional<SqrlAwareFunction> functionOptional = namespace.lookupFunction(node.getNamePath());
    Check.state(functionOptional.isPresent(), node, Errors.FUNCTION_NOT_FOUND);

    SqrlAwareFunction function = functionOptional.get();
    if (isLocalAggregate(function, node, scope)) {
      Identifier identifier = (Identifier) node.getArguments().get(0);
      ResolvedNamePath path = scope.resolveNamePathOrThrow(identifier.getNamePath());

      analysis.getResolvedNamePath().put(identifier, path);
      analysis.getIsLocalAggregate().add(node);
    } else {
      for (Expression arg : node.getArguments()) {
        arg.accept(this, scope);
      }

      Check.state(!(node.getOver().isPresent() && !function.requiresOver()), node,
          Errors.FUNCTION_ORDER_UNEXPECTED);
      Check.state(!(node.getOver().isEmpty() && function.requiresOver()), node,
          Errors.FUNCTION_REQUIRES_OVER);
      node.getOver().map(over -> over.accept(this, scope));
    }
    analysis.getResolvedFunctions().put(node, new ResolvedFunctionCall(function));

    return scope;
  }

  private boolean isLocalAggregate(SqrlAwareFunction function, FunctionCall node, Scope scope) {
    if (!(function.isAggregate() && node.getArguments().size() == 1 && node.getArguments()
        .get(0) instanceof Identifier)) {
      return false;
    }

    Identifier identifier = (Identifier) node.getArguments().get(0);
    ResolvedNamePath path = scope.resolveNamePathOrThrow(identifier.getNamePath());
    return path.isToMany();
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
