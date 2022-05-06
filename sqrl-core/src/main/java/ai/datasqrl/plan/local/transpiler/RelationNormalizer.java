package ai.datasqrl.plan.local.transpiler;

import static ai.datasqrl.parse.util.SqrlNodeUtil.toGroupBy;
import static ai.datasqrl.parse.util.SqrlNodeUtil.toOrderBy;
import static ai.datasqrl.plan.local.transpiler.util.JoinUtil.mergeJoins;

import ai.datasqrl.parse.tree.AliasedRelation;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
import ai.datasqrl.parse.tree.Except;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionRewriter;
import ai.datasqrl.parse.tree.ExpressionTreeRewriter;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Intersect;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.TableSubquery;
import ai.datasqrl.parse.tree.Union;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceOrdinal;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.node.SelectNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import ai.datasqrl.plan.local.transpiler.nodes.schemaRef.TableRef;
import ai.datasqrl.plan.local.transpiler.transforms.JoinContextTransform;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Normalizes an sql query into resolved expressions and references to expressions.
 */
@AllArgsConstructor
@Getter
@Slf4j
public class RelationNormalizer extends AstVisitor<RelationNorm, RelationScope> {

  public RelationNorm normalize(Query query, RelationScope scope) {
    return query.accept(this, scope);
  }

  @Override
  public RelationNorm visitQuery(Query node, RelationScope scope) {
    //TODO: Order + Limit on Query
    return node.getQueryBody().accept(this, scope);
  }

  @Override
  public RelationNorm visitQuerySpecification(QuerySpecification node, RelationScope scope) {
    boolean isSelfInScope = isSelfInScope(node.getFrom());
    scope.setSelfInScope(isSelfInScope);

    Relation relation = JoinContextTransform.addContextTable(node.getFrom(), scope.getContextTable(),
        isSelfInScope);

    RelationNorm fromNorm = relation.accept(this, scope);

    Optional<Expression> where = node.getWhere().map(w -> rewriteExpression(w, true, scope));

    List<SingleColumn> expandSelect = scope.expandSelect(node.getSelect().getSelectItems(), scope);

    Set<ReferenceOrdinal> groupByIndices =
        NormalizerUtils.mapGroupByToOrdinal(node.getGroupBy(), node.getSelect(), expandSelect).stream()
          .map(ReferenceOrdinal::new)
          .collect(Collectors.toSet());

    Optional<Expression> having = analyzeHaving(node, null, expandSelect);

    List<SortItem> sortItems = analyzeOrderBy(expandSelect, node.getOrderBy());

    Optional<Integer> limit = node.getLimit().flatMap(Limit::getIntValue);

    //only analyze select list as the group by and order have been extracted
    List<SingleColumn> columns = expandSelect.stream()
        .map(
            column -> new SingleColumn(column.getLocation(),
                rewriteExpression(column.getExpression(), true, scope),
                SingleColumn.alias(getSelectItemName(column, scope))))
        .collect(Collectors.toList());

    List<ResolvedColumn> parentPrimaryKeys = resolvedParentPrimaryKeys(scope);

    RelationNorm merged = mergeJoins(fromNorm, scope.getAddlJoins());
    SelectNorm selectNorm = SelectNorm.create(node.getSelect(), columns);
    List<Expression> addedPrimaryKeys = new ArrayList<>();
    QuerySpecNorm specNorm = new QuerySpecNorm(Optional.empty(),
        parentPrimaryKeys,
        addedPrimaryKeys,
        selectNorm,
        merged,
        where,
        toGroupBy(groupByIndices),
        having,
        toOrderBy(sortItems),
        node.getLimit(), //todo: limit
        new PrimaryKeyDeriver(scope, selectNorm, groupByIndices, fromNorm).get()
    );

    //Set references for ordinals
    groupByIndices.stream().forEach(f->f.setTable(specNorm));

    //Sometimes PKs need to get added back
    List<Expression> selectExpr = selectNorm.getAsExpressions();
    for (Expression expression : specNorm.getPrimaryKeys()) {
      if (!parentPrimaryKeys.contains(expression) && !selectExpr.contains(expression)) {
        addedPrimaryKeys.add(expression);
      }
    }


    return specNorm;
  }

  @Override
  public RelationNorm visitTableSubquery(TableSubquery node, RelationScope scope) {
    RelationScope subQueryScope = createEmptyScope(scope.getSchema());
    RelationNorm norm = normalize(node.getQuery(), subQueryScope);

    scope.getJoinScopes().put(Name.system("_subquery_" + scope.getInternalIncrementer().incrementAndGet()), norm);

    return norm;
  }

  @Override
  public RelationNorm visitAliasedRelation(AliasedRelation node, RelationScope scope) {
    RelationScope subQueryScope = createEmptyScope(scope.getSchema());
    RelationNorm norm = node.getRelation().accept(this, subQueryScope);

    scope.getJoinScopes().put(node.getAlias().getNamePath().getLast(), norm);

    return norm;
  }

  @Override
  public RelationNorm visitUnion(Union node, RelationScope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public RelationNorm visitIntersect(Intersect node, RelationScope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public RelationNorm visitExcept(Except node, RelationScope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public RelationNorm visitJoin(Join node, RelationScope scope) {
    RelationNorm left = node.getLeft().accept(this, scope);
    RelationNorm right = node.getRight().accept(this, scope);

    Optional<JoinOn> criteria = node.getCriteria().map(j ->
        new JoinOn(j.getLocation(), rewriteExpression(j.getExpression(), scope)));

    return new JoinNorm(Optional.empty(), node.getType(), left, right, criteria);
  }

  /**
   * Expands table identifiers.
   *
   * Local scoped tables are expanded when they are first referenced
   */
  @Override
  public RelationNorm visitTableNode(TableNode node, RelationScope scope) {
    //Attempt to set the self scope
    setSelfScope(scope);
    //special case for explicit context table
    if (node.getNamePath().getLength() == 1 && node.getNamePath().get(0).equals(Name.SELF_IDENTIFIER)) {
      RelationNorm tableNorm = scope.getJoinScopes().get(Name.SELF_IDENTIFIER);
      scope.getHasExpandedSelf().set(true);
      return tableNorm;
    }

    RelationNorm result = TablePathToJoins.expand(node.getNamePath(), scope);

    Name alias = getTableName(node, scope);
    scope.getJoinScopes().put(alias, result.getRightmost());

    return result;
  }

  private Name getSelectItemName(SingleColumn column, RelationScope scope) {
    if (column.getAlias().isPresent()) {
      return column.getAlias().get().getNamePath().getLast();
    } else if (column.getExpression() instanceof Identifier) {
      return ((Identifier)column.getExpression()).getNamePath().getLast();
    } else if (scope.getIsExpression().isPresent() && scope.getIsExpression().get()) {
      return scope.getTargetName().get();
    }
    return Name.system("_expr");
  }

  private List<ResolvedColumn> resolvedParentPrimaryKeys(RelationScope scope) {
    if (scope.getContextTable().isPresent()) {
      //todo check for missing context table so we can add it
      RelationNorm tableNorm = scope.getJoinScopes().get(Name.SELF_IDENTIFIER);
      return (List)new ArrayList<>(tableNorm.getPrimaryKeys());
    }

    return List.of();
  }

  private void setSelfScope(RelationScope scope) {
    if (scope.getJoinScopes().containsKey(Name.SELF_IDENTIFIER)) {
      return;
    }
    if (scope.getContextTable().isPresent()) {
      Table table = scope.getContextTable().get();
      TableNodeNorm tableNodeNorm = new TableNodeNorm(Optional.empty(), table.getPath(),
          Optional.of(Name.SELF_IDENTIFIER), new TableRef(table), true);
      scope.getJoinScopes().put(Name.SELF_IDENTIFIER, tableNodeNorm);
    }
  }

  private List<SortItem> analyzeOrderBy(List<SingleColumn> selectList, Optional<OrderBy> orders) {
    if (orders.isEmpty()) {
      return new ArrayList<>();
    }
    List<SortItem> newSortItems = new ArrayList<>();
    for (SortItem sortItem : orders.get().getSortItems()) {
      //Look at the alias name, if its the alias name then return ordinal
      //Look in select list, if the identifier is in there exactly then return ordinal
      //Else: rewrite expression
      int ordinal = getSelectListOrdinal(selectList, sortItem.getSortKey());
      if (ordinal != -1) {
        newSortItems.add(new SortItem(sortItem.getLocation(), new ReferenceOrdinal(ordinal), sortItem.getOrdering()));
      } else {
        newSortItems.add(sortItem);
      }
    }

    return newSortItems;
  }

  private int getSelectListOrdinal(List<SingleColumn> selectList, Expression expression) {
    //Check in selectlist for alias
    for (int i = 0; i < selectList.size(); i++) {
      SingleColumn item = selectList.get(i);
      if (item.getAlias().isPresent() && item.getAlias().get().equals(expression)) {
        return i;
      }
    }

    return selectList.stream()
        .map(e->e.getExpression()).collect(Collectors.toList())
        .indexOf(expression);
  }

  private Optional<Expression> analyzeHaving(QuerySpecification node, RelationScope scope,
      List<SingleColumn> selectList) {
    if (node.getHaving().isPresent()) {
      return Optional.of(rewriteHavingExpression(node.getHaving().get(), selectList, scope));
    }
    return Optional.empty();
  }

  private Expression rewriteHavingExpression(Expression expression, List<SingleColumn> selectList, RelationScope scope) {
    List<Expression> expressionList = selectList.stream().map(s->s.getExpression()).collect(
        Collectors.toList());

    Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>(){
      @Override
      public Expression rewriteFunctionCall(FunctionCall node, List<SingleColumn> context,
          ExpressionTreeRewriter<List<SingleColumn>> treeRewriter) {
        int index = expressionList.indexOf(node);
        if (index != -1) {
          return new ReferenceOrdinal(index);
        }

        return node;
      }
    }, expression, selectList);
    //remove expressions that are in the select list and replace them with identifiers. The
    // replacing identifier is always a FunctionCall.

    //Also, qualify all of the fields, disallow paths.
    return rewriteExpression(rewritten, false, scope);
  }

  private Expression rewriteExpression(Expression expression, boolean allowPaths, RelationScope scope) {
    ExpressionNormalizer expressionNormalizer = new ExpressionNormalizer(allowPaths);
    Expression rewritten = ExpressionTreeRewriter.rewriteWith(expressionNormalizer, expression, scope);
    scope.getAddlJoins().addAll(expressionNormalizer.getAddlJoins());
    return rewritten;
  }


  protected Optional<OrderBy> rewriteOrderBy(Optional<OrderBy> orderBy, RelationScope scope) {
    if (orderBy.isEmpty()) {
      return Optional.empty();
    }

    List<SortItem> sortItems = new ArrayList<>();
    for (SortItem sortItem : orderBy.get().getSortItems()) {
      Expression key = rewriteExpression(sortItem.getSortKey(), scope);
      sortItems.add(new SortItem(sortItem.getLocation(), key, sortItem.getOrdering()));
    }

    OrderBy order = new OrderBy(orderBy.get().getLocation(), sortItems);
    return Optional.of(order);
  }

  private Expression rewriteExpression(Expression expression, RelationScope scope) {
    return ExpressionTreeRewriter.rewriteWith(new ExpressionNormalizer(false), expression, scope);
  }

  private boolean isSelfInScope(Relation node) {
    AtomicBoolean atomicBoolean = new AtomicBoolean(false);
    node.accept(new DefaultTraversalVisitor<>() {
      @Override
      public Object visitTableNode(TableNode node, Object context) {
        if (node.getNamePath().getLength() == 1 &&
            node.getNamePath().getFirst().equals(Name.SELF_IDENTIFIER)) {
          atomicBoolean.set(true);
        }
        return null;
      }
    }, null);

    return atomicBoolean.get();
  }

  private RelationScope createEmptyScope(Schema schema) {
    return new RelationScope(schema,
        Optional.empty(), Optional.empty(), Optional.empty());
  }

  private Name getTableName(TableNode node, RelationScope scope) {
    return node.getAlias().or(()->node.getNamePath().getLength() == 1 ? Optional.of(node.getNamePath().getFirst()) : Optional.empty())
        //If we're in a table path, the fields cannot be referenced using the path syntax
        .orElseGet(()->Name.system("_internal$" + scope.getInternalIncrementer().incrementAndGet()));
  }
}