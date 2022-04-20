package ai.datasqrl.transform;

import ai.datasqrl.function.FunctionLookup;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.schema.Table;
import ai.datasqrl.transform.ExpressionTransformer.JoinResult;
import ai.datasqrl.transform.transforms.AddColumnToQuery;
import ai.datasqrl.transform.transforms.AddContextToQuery;
import ai.datasqrl.transform.transforms.ConvertLimitToWindow;
import ai.datasqrl.transform.transforms.TablePathToJoin;
import ai.datasqrl.validate.aggs.AggregationDetector;
import ai.datasqrl.validate.scopes.StatementScope;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RelationTransformer extends AstVisitor<Relation, StatementScope> {
  AggregationDetector aggregationDetector = new AggregationDetector(new FunctionLookup());
  private List<JoinResult> additionalJoins = new ArrayList<>();

  @Override
  public Relation visitQuerySpecification(QuerySpecification node, StatementScope scope) {


    Select select = rewriteSelect(node.getSelect(), scope);

    Relation from = node.getFrom().accept(this, scope);
//
//    Select expandedSelect = expandStar(node.getSelect(), scope);
//
//    // We're doing a lot of transformations so convert grouping conditions to ordinals.
//    Optional<GroupBy> groupBy = node.getGroupBy().map(group -> mapToOrdinal(expandedSelect, group));
//
//    Optional<OrderBy> orderBy = node.getOrderBy().map(order -> rewriteOrderBy(order, scope));
//
//    // Qualify other expressions
//    Optional<Expression> where = node.getWhere().map(w->rewriteExpression(w, scope));
//    Optional<Expression> having = node.getHaving().map(h->rewriteExpression(h, scope)); //todo identify and replace having clause
    Relation joined = appendAdditionalJoins(from);
//    QuerySpecification spec = new QuerySpecification(
//        node.getLocation(),
//        select,
//        from,
//        where,
//        groupBy,
//        having,
//        orderBy,
//        node.getLimit()
//    );

    return rewriteQuerySpec(new QuerySpecification(
        node.getLocation(),
        select,
        joined,
        node.getWhere(),
        node.getGroupBy(),
        node.getHaving(),
        node.getOrderBy(),
        node.getLimit()
    ), scope);
//    return
  }

  public Select rewriteSelect(Select select, StatementScope scope) {
    List<SelectItem> items = select.getSelectItems().stream()
        .map(s->(SingleColumn)s)
        .map(s-> new SingleColumn(rewriteExpression(s.getExpression(), scope), s.getAlias()))
        .collect(Collectors.toList());

    return new Select(select.getLocation(), select.isDistinct(), items);
  }
//
//  private OrderBy rewriteOrderBy(OrderBy order, Scope scope) {
//    List<SortItem> items = order.getSortItems().stream()
//        .map(s->new SortItem(s.getLocation(), rewriteExpression(s.getSortKey(), scope), s.getOrdering()))
//        .collect(Collectors.toList());
//    return new OrderBy(order.getLocation(), items);
//  }
//
  /*
   * Rewrites queries to form compliant sql
   */
  private Relation rewriteQuerySpec(QuerySpecification spec,
      StatementScope scope) {

    Optional<Table> contextTable = scope.getContextTable();

    QuerySpecification rewrittenNode = contextTable
        .map(t->{
          QuerySpecification rewritten = new AddContextToQuery().transform(spec, t);
          return spec.getLimit().map(l -> new ConvertLimitToWindow().transform(rewritten, t))
              .orElse(rewritten);
        })
        .orElse(spec);

    if (/*scope.isExpression() && */contextTable.isPresent()) {
      QuerySpecification query = new AddColumnToQuery().transform(contextTable.get(), scope.getNamePath().getLast(),
          aggregationDetector.isAggregating(spec.getSelect()), rewrittenNode);
      return query;
    }

    return rewrittenNode;
  }

  private Relation appendAdditionalJoins(Relation from) {
    for (JoinResult result : additionalJoins) {
      from = new Join(Optional.empty(), result.getType(), from, result.getRelation(), result.getCriteria());
    }
    return from;
  }

  /**
   * Note: This node is not walked from the rhs of a join.
   *
   * Expand the path, if applicable. The first name will always be either a SELF or a base table.
   * The remaining path are always relationships and are wrapped in a join. The final table
   * is aliased by either a given alias or generated alias.
   */
  @Override
  public Relation visitTableNode(TableNode tableNode, StatementScope scope) {
    TablePathToJoin visitor = new TablePathToJoin(scope);
    return tableNode.accept(visitor, null);
  }

  /**
   * Joins have the assumption that the rhs will always be a TableNode so the rhs will be
   * unwrapped at this node, so it can provide additional criteria to the join.
   *
   * The rhs first table name will either be a join scoped alias or a base table. If it is
   * a join scoped alias, we will use that alias information to construct additional criteria
   * on the join, otherwise it'll be a cross join.
   */
//  @Override
//  public Relation rewriteJoin(Join node, Scope scope, RelationTreeRewriter relationTreeRewriter) {
//    Scope left = node.getLeft().accept(this, scope);
//
//    TableNode rhs = (TableNode)node.getRight();
//    Name lastAlias = rhs.getAlias().isPresent()
//        ? rhs.getAlias().get()
//        : rhs.getNamePath().getLength() > 1
//            ? gen.nextTableAliasName()  //todo needs to be unique
//            : rhs.getNamePath().getFirst();
//
//    //A join traversal, e.g. FROM orders AS o JOIN o.entries
//    if (scope.getJoinScope().containsKey(rhs.getNamePath().getFirst())) {
//      Table baseTable = scope.getJoinScope().get(rhs.getNamePath().getFirst());
//      //Immediately register the field scope so the criteria can be resolved
//      scope.getFieldScope().put(lastAlias, baseTable.walk(rhs.getNamePath().popFirst()).get());
//
//      JoinWalker joinWalker = new JoinWalker();
//      WalkResult result = joinWalker.walk(
//          rhs.getNamePath().getFirst(),
//          Optional.of(lastAlias),
//          rhs.getNamePath().popFirst(),
//          Optional.of((Relation)left.getNode()),
//          rewrite(node.getCriteria(), scope),
//          scope.getJoinScope()
//      );
//
//      return createScope(result.getRelation(), scope);
//    } else { //A regular join: FROM Orders JOIN entries
//      Table table = analyzer.getDag().getSchema().getByName(rhs.getNamePath().get(0)).get();
//      Name firstAlias = AliasUtil.getTableAlias(rhs, 0, gen::nextTableAliasName);
//      scope.getJoinScope().put(firstAlias, table);
//      scope.getFieldScope().put(lastAlias, table.walk(rhs.getNamePath().popFirst()).get());
//
//      TableNode tableNode = new TableNode(Optional.empty(), table.getId().toNamePath(), Optional.of(firstAlias));
//      //If there is one entry in the path, do an inner join w/ the given criteria
//      Type type = rhs.getNamePath().getLength() > 1 || node.getCriteria().isEmpty()
//          ? Type.CROSS
//          : Type.INNER;
//      //Similar, we want to push in the first join criteria if possible
//      Optional<JoinCriteria> firstCriteria = rhs.getNamePath().getLength() > 1
//          ? Optional.empty()
//          : node.getCriteria();
//
//      Join join = new Join(Optional.empty(),
//          type,
//          (Relation)left.getNode(),
//          tableNode,
//          rewrite(firstCriteria, scope)
//      );
//
//      JoinWalker joinWalker = new JoinWalker();
//      WalkResult result = joinWalker.walk(
//          firstAlias,
//          Optional.of(lastAlias),
//          rhs.getNamePath().popFirst(),
//          Optional.of(join),
//          rewrite(node.getCriteria(), scope),
//          scope.getJoinScope()
//      );
//
//      return createScope(result.getRelation(), scope);
//    }
//    return null;
//  }
//
//  private Optional<JoinCriteria> rewrite(Optional<JoinCriteria> criteria, Scope scope) {
//    return criteria.map(e->
//        new JoinOn(e.getLocation(), rewriteExpression(((JoinOn)e).getExpression(), scope)));
//  }
//
//  @Override
//  public Scope visitUnion(Union node, Scope scope) {
//    return visitSetOperation(node, scope);
//  }
//
//  @Override
//  public Scope visitIntersect(Intersect node, Scope scope) {
//    return visitSetOperation(node, scope);
//  }
//
//  @Override
//  public Scope visitExcept(Except node, Scope scope) {
//    return visitSetOperation(node, scope);
//  }
//
//  @Override
//  public Scope visitSetOperation(SetOperation node, Scope scope) {
//    return null;
//  }
//
//  /**
//   * Expands STAR alias
//   */
//  private Select expandStar(Select select, Scope scope) {
//    List<SelectItem> expanded = new ArrayList<>();
//    for (SelectItem item : select.getSelectItems()) {
//      if (item instanceof AllColumns) {
//        Optional<Name> starPrefix = ((AllColumns) item).getPrefix()
//            .map(e->e.getFirst());
//        List<Identifier> fields = scope.resolveFieldsWithPrefix(starPrefix);
//        for (Identifier identifier : fields) {
//          expanded.add(new SingleColumn(identifier,Optional.empty()));
//        }
//      } else {
//        expanded.add(item);
//      }
//    }
//    return new Select(expanded);
//  }
//
//  private Scope createScope(Node node, Scope parentScope) {
//    return new Scope(parentScope.getContextTable(), node, parentScope.getJoinScope(),
//        parentScope.getFieldScope(),
//        parentScope.isExpression(), parentScope.getExpressionName());
//  }
//
  private Expression rewriteExpression(Expression expression, StatementScope scope) {
    ExpressionTransformer expressionTransformer = new ExpressionTransformer();
    Expression expr = expressionTransformer.transform(expression, scope);

    this.additionalJoins.addAll(expressionTransformer.joinResults);
    return expr;
  }
}
